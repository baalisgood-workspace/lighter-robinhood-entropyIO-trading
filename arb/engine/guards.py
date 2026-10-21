"""Instance-wide pauses, open-only blocks and alerts (ported from lighter-rh's global risk control).

Pauses stop NEW slices (opens and closes) of this instance for a while; they never touch positions.
Open-only blocks (volatility, leg spread, daily limit, reduce-only market) keep closes working.
Alerts only warn (event log, dashboard, terminal bell).

  slippage pause    realised edge - required edge <= -slippage_pause_pct, or a burst of small losses
  latency pause     a taker hedge IOC took longer than hedge_latency_max_s from submit to final
  venue backoff     HTTP 429 / rate limit / invalid nonce -> exponential pause (base * 2^(n-1), capped)
  API failure pause transport errors / 5xx / timeouts >= api_failure_count within the window (opt-in)
  volatility block  mid range (max-min)/min >= volatility_max_pct within volatility_window_s
"""
from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from arb.engine import signals as sg

if TYPE_CHECKING:  # pragma: no cover
    from arb.engine.engine import Ctx, Engine
    from arb.engine.executor import ExecPlan, ExecResult

log = logging.getLogger("arb.guards")

BURST_SLIP_PCT = 0.01  # a slice counts towards the burst when its slippage is this bad or worse
REDUCE_ONLY_HINTS = ("reduce only", "reduce-only", "reduceonly", "reduce_only")


class Guards:
    def __init__(self, eng: "Engine"):
        self.eng = eng
        self.g = eng.cfg.guards
        self.pauses: dict[str, tuple[float, str]] = {}  # key -> (until, reason)
        self.alerts: dict[str, str] = {}  # key -> text (dashboard)
        self.hedge_lat: deque[float] = deque(maxlen=200)
        self.slips: deque[float] = deque(maxlen=200)  # recent slippage values (dashboard)
        self._burst: deque[float] = deque()
        self._fail_seen: dict[str, int] = {}
        self._api_fail: dict[str, deque] = {}
        self._backoff_n: dict[str, int] = {}
        self._backoff_last: dict[str, float] = {}
        self._last_bell = 0.0
        self._last_gap_warn = 0.0
        self._warned: set[str] = set()

    # ------------------------------------------------------------ helpers
    def bell(self) -> None:
        if not self.g.bell:
            return
        now = time.time()
        if now - self._last_bell >= self.g.bell_min_interval_s:
            self._last_bell = now
            print("\a", end="", flush=True)

    def pause(self, key: str, seconds: float, reason: str) -> None:
        if seconds <= 0:
            return
        now = time.time()
        until = now + seconds
        cur = self.pauses.get(key)
        if cur and cur[0] >= until:
            return
        fresh = cur is None or cur[0] <= now
        self.pauses[key] = (until, reason)
        if fresh:
            self.eng.event(f"PAUSE {seconds:.0f}s: {reason}", logging.WARNING)
            self.eng.journal("pause", key=key, seconds=round(seconds, 1), reason=reason)
            self.bell()

    def global_reason(self, now: Optional[float] = None) -> str:
        now = time.time() if now is None else now
        for k in [k for k, (u, _) in self.pauses.items() if u <= now]:
            _, why = self.pauses.pop(k)
            self.eng.event(f"pause over: {why}")
            self.eng.journal("pause_over", key=k)
        if not self.pauses:
            return ""
        _k, (u, why) = max(self.pauses.items(), key=lambda kv: kv[1][0])
        return f"{why} (剩余 {u - now:.0f}s)"

    def venue_paused(self, venue: str, now: float) -> bool:
        return any(u > now for k, (u, _) in self.pauses.items() if k.endswith(":" + venue))

    # ------------------------------------------------------------ venue failures
    def tick(self, now: float) -> None:
        names = [self.eng.active.maker, self.eng.active.taker] if self.eng.active else []
        for n in names:
            v = self.eng.venues.get(n)
            if v is None:
                continue
            seen = self._fail_seen.get(n, 0)
            for seq, ts, kind, detail in list(v.failures):
                if seq <= seen:
                    continue
                seen = seq
                if kind in ("rate_limit", "nonce"):
                    last = self._backoff_last.get(n, 0.0)
                    k = self._backoff_n.get(n, 0) + 1 if ts - last < self.g.backoff_reset_s else 1
                    self._backoff_n[n], self._backoff_last[n] = k, ts
                    dur = min(self.g.backoff_base_s * 2 ** (k - 1), self.g.backoff_max_s)
                    what = "限流" if kind == "rate_limit" else "nonce 错误"
                    self.pause(f"backoff:{n}", dur, f"{n} {what} 第{k}次, 退避 {dur:.0f}s ({detail[:80]})")
                elif kind == "api_error" and self.g.api_failure_pause:
                    dq = self._api_fail.setdefault(n, deque())
                    dq.append(ts)
                    while dq and dq[0] < now - self.g.api_failure_window_s:
                        dq.popleft()
                    if len(dq) >= self.g.api_failure_count:
                        self.pause(f"api:{n}", self.g.api_failure_pause_s,
                                   f"{n} API 故障 {len(dq)} 次/{self.g.api_failure_window_s:.0f}s ({detail[:80]})")
                        dq.clear()
            self._fail_seen[n] = seen

    # ------------------------------------------------------------ per symbol observation
    def observe(self, c: "Ctx", now: float, mid: float) -> None:
        rk = c.sym.risk
        if rk.volatility_max_pct is not None:
            dq = c.vol_mids
            if not dq or now - dq[-1][0] >= 1.0:
                dq.append((now, mid))
            while dq and dq[0][0] < now - rk.volatility_window_s:
                dq.popleft()
            if len(dq) >= 2:
                lo = min(x for _, x in dq)
                hi = max(x for _, x in dq)
                c.vol_amp = (hi - lo) / lo * 100 if lo > 0 else 0.0
                if c.vol_amp >= rk.volatility_max_pct:
                    if now >= c.vol_block_until:
                        self.eng.event(f"{c.sym.key} 波动 {c.vol_amp:.2f}% / {rk.volatility_window_s:.0f}s >= "
                                       f"{rk.volatility_max_pct:g}%: 禁止开仓 {rk.volatility_block_s:.0f}s", logging.WARNING)
                        self.eng.journal("volatility_block", symbol=c.sym.key, amplitude_pct=round(c.vol_amp, 3))
                        self.bell()
                    c.vol_block_until = now + rk.volatility_block_s  # refreshed while still triggered
        warns = []
        sp = self.eng.position(c)
        if sp.grids and rk.max_hold_hours is not None:
            age_h = (now - min(g.opened_ts for g in sp.grids)) / 3600
            if age_h >= rk.max_hold_hours:
                warns.append(f"持仓{age_h:.1f}h")
                self._warn_once(f"hold:{c.key}", f"{c.sym.key} 持仓已 {age_h:.1f}h (> {rk.max_hold_hours:g}h)")
        if sp.direction and self.g.adverse_funding_warn_hours is not None:
            open_dir = sg.SELL_MAKER if sp.direction == sg.SHORT_MAKER else sg.BUY_MAKER
            hourly = self.eng._funding_hourly(c, open_dir)
            if hourly is not None and hourly >= self.g.adverse_funding_warn_hourly_pct:
                c.adverse_since = c.adverse_since or now
                hrs = (now - c.adverse_since) / 3600
                if hrs >= self.g.adverse_funding_warn_hours:
                    warns.append(f"费率不利{hrs:.1f}h")
                    self._warn_once(f"fund:{c.key}", f"{c.sym.key} 资金费率持续不利 {hrs:.1f}h ({hourly:.4f}%/h)")
            else:
                c.adverse_since = None
                self._warned.discard(f"fund:{c.key}")
        else:
            c.adverse_since = None
        c.warn = " ".join(warns)

    def _warn_once(self, key: str, msg: str) -> None:
        if key in self._warned:
            return
        self._warned.add(key)
        self.eng.event("WARNING " + msg, logging.WARNING)
        self.eng.journal("warning", key=key, message=msg)
        self.bell()

    # ------------------------------------------------------------ open-only blocks
    def open_block(self, c: "Ctx", now: float) -> str:
        rk = c.sym.risk
        if now < c.vol_block_until:
            return f"波动 {c.vol_amp:.1f}% 禁开 {c.vol_block_until - now:.0f}s"
        if now < c.ro_block_until:
            return f"交易所只减仓 {c.ro_block_until - now:.0f}s"
        if rk.max_daily_opens is not None:
            day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if c.opens_day == day and c.opens_today >= rk.max_daily_opens:
                return f"今日开仓 {c.opens_today}/{rk.max_daily_opens}"
        if rk.max_leg_spread_pct is not None:
            legs = [(c.pair.taker, c.taker_sym)]
            if c.sym.execution.close_mode == "taker_taker":
                legs.append((c.pair.maker, c.maker_sym))
            for vname, sym in legs:
                b = self.eng.venues[vname].book(sym)
                if b and b.best_bid and b.best_ask and b.mid:
                    spr = float((b.best_ask.price - b.best_bid.price) / b.mid * 100)
                    if spr > rk.max_leg_spread_pct:
                        return f"{vname} 盘口点差 {spr:.3f}% > {rk.max_leg_spread_pct:g}%"
        return ""

    # ------------------------------------------------------------ results
    def on_result(self, c: "Ctx", plan: "ExecPlan", res: "ExecResult", dev: Optional[float]) -> Optional[float]:
        now = time.time()
        g = self.g
        for lat in res.hedge_latencies:
            self.hedge_lat.append(lat)
            if g.hedge_latency_max_s is not None and lat >= g.hedge_latency_max_s:
                self.pause("latency", g.hedge_latency_pause_s,
                           f"对冲延迟 {lat:.2f}s >= {g.hedge_latency_max_s:g}s ({c.sym.key} on {c.pair.taker})")
        text = " ".join(res.events + [res.error or ""]).lower()
        if plan.kind == "open" and any(h in text for h in REDUCE_ONLY_HINTS):
            c.ro_block_until = now + c.sym.risk.reduce_only_block_s
            self.eng.event(f"{c.sym.key}: 交易所拒绝开仓(只减仓模式), 禁止开仓 {c.sym.risk.reduce_only_block_s:.0f}s",
                           logging.WARNING)
        if plan.kind == "open" and res.hedged > 0:
            day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if c.opens_day != day:
                c.opens_day, c.opens_today = day, 0
            c.opens_today += 1
        slip = None
        if res.hedged > 0 and dev is not None:
            slip = dev - plan.required_dev
            self.slips.append(slip)
            trigger = ""
            if g.slippage_pause_pct is not None and slip <= -g.slippage_pause_pct:
                trigger = f"{c.sym.key} {plan.kind} 滑点 {slip:+.3f}% <= -{g.slippage_pause_pct:g}%"
            if g.slippage_burst_count > 0 and slip <= -BURST_SLIP_PCT:
                self._burst.append(now)
                while self._burst and self._burst[0] < now - g.slippage_burst_window_s:
                    self._burst.popleft()
                if len(self._burst) >= g.slippage_burst_count and not trigger:
                    trigger = f"{len(self._burst)} 次负滑点 / {g.slippage_burst_window_s:.0f}s"
            if trigger:
                self._burst.clear()
                if g.slippage_pause_scope == "all":
                    self.pause("slippage", g.slippage_pause_s, trigger)
                else:
                    c.paused_until = max(c.paused_until, now + g.slippage_pause_s)
                    c.pause_reason = trigger
                    self.eng.event(f"PAUSE {g.slippage_pause_s:.0f}s: {trigger}", logging.WARNING)
                    self.eng.journal("pause", key=f"slippage:{c.sym.key}", seconds=g.slippage_pause_s, reason=trigger)
                    self.bell()
        return slip

    # ------------------------------------------------------------ balances
    def on_balances(self, now: float) -> None:
        eng, g = self.eng, self.g
        if not eng.active:
            return
        names = (eng.active.maker, eng.active.taker)
        for n in names:
            b = eng.balances.get(n)
            key = f"balance:{n}"
            if g.balance_warn_usdc is not None and b is not None and float(b.equity) < g.balance_warn_usdc:
                if key not in self.alerts:
                    eng.event(f"WARNING {n} 权益 {float(b.equity):.0f} < {g.balance_warn_usdc:.0f} USDC", logging.WARNING)
                    self.bell()
                self.alerts[key] = f"{n} 权益 {float(b.equity):.0f} < {g.balance_warn_usdc:.0f}"
            else:
                self.alerts.pop(key, None)
        bm, bt = eng.balances.get(names[0]), eng.balances.get(names[1])
        if g.equity_gap_warn_usdc is not None and bm is not None and bt is not None:
            gap = abs(float(bm.equity) - float(bt.equity))
            if gap >= g.equity_gap_warn_usdc:
                self.alerts["equity_gap"] = f"两所权益差 {gap:.0f} USDC, 需手动调仓"
                if now - self._last_gap_warn >= g.equity_gap_interval_s:
                    self._last_gap_warn = now
                    eng.event(f"WARNING 两所权益差 {gap:.0f} USDC ({names[0]} {float(bm.equity):.0f} / "
                              f"{names[1]} {float(bt.equity):.0f}) - 资金不会自动划转", logging.WARNING)
                    self.bell()
            else:
                self.alerts.pop("equity_gap", None)

    # ------------------------------------------------------------ dashboard
    def latency_stats(self) -> tuple[Optional[float], Optional[float]]:
        if not self.hedge_lat:
            return None, None
        xs = sorted(self.hedge_lat)
        return xs[len(xs) // 2], xs[-1]
