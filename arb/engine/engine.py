"""Orchestration: market data -> basis -> signals -> (live) maker->taker execution."""
from __future__ import annotations

import asyncio
import csv
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

from arb.config import AppConfig, ConfigError, Pair, SymbolConfig, effective_t1
from arb.engine import signals as sg
from arb.engine.basis import BasisBook, BasisState
from arb.engine.executor import ExecPlan, ExecResult, Executor
from arb.engine.state import PositionBook, SymbolPosition
from arb.models import Balance, Position
from arb.venues.base import Venue

log = logging.getLogger("arb.engine")
ZERO = Decimal("0")


@dataclass
class Ctx:
    pair: Pair
    sym: SymbolConfig
    maker_sym: str
    taker_sym: str
    basis: Optional[BasisState]
    mids_m: deque = field(default_factory=deque)
    mids_t: deque = field(default_factory=deque)
    q_sell: Optional[sg.Quote] = None
    q_buy: Optional[sg.Quote] = None
    qty: Decimal = ZERO
    basis_value: float = 0.0
    ready: bool = False
    status: str = "waiting"
    disabled: str = ""
    cooldown_until: float = 0.0
    failures: int = 0
    paused_until: float = 0.0
    next_open: Optional[float] = None
    next_close: Optional[float] = None

    @property
    def key(self) -> str:
        return f"{self.pair.key}|{self.sym.key}"


class Engine:
    def __init__(self, cfg: AppConfig, venues: dict[str, Venue], live: bool):
        self.cfg = cfg
        self.venues = venues
        self.live = live
        enabled = cfg.enabled_venues()
        self.active: Optional[Pair] = cfg.active_pair() if (live or len(enabled) == 2) else None
        data = Path(cfg.data_dir)
        self.basis_book = BasisBook(data / "basis_state.json")
        self.positions = PositionBook(data / "positions.json")
        self.sample_dir = data / "spread_samples"
        self.ctxs: list[Ctx] = []
        self.events: deque[str] = deque(maxlen=14)
        self.halted = ""
        self.stopping = False
        self.balances: dict[str, Balance] = {}
        self.exch_positions: dict[str, dict[str, Position]] = {}
        self.mismatch_strikes = 0
        self.reconciled_ok = False
        self.exec_task: Optional[asyncio.Task] = None
        self.exec_ctx: Optional[Ctx] = None
        self.executor = Executor(venues, stop_flag=lambda: self.stopping)
        self.started = time.time()

    # ------------------------------------------------------------ setup
    def event(self, msg: str, level: int = logging.INFO) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self.events.append(f"{stamp} {msg}")
        log.log(level, msg)

    def build_contexts(self) -> None:
        now = time.time()
        for pair in self.cfg.watched_pairs():
            if pair.a not in self.venues or pair.b not in self.venues:
                continue
            for s in self.cfg.symbols_for_pair(pair):
                ms, ts = s.venues[pair.maker], s.venues[pair.taker]
                if ms not in self.venues[pair.maker].markets or ts not in self.venues[pair.taker].markets:
                    continue
                basis = self.basis_book.get(pair.key, s.key, s.basis, now) if s.basis.enabled else None
                self.ctxs.append(Ctx(pair=pair, sym=s, maker_sym=ms, taker_sym=ts, basis=basis))
        if self.active and not any(c.pair == self.active for c in self.ctxs):
            raise ConfigError(f"no tradable symbol for active pair {self.active.key}")

    def active_ctxs(self) -> list[Ctx]:
        return [c for c in self.ctxs if self.active and c.pair == self.active]

    def position(self, ctx: Ctx) -> SymbolPosition:
        return self.positions.get(ctx.pair.key, ctx.sym.key)

    # ------------------------------------------------------------ live preflight
    async def preflight(self) -> None:
        assert self.active is not None
        other = self.positions.open_pairs() - {self.active.key}
        if other:
            raise ConfigError(
                f"local state still has open positions on pair(s) {sorted(other)}. Enable exactly those two venues with "
                f"trading.close_only: true and flatten them first (or resolve data/positions.json manually).")
        await self.reconcile(initial=True)
        for vname in (self.active.maker, self.active.taker):
            syms = [c.maker_sym if vname == self.active.maker else c.taker_sym for c in self.active_ctxs()]
            await self.venues[vname].cancel_all(syms)
        self.event(f"live preflight ok: {self.active.maker} maker -> {self.active.taker} taker, "
                   f"{len(self.active_ctxs())} symbols")

    async def reconcile(self, initial: bool = False) -> bool:
        assert self.active is not None
        M, T = self.venues[self.active.maker], self.venues[self.active.taker]
        pm, pt = await M.positions(), await T.positions()
        self.exch_positions[M.name], self.exch_positions[T.name] = pm, pt
        problems = []
        for c in self.active_ctxs():
            sp = self.position(c)
            am = pm.get(c.maker_sym).qty if c.maker_sym in pm else ZERO
            at = pt.get(c.taker_sym).qty if c.taker_sym in pt else ZERO
            tol = Decimal(str(c.sym.risk.reconcile_tolerance_mult)) * max(M.market(c.maker_sym).min_qty, T.market(c.taker_sym).min_qty)
            dm, dt = am - sp.expected_maker_qty(), at - sp.expected_taker_qty()
            if abs(dm) > tol or abs(dt) > tol:
                problems.append(f"{c.sym.key}: {M.name} {am} (expected {sp.expected_maker_qty()}), "
                                f"{T.name} {at} (expected {sp.expected_taker_qty()})")
        if initial and problems:
            msg = "exchange positions do not match local state:\n  " + "\n  ".join(problems)
            if not self.cfg.allow_preexisting_positions:
                raise ConfigError(msg + "\nUse dedicated accounts, flatten manually, or set trading.allow_preexisting_positions: true.")
            self.event("WARNING " + msg.replace("\n", " "), logging.WARNING)
        if problems and not initial:
            self.mismatch_strikes += 1
            self.event(f"position mismatch ({self.mismatch_strikes}): {'; '.join(problems)}", logging.WARNING)
            if self.mismatch_strikes >= 2 and not self.cfg.allow_preexisting_positions:
                self.halt("positions do not match local state: " + "; ".join(problems))
        elif not problems:
            self.mismatch_strikes = 0
        self.reconciled_ok = not problems
        return not problems

    def halt(self, reason: str) -> None:
        if not self.halted:
            self.halted = reason
            self.event(f"HALTED: {reason}", logging.ERROR)
            print("\a", end="", flush=True)

    # ------------------------------------------------------------ main loops
    async def run(self) -> None:
        tasks = [asyncio.create_task(self._tick_loop(), name="tick"),
                 asyncio.create_task(self._persist_loop(), name="persist"),
                 asyncio.create_task(self._sampler_loop(), name="sampler"),
                 asyncio.create_task(self._account_loop(), name="account")]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()

    async def shutdown(self) -> None:
        self.stopping = True
        if self.exec_task and not self.exec_task.done():
            self.event("waiting for in-flight execution to finish ...")
            try:
                await asyncio.wait_for(asyncio.shield(self.exec_task), timeout=60)
            except asyncio.TimeoutError:
                self.event("in-flight execution did not finish in 60s", logging.ERROR)
        if self.live and self.active:
            for vname in (self.active.maker, self.active.taker):
                syms = [c.maker_sym if vname == self.active.maker else c.taker_sym for c in self.active_ctxs()]
                try:
                    await self.venues[vname].cancel_all(syms)
                except Exception as exc:  # noqa: BLE001
                    log.warning("cancel_all on shutdown failed for %s: %s", vname, exc)
        self.basis_book.save()
        self.positions.save()

    async def _tick_loop(self) -> None:
        while not self.stopping:
            now = time.time()
            for c in self.ctxs:
                try:
                    self.compute(c, now)
                except Exception as exc:  # noqa: BLE001
                    c.status = f"error: {exc}"
                    log.exception("compute %s", c.key)
            if self.live and not self.halted and not self.stopping and (self.exec_task is None or self.exec_task.done()):
                for c in self.active_ctxs():
                    plan = self.decide(c, now)
                    if plan is not None:
                        self.exec_ctx = c
                        self.exec_task = asyncio.create_task(self._execute(c, plan))
                        break
            await asyncio.sleep(0.1)

    async def _persist_loop(self) -> None:
        while not self.stopping:
            await asyncio.sleep(60)
            self.basis_book.save()

    async def _account_loop(self) -> None:
        interval_bal = 30.0
        last_bal = last_rec = 0.0
        while not self.stopping:
            now = time.time()
            names = [self.active.maker, self.active.taker] if self.active else []
            if now - last_bal >= interval_bal:
                last_bal = now
                for n in names:
                    v = self.venues.get(n)
                    if v and (self.live or v.has_credentials()):
                        try:
                            self.balances[n] = await v.balance()
                        except Exception as exc:  # noqa: BLE001
                            log.debug("balance %s: %s", n, exc)
            rec_iv = min((c.sym.risk.reconcile_interval_s for c in self.active_ctxs()), default=30.0)
            if self.live and self.active and now - last_rec >= rec_iv and (self.exec_task is None or self.exec_task.done()):
                last_rec = now
                try:
                    await self.reconcile()
                except Exception as exc:  # noqa: BLE001
                    self.reconciled_ok = False
                    log.warning("reconcile failed: %s", exc)
                if self.reconciled_ok and not self.halted:
                    await self._fix_residuals()
            await asyncio.sleep(1.0)

    async def _sampler_loop(self) -> None:
        self.sample_dir.mkdir(parents=True, exist_ok=True)
        cols = ["ts", "pair", "maker", "taker", "symbol", "mid_maker", "mid_taker", "basis_now", "basis_ema", "basis_ready",
                "raw_sell_maker", "dev_sell_maker", "raw_buy_maker", "dev_buy_maker", "qty"]
        while not self.stopping:
            await asyncio.sleep(self.cfg.sampler_interval_s)
            day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            path = self.sample_dir / f"{day}.csv"
            new = not path.exists()
            rows = []
            for c in self.ctxs:
                mb, tb = self.venues[c.pair.maker].book(c.maker_sym), self.venues[c.pair.taker].book(c.taker_sym)
                if not (mb and tb and mb.mid and tb.mid):
                    continue
                rows.append([f"{time.time():.3f}", c.pair.key, c.pair.maker, c.pair.taker, c.sym.key, str(mb.mid), str(tb.mid),
                             f"{sg.basis_value(mb.mid, tb.mid):.5f}", f"{c.basis_value:.5f}", int(c.ready),
                             _f(c.q_sell.raw if c.q_sell else None), _f(c.q_sell.dev if c.q_sell else None),
                             _f(c.q_buy.raw if c.q_buy else None), _f(c.q_buy.dev if c.q_buy else None), str(c.qty)])
            if rows:
                with path.open("a", newline="") as f:
                    w = csv.writer(f)
                    if new:
                        w.writerow(cols)
                    w.writerows(rows)

    # ------------------------------------------------------------ compute
    def compute(self, c: Ctx, now: float) -> None:
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        rk = c.sym.risk
        mb, tb = M.book(c.maker_sym), T.book(c.taker_sym)
        fresh = bool(mb and tb and mb.is_valid() and tb.is_valid() and M.quote_age(c.maker_sym, now) <= rk.max_quote_age_s and T.quote_age(c.taker_sym, now) <= rk.max_quote_age_s)
        if not fresh:
            c.q_sell = c.q_buy = None
            c.ready = False
            c.status = "no fresh quotes"
            if c.basis:
                c.basis.observe(now, None)
            return
        mm, tm = float(mb.mid), float(tb.mid)
        div = abs(mm - tm) / tm * 100
        if div > rk.max_price_divergence_pct:
            c.disabled = f"price divergence {div:.1f}% (unit mismatch?)"
            c.status = c.disabled
            c.q_sell = c.q_buy = None
            return
        c.disabled = ""
        for dq, v in ((c.mids_m, mm), (c.mids_t, tm)):
            dq.append((now, v))
            while dq and dq[0][0] < now - rk.stability_window_s:
                dq.popleft()
        if c.basis:
            c.basis.observe(now, sg.basis_value(mb.mid, tb.mid))
            c.basis_value = c.basis.ema or 0.0
            c.ready, reason = c.basis.ready(now)
        else:
            c.basis_value, c.ready, reason = 0.0, True, ""
        st = c.sym.strategy
        ref = max(mb.mid, tb.mid)
        step = max(M.market(c.maker_sym).size_step, T.market(c.taker_sym).size_step)
        from arb.util import floor_to_step
        c.qty = floor_to_step(Decimal(str(st.order_value_usdc)) / ref, step)
        ex = c.sym.execution
        tick = M.price_tick_at(c.maker_sym, mb.mid)
        c.q_sell = sg.build_quote(sg.SELL_MAKER, mb, tb, c.qty, c.basis_value, ex.maker_price_mode, tick, rk.depth_levels, rk.depth_band_pct)
        c.q_buy = sg.build_quote(sg.BUY_MAKER, mb, tb, c.qty, c.basis_value, ex.maker_price_mode, tick, rk.depth_levels, rk.depth_band_pct)
        c.status = "ready" if c.ready else reason

    # ------------------------------------------------------------ decide
    def _fees(self, c: Ctx) -> float:
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        tt = c.sym.execution.close_mode == "taker_taker"
        return sg.roundtrip_fee_pct(M.maker_fee_pct(c.maker_sym), T.taker_fee_pct(c.taker_sym), tt, M.taker_fee_pct(c.maker_sym))

    def _funding(self, c: Ctx, direction: str) -> float:
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        return sg.adverse_funding_pct(direction, M.funding_hourly.get(c.maker_sym), T.funding_hourly.get(c.taker_sym),
                                      c.sym.risk.funding_horizon_hours)

    def _venue_notional(self, venue: str) -> float:
        total = 0.0
        for c in self.active_ctxs():
            sp = self.position(c)
            if sp.total_qty > 0:
                b = self.venues[venue].book(c.maker_sym if venue == c.pair.maker else c.taker_sym)
                if b and b.mid:
                    total += float(sp.total_qty * b.mid)
        return total

    def _balances_ok(self, c: Ctx, now: float) -> tuple[bool, str]:
        for n in (c.pair.maker, c.pair.taker):
            b = self.balances.get(n)
            if b is None or now - b.ts > 180:
                return False, f"{n} balance unknown"
            if float(b.available) < c.sym.risk.min_available_usdc:
                return False, f"{n} available {float(b.available):.0f} < {c.sym.risk.min_available_usdc:.0f}"
        return True, ""

    def decide(self, c: Ctx, now: float) -> Optional[ExecPlan]:
        c.next_open = c.next_close = None
        if c.disabled or not c.ready or c.q_sell is None or c.q_buy is None:
            return None
        if now < c.paused_until or now < c.cooldown_until:
            return None
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        st, rk, ex = c.sym.strategy, c.sym.risk, c.sym.execution
        sp = self.position(c)
        t1 = effective_t1(st, now)
        min_qty = max(M.market(c.maker_sym).min_qty, T.market(c.taker_sym).min_qty)
        sp.merge_dust(min_qty)
        basis_fn = (lambda: c.basis.ema or 0.0) if c.basis else (lambda: 0.0)
        tt = ex.close_mode == "taker_taker"

        # ---------------- close (highest grid first)
        if sp.grids:
            top = sp.grids[-1]
            cdir = sg.CLOSE_DIRECTION[sp.direction]
            qty = min(c.qty, self._rq(c, top.qty))
            if top.qty - qty < min_qty:  # do not leave an untradable remainder
                qty = self._rq(c, top.qty)
            cl = sg.close_level(t1, st.grid_step_pct, st.first_close_ratio, top.level)
            c.next_close = cl
            mb, tb = M.book(c.maker_sym), T.book(c.taker_sym)
            q = sg.build_quote(cdir, mb, tb, qty, c.basis_value, ex.maker_price_mode, M.price_tick_at(c.maker_sym, mb.mid),
                               rk.depth_levels, rk.depth_band_pct, maker_as_taker=tt)
            if q.ok and -q.dev <= cl and qty >= min_qty:
                if not self._stable(c):
                    c.status = "close signal, prices unstable"
                    return None
                return ExecPlan("close", c.pair, c.sym, c.maker_sym, c.taker_sym, cdir, qty, top.level, -cl, basis_fn, taker_taker=tt)

        # ---------------- open
        if self.cfg.close_only or c.sym.close_only:
            return None
        fees = self._fees(c)
        cands = []
        for q in (c.q_sell, c.q_buy):
            if not q.ok:
                continue
            pos_dir = sg.POSITION_OF[q.direction]
            if sp.direction and sp.direction != pos_dir:
                continue
            if sp.grids and sp.grids[-1].qty < sp.grids[-1].target_qty - min_qty:
                level, target = sp.grids[-1].level, sp.grids[-1].target_qty
                remaining = target - sp.grids[-1].qty
            else:
                level = len(sp.grids) + 1
                if level > st.max_grids:
                    continue
                target = self._rq(c, Decimal(str(st.grid_value_usdc)) / max(M.book(c.maker_sym).mid, T.book(c.taker_sym).mid))
                remaining = target
            funding = self._funding(c, q.direction)
            need = sg.open_level(t1, st.grid_step_pct, level) + fees + funding
            if c.next_open is None or need < c.next_open:
                c.next_open = need
            hourly_adverse = funding / max(rk.funding_horizon_hours, 1e-9)
            if rk.funding_gate_enabled and hourly_adverse > rk.max_adverse_funding_hourly_pct:
                continue
            if q.dev >= need:
                cands.append((q.dev - need, q, level, target, remaining, need))
        if not cands:
            return None
        _, q, level, target, remaining, need = max(cands, key=lambda x: x[0])
        qty = self._rq(c, min(c.qty, remaining))
        if qty < min_qty:
            return None
        if not self._stable(c):
            c.status = "open signal, prices unstable"
            return None
        ok, why = self._balances_ok(c, now)
        if not ok:
            c.status = f"blocked: {why}"
            return None
        px = float(max(M.book(c.maker_sym).mid, T.book(c.taker_sym).mid))
        if float(sp.total_qty + qty) * px > rk.max_symbol_value_usdc * 1.0001:
            c.status = "blocked: symbol cap"
            return None
        for n in (c.pair.maker, c.pair.taker):
            if self._venue_notional(n) + float(qty) * px > rk.max_venue_notional_usdc:
                c.status = f"blocked: {n} notional cap"
                return None
        plan = ExecPlan("open", c.pair, c.sym, c.maker_sym, c.taker_sym, q.direction, qty, level, need, basis_fn)
        plan.target = target  # type: ignore[attr-defined]
        return plan

    def _rq(self, c: Ctx, q: Decimal) -> Decimal:
        """Round a quantity so it is valid on BOTH venues (coarser size step)."""
        from arb.util import floor_to_step
        step = max(self.venues[c.pair.maker].market(c.maker_sym).size_step, self.venues[c.pair.taker].market(c.taker_sym).size_step)
        return floor_to_step(q, step)

    def _stable(self, c: Ctx) -> bool:
        lim = c.sym.risk.stability_max_move_pct
        return sg.mid_move_pct(list(c.mids_m)) <= lim and sg.mid_move_pct(list(c.mids_t)) <= lim

    # ------------------------------------------------------------ execute
    async def _execute(self, c: Ctx, plan: ExecPlan) -> None:
        self.event(f"{plan.kind.upper()} {c.sym.key} {plan.direction} qty={plan.qty} L{plan.level} "
                   f"need>={plan.required_dev:.3f}% ({c.pair.maker}->{c.pair.taker})")
        res = await self.executor.run(plan)
        self.apply_result(c, plan, res)
        c.cooldown_until = time.time() + c.sym.execution.cooldown_s

    def apply_result(self, c: Ctx, plan: ExecPlan, res: ExecResult) -> None:
        sp = self.position(c)
        s_m = Decimal(1) if sg.maker_is_buy(plan.direction) else Decimal(-1)
        hedged = res.hedged
        sp.residual_maker += s_m * (res.maker_net - hedged)
        sp.residual_taker += -s_m * (res.taker_filled - hedged)
        dev = res.realized_dev(plan.direction, plan.basis_fn())
        if plan.kind == "open":
            if hedged > 0:
                sp.add_open(sg.POSITION_OF[plan.direction], plan.level, hedged, getattr(plan, "target", hedged), dev or 0.0)
        else:
            if hedged > 0 and sp.grids:
                od = sp.grids[-1].open_dev or 0.0
                px = float(res.taker_avg or 0)
                sp.realized_edge_usdc += (od + (dev or 0.0)) / 100 * float(hedged) * px
            sp.reduce_close(hedged)
        self.positions.save()
        msg = (f"{plan.kind} {c.sym.key}: maker {res.maker_filled}@{_f(res.maker_avg)} taker {res.taker_filled}@{_f(res.taker_avg)}"
               f" hedged={hedged} edge={_f(dev)}% {res.note}")
        if res.maker_unwound:
            msg += f" unwound={res.maker_unwound}"
        for e in res.events:
            self.event(f"  {c.sym.key}: {e}", logging.WARNING)
        self.event(msg)
        if res.halted:
            self.halt(f"{c.sym.key}: {res.error}")
        elif res.error or (res.orders_placed and res.maker_filled == 0 and "rejected" in " ".join(res.events)):
            c.failures += 1
            if c.failures >= c.sym.risk.max_consecutive_failures:
                c.paused_until = time.time() + c.sym.risk.failure_pause_s
                self.event(f"{c.sym.key} paused {c.sym.risk.failure_pause_s:.0f}s after {c.failures} failures", logging.WARNING)
                c.failures = 0
        else:
            c.failures = 0

    async def _fix_residuals(self) -> None:
        """Flatten unhedged leftovers that became tradable (only after a clean reconcile)."""
        for c in self.active_ctxs():
            sp = self.position(c)
            for vname, sym, attr in ((c.pair.maker, c.maker_sym, "residual_maker"), (c.pair.taker, c.taker_sym, "residual_taker")):
                r = getattr(sp, attr)
                if r == 0:
                    continue
                v = self.venues[vname]
                qty = v.round_qty(sym, abs(r))
                b = v.book(sym)
                if not b or not b.is_valid() or qty < v.market(sym).min_qty or qty * b.mid < v.market(sym).min_notional:
                    continue
                st = await self.executor._ioc(v, sym, r < 0, qty, False, c.sym.execution.hedge_slippage_pct, c.sym.risk.max_quote_age_s)
                if st is None:
                    continue
                if st.status.value == "unknown":
                    self.halt(f"residual fix order on {vname} has unknown outcome")
                    return
                if st.filled > 0:
                    setattr(sp, attr, r + (st.filled if r < 0 else -st.filled))
                    self.positions.save()
                    self.event(f"residual {c.sym.key} on {vname}: flattened {st.filled}")


def _f(x) -> str:
    if x is None:
        return "-"
    try:
        return f"{float(x):.4f}"
    except (TypeError, ValueError):
        return str(x)
