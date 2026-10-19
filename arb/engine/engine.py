"""Orchestration: market data -> basis -> signals -> (live) maker->taker execution."""
from __future__ import annotations

import asyncio
import csv
import json
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
from arb.engine.claims import Claims
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
    claim_block: str = ""  # symbol owned by another instance on a shared account
    fresh_since: Optional[float] = None  # start of the current continuous fresh two-venue window
    last_fresh: float = 0.0
    epochs: tuple = ()
    ready_s: float = 0.0  # cumulated seconds with a tradable (ready) quote, for uptime stats
    last_tick: float = 0.0

    @property
    def key(self) -> str:
        return f"{self.pair.key}|{self.sym.key}"


class Engine:
    def __init__(self, cfg: AppConfig, venues: dict[str, Venue], live: bool, data_dir: Optional[Path] = None):
        self.cfg = cfg
        self.venues = venues
        self.live = live
        enabled = cfg.enabled_venues()
        self.active: Optional[Pair] = cfg.active_pair() if (live or len(enabled) == 2) else None
        data = Path(data_dir) if data_dir is not None else Path(cfg.data_dir)
        self.data_dir = data
        self.claims: Optional[Claims] = None
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
        self.exec_tasks: dict[str, asyncio.Task] = {}  # ctx key -> in-flight slice
        self.inflight: dict[str, ExecPlan] = {}
        self.reconcile_due = False  # barrier: no new slices until the running ones finished and we reconciled
        self.executor = Executor(venues, stop_flag=lambda: self.stopping or bool(self.halted))
        self.started = time.time()
        self.lock_path = data / "MANUAL_INTERVENTION.json"
        self.journal_path = data / "events.jsonl"

    # ------------------------------------------------------------ journal / lock
    def journal(self, kind: str, **payload) -> None:
        """Append-only JSONL diagnostics (fills, closes, halts, market-health alarms)."""
        try:
            self.journal_path.parent.mkdir(parents=True, exist_ok=True)
            rec = {"ts": round(time.time(), 3), "kind": kind, "pair": self.active.key if self.active else None, **payload}
            with self.journal_path.open("a") as f:
                f.write(json.dumps(rec, default=str) + "\n")
        except Exception as exc:  # noqa: BLE001 - diagnostics must never break trading
            log.debug("journal write failed: %s", exc)

    def _drain_alarms(self) -> None:
        for v in self.venues.values():
            while v.alarms:
                a = dict(v.alarms.pop(0))
                a["alarm"] = a.pop("kind")
                a["at"] = a.pop("ts")
                self.journal("market_health", **a)

    def read_lock(self) -> Optional[dict]:
        if not self.lock_path.exists():
            return None
        try:
            return json.loads(self.lock_path.read_text())
        except Exception:  # noqa: BLE001
            return {"reason": "unreadable lock file"}

    def _write_lock(self, reason: str) -> None:
        if not self.live or self.lock_path.exists():
            return  # the FIRST halt cause is kept
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        snap = {k: v.to_json() for k, v in self.positions.items.items() if not v.is_flat()}
        self.lock_path.write_text(json.dumps({"ts": time.time(), "time": datetime.now().isoformat(timespec="seconds"),
                                              "pair": self.active.key if self.active else None, "reason": reason,
                                              "positions": snap}, indent=1, default=str))

    @property
    def busy(self) -> bool:
        return any(not t.done() for t in self.exec_tasks.values())

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
        return [c for c in self.ctxs if self.active and c.pair == self.active and not c.claim_block]

    def position(self, ctx: Ctx) -> SymbolPosition:
        return self.positions.get(ctx.pair.key, ctx.sym.key)

    # ------------------------------------------------------------ live preflight
    async def preflight(self) -> None:
        assert self.active is not None
        lock = self.read_lock()
        if lock:
            raise ConfigError(f"manual intervention required ({self.lock_path}): {lock.get('reason')}\n"
                              f"Check both venues, fix positions / data/<pair>/positions.json, then run "
                              f"`python -m arb unlock --venues {self.active.a},{self.active.b}`.")
        other = self.positions.open_pairs() - {self.active.key}
        if other:
            raise ConfigError(f"{self.data_dir / 'positions.json'} has open positions of other pair(s) {sorted(other)}")
        # parallel instances: claim every (venue account, symbol) this instance will trade
        self.claims = Claims(Path(self.cfg.data_dir), self.active.key)
        names = (self.active.maker, self.active.taker)
        self.claims.write_accounts({n: self.venues[n].account_id for n in names})
        for c in [c for c in self.ctxs if c.pair == self.active]:
            for n in names:
                why = self.claims.acquire(n, self.venues[n].account_id, c.sym.key)
                if why:
                    c.claim_block = why
                    c.status = f"skipped: {why}"
                    lvl = logging.ERROR if not self.position(c).is_flat() else logging.WARNING
                    self.event(f"{c.sym.key} skipped: {why}", lvl)
                    break
        if not self.active_ctxs():
            raise ConfigError("every symbol of this pair is owned by another running instance")
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
        """Stop all new orders, cancel every maker, keep positions, lock until manual intervention."""
        if not self.halted:
            self.halted = reason
            self.event(f"HALTED: {reason}", logging.ERROR)
            self.journal("halt", reason=reason)
            try:
                self._write_lock(reason)
            except Exception as exc:  # noqa: BLE001
                log.error("could not write manual-intervention lock: %s", exc)
            if self.live and self.active:
                try:
                    asyncio.get_running_loop().create_task(self._cancel_all_makers())
                except RuntimeError:
                    pass
            print("\a", end="", flush=True)

    async def _cancel_all_makers(self) -> None:
        for vname in (self.active.maker, self.active.taker):
            syms = [c.maker_sym if vname == self.active.maker else c.taker_sym for c in self.active_ctxs()]
            try:
                await self.venues[vname].cancel_all(syms)
            except Exception as exc:  # noqa: BLE001
                log.warning("cancel_all after halt failed for %s: %s", vname, exc)

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
        running = [t for t in self.exec_tasks.values() if not t.done()]
        if running:
            self.event(f"waiting for {len(running)} in-flight execution(s) to finish ...")
            try:
                await asyncio.wait_for(asyncio.shield(asyncio.gather(*running, return_exceptions=True)), timeout=60)
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
        self._drain_alarms()
        self.journal("uptime", symbols={c.key: round(c.ready_s / max(time.time() - self.started, 1e-9), 4) for c in self.ctxs})
        if self.claims:
            self.claims.release_all()

    async def _tick_loop(self) -> None:
        while not self.stopping:
            now = time.time()
            for c in self.ctxs:
                try:
                    self.compute(c, now)
                except Exception as exc:  # noqa: BLE001
                    c.status = f"error: {exc}"
                    log.exception("compute %s", c.key)
            for k in [k for k, t in self.exec_tasks.items() if t.done()]:
                self.exec_tasks.pop(k)
                self.inflight.pop(k, None)
            if self.live and not self.halted and not self.stopping and not self.reconcile_due:
                for c in self.active_ctxs():
                    if len(self.exec_tasks) >= self.cfg.max_parallel_slices:
                        break
                    if c.key in self.exec_tasks:
                        continue
                    plan = self.decide(c, now)
                    if plan is not None:
                        self.inflight[c.key] = plan
                        self.exec_tasks[c.key] = asyncio.create_task(self._execute(c, plan))
            self._drain_alarms()
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
            if self.live and self.active and now - last_rec >= rec_iv:
                self.reconcile_due = True  # stop launching slices; reconcile once the running ones are done
            if self.live and self.active and self.reconcile_due and not self.busy:
                last_rec = now
                try:
                    await self.reconcile()
                except Exception as exc:  # noqa: BLE001
                    self.reconciled_ok = False
                    log.warning("reconcile failed: %s", exc)
                if self.reconciled_ok and not self.halted:
                    await self._fix_residuals()
                self.reconcile_due = False
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
        dt = now - c.last_tick if c.last_tick else 0.0
        c.last_tick = now
        if c.ready and dt < 5:
            c.ready_s += dt
        if not fresh:
            c.q_sell = c.q_buy = None
            c.ready = False
            c.status = "no fresh quotes"
            if c.basis:
                c.basis.observe(now, None)
            return
        # readiness warmup: continuous fresh data on both venues. A short gap on the same socket
        # generation (< quick_resume_s) keeps the window; a reconnect / sequence gap / long gap restarts it.
        ep = (M.epoch(c.maker_sym), T.epoch(c.taker_sym))
        if c.fresh_since is None or ep != c.epochs or now - c.last_fresh > rk.quick_resume_s:
            if c.fresh_since is not None and ep != c.epochs:
                self.journal("warmup_reset", symbol=c.sym.key, why="generation_change")
            c.fresh_since = now
            c.epochs = ep
        c.last_fresh = now
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
        warm = now - c.fresh_since
        if c.ready and warm < rk.warmup_fresh_s:
            c.ready, reason = False, f"warmup {warm:.0f}/{rk.warmup_fresh_s:.0f}s fresh data"
        c.status = "ready" if c.ready else reason
        if c.claim_block:
            c.status = f"skipped: {c.claim_block}"

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
        thr = ex.maker_price_mode == "threshold"
        cands: list[tuple[float, ExecPlan]] = []  # (gap to trigger, plan); gap <= 0 means signal now

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
            q = sg.build_quote(cdir, mb, tb, qty, c.basis_value, ex.maker_price_mode if not thr else "join",
                               M.price_tick_at(c.maker_sym, mb.mid), rk.depth_levels, rk.depth_band_pct, maker_as_taker=tt)
            floor = None
            if st.min_close_profit_pct is not None:
                floor = top.close_floor(qty, now, st.close_floor_timeout_s, st.min_close_profit_pct)
            if q.ok and qty >= min_qty:
                gap = (-q.dev) - cl  # close when -dev <= cl
                if floor is not None:
                    gap = max(gap, floor - q.raw)
                plan = ExecPlan("close", c.pair, c.sym, c.maker_sym, c.taker_sym, cdir, qty, top.level, -cl, basis_fn,
                                taker_taker=tt, required_raw=floor)
                if not tt and plan.qty + self._carry(c, sp, cdir) <= sp.total_qty:
                    plan.carry_in = self._carry(c, sp, cdir)
                if gap <= 0 and not thr:
                    if not self._stable(c):
                        c.status = "close signal, prices unstable"
                        return None
                    return plan
                if floor is not None and gap > 0 and -q.dev <= cl:
                    c.status = f"close held by profit floor (raw {q.raw:.3f}% < {floor:.3f}%)"
                cands.append((gap, plan))

        # ---------------- open
        if not (self.cfg.close_only or c.sym.close_only):
            fees = self._fees(c)
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
                qty = self._rq(c, min(c.qty, remaining))
                if qty < min_qty:
                    continue
                plan = ExecPlan("open", c.pair, c.sym, c.maker_sym, c.taker_sym, q.direction, qty, level, need, basis_fn,
                                target=target)
                plan.carry_in = self._carry(c, sp, q.direction)
                cands.append((need - q.dev, plan))
        if not cands:
            return None
        if thr:
            gap, plan = min(cands, key=lambda x: x[0])
            if ex.threshold_max_gap_pct is not None and gap > ex.threshold_max_gap_pct:
                return None
        else:
            opens = [x for x in cands if x[1].kind == "open" and x[0] <= 0]
            if not opens:
                return None
            gap, plan = min(opens, key=lambda x: x[0])
        if not self._stable(c):
            c.status = f"{plan.kind} signal, prices unstable"
            return None
        if plan.kind == "open":
            why = self._open_blocked(c, plan, now)
            if why:
                c.status = f"blocked: {why}"
                return None
        return plan

    def _carry(self, c: Ctx, sp: SymbolPosition, direction: str) -> Decimal:
        """Unhedged maker dust with the same sign as this slice's maker fills is hedged together with it."""
        s_m = 1 if sg.maker_is_buy(direction) else -1
        if sp.residual_taker == 0 and sp.residual_maker * s_m > 0:
            return abs(sp.residual_maker)
        return ZERO

    def _open_blocked(self, c: Ctx, plan: ExecPlan, now: float) -> str:
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        rk = c.sym.risk
        sp = self.position(c)
        ok, why = self._balances_ok(c, now)
        if not ok:
            return why
        px = float(max(M.book(c.maker_sym).mid, T.book(c.taker_sym).mid))
        if float(sp.total_qty + plan.qty) * px > rk.max_symbol_value_usdc * 1.0001:
            return "symbol cap"
        add = float(plan.qty) * px
        for n in (c.pair.maker, c.pair.taker):
            gross = self._venue_notional(n) + self._inflight_notional(n) + add
            if gross > rk.max_venue_notional_usdc:
                return f"{n} notional cap"
            b = self.balances.get(n)
            if b is not None and float(b.equity) > 0:
                usable = float(b.equity) * (1 - rk.reserve_ratio)
                if gross > usable * rk.max_leverage:
                    return f"{n} leverage cap ({gross:.0f} > {rk.max_leverage:g}x {usable:.0f})"
        if self._unhedged_notional() > rk.max_unhedged_notional_usdc:
            return f"unhedged exposure {self._unhedged_notional():.0f} > {rk.max_unhedged_notional_usdc:.0f} USDC"
        return ""

    def _inflight_notional(self, venue: str) -> float:
        total = 0.0
        for k, p in self.inflight.items():
            if p.kind != "open" or k not in self.exec_tasks:
                continue
            v = self.venues[venue]
            b = v.book(p.maker_sym if venue == p.pair.maker else p.taker_sym)
            if b and b.mid:
                total += float(p.qty * b.mid)
        return total

    def _unhedged_notional(self) -> float:
        total = 0.0
        for c in self.active_ctxs():
            sp = self.position(c)
            for vname, sym, r in ((c.pair.maker, c.maker_sym, sp.residual_maker), (c.pair.taker, c.taker_sym, sp.residual_taker)):
                b = self.venues[vname].book(sym)
                if r and b and b.mid:
                    total += abs(float(r * b.mid))
        return total

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
        sp.residual_maker -= s_m * res.carry_in  # carried dust is now part of this slice
        sp.residual_maker += s_m * (res.maker_net + res.carry_in - hedged)
        sp.residual_taker += -s_m * (res.taker_filled - hedged)
        dev = res.realized_dev(plan.direction, plan.basis_fn())
        raw = (sg.raw_edge(plan.direction, res.maker_avg, res.taker_avg)
               if res.maker_avg is not None and res.taker_avg is not None else None)
        pnl = None
        if plan.kind == "open":
            if hedged > 0:
                sp.add_open(sg.POSITION_OF[plan.direction], plan.level, hedged, plan.target or hedged, dev or 0.0, raw=raw)
        else:
            if hedged > 0 and sp.grids:
                g = sp.grids[-1]
                od = g.open_dev or 0.0
                px = float(res.taker_avg or 0)
                sp.realized_edge_usdc += (od + (dev or 0.0)) / 100 * float(hedged) * px
                left, wq, wr = hedged, 0.0, 0.0  # FIFO lots consumed by this close
                for q, r, _ in g.lots:
                    if left <= 0:
                        break
                    part = min(q, left)
                    left -= part
                    if r is not None:
                        wq, wr = wq + float(part), wr + float(part) * r
                if wq > 0 and raw is not None:
                    pnl = (wr / wq + raw) / 100 * float(hedged) * px  # gross two-leg, before fees/funding
            sp.reduce_close(hedged)
        self.positions.save()
        self.journal(plan.kind, symbol=c.sym.key, direction=plan.direction, level=plan.level, qty=plan.qty,
                     maker_filled=res.maker_filled, maker_avg=res.maker_avg, taker_filled=res.taker_filled,
                     taker_avg=res.taker_avg, hedged=hedged, carry_in=res.carry_in, raw_edge=raw, adj_edge=dev,
                     required=plan.required_dev, floor=plan.required_raw, gross_pnl_usdc=pnl, note=res.note,
                     requotes=res.requotes, hedge_orders=res.hedge_orders, zero_fill_hedges=res.zero_fill_hedges,
                     unwound=res.maker_unwound, error=res.error or None,
                     residual_maker=sp.residual_maker, residual_taker=sp.residual_taker)
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
                if vname == c.pair.maker and qty * b.mid < Decimal(str(c.sym.execution.hedge_min_notional_usdc)):
                    continue  # maker dust waits to be hedged together with the next same-direction slice
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
