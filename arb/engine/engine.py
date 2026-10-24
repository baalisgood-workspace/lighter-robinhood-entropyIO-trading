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
from arb.engine.guards import Guards
from arb.engine.state import PositionBook, SymbolPosition
from arb.models import Balance, Position
from arb.util import ceil_to_step
from arb.venues.base import Venue

log = logging.getLogger("arb.engine")
ZERO = Decimal("0")


SAMPLE_COLS = ["ts", "pair", "maker", "taker", "symbol", "mid_maker", "mid_taker", "basis_now", "basis_ema", "basis_ready",
               "raw_sell_maker", "dev_sell_maker", "raw_buy_maker", "dev_buy_maker", "qty",
               # v5 (arb rank): open mode of the dev columns, T1, round-trip fees, adverse funding per direction,
               # taker-taker edges, liquidity EMAs, NY regular hours flag, category
               "open_mode", "t1", "fee_mt", "fee_tt", "fund_sell", "fund_buy", "tt_dev_sell", "tt_dev_buy",
               "book_spread_pct", "depth_usdc", "rth", "category"]


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
    # guards (arb/engine/guards.py)
    vol_mids: deque = field(default_factory=deque)  # (ts, taker mid), at most one sample per second
    vol_amp: float = 0.0
    vol_block_until: float = 0.0
    ro_block_until: float = 0.0
    signal_since: Optional[float] = None
    pause_reason: str = ""
    opens_day: str = ""
    opens_today: int = 0
    adverse_since: Optional[float] = None
    warn: str = ""
    # v5: trade policy (trade | filter | monitor) and live liquidity filter
    policy: str = "trade"
    policy_why: str = ""
    liq_spread: Optional[float] = None  # EMA of the widest book spread (%) of the two legs
    liq_depth: Optional[float] = None  # EMA of the thinnest side depth (USDC) of the two legs
    liq_since: Optional[float] = None
    liq_last: float = 0.0
    need_sell: Optional[float] = None  # open trigger incl. fees + funding (flat: level 1), per direction
    need_buy: Optional[float] = None
    gap: Optional[float] = None  # min(need - dev) over directions: <= 0 means an open signal
    unclosable: str = ""  # v5.3: why the top grid cannot be closed right now (warned once per change)
    unclosable_since: Optional[float] = None  # v5.6: start of the current unclosable stretch (dust clean-up grace)
    dust_next: float = 0.0  # v5.6: no dust clean-up attempt before this time (after a failed / refused one)
    dust_note: str = ""  # v5.6: last "too large for clean-up" warning (warned once per change)
    skip_logged: dict = field(default_factory=dict)  # v5.6: reason -> last time a skipped signal was logged

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
        self._last_mismatch: dict[str, tuple] = {}  # ctx key -> (maker delta, taker delta) of the previous check
        self._adoptions: dict[str, deque] = {}  # ctx key -> timestamps of auto-adopted mismatches
        self.reconciled_ok = False
        self.exec_tasks: dict[str, asyncio.Task] = {}  # ctx key -> in-flight slice
        self.inflight: dict[str, ExecPlan] = {}
        self.reconcile_due = False  # barrier: no new slices until the running ones finished and we reconciled
        self.executor = Executor(venues, stop_flag=lambda: self.stopping or bool(self.halted))
        self._markout_tasks: set[asyncio.Task] = set()
        if live:
            self.executor.on_fill = self._on_fill
        self.started = time.time()
        self.lock_path = data / "MANUAL_INTERVENTION.json"
        self.journal_path = data / "events.jsonl"
        # dashboard state
        self.trades: deque[dict] = deque(maxlen=50)
        self.last_check: dict = {}  # last reconcile: ts, ok, rows
        self.equity0: dict[str, float] = {}
        self.tick_ms = 0.0
        self.tick_max_ms = 0.0
        self.ticks = 0
        self.guards = Guards(self)
        self.pause_text = ""  # active instance-wide pause (dashboard)
        self._load_trades()

    def _load_trades(self) -> None:
        if not self.journal_path.exists():
            return
        try:
            with self.journal_path.open() as f:
                for line in f:
                    if '"kind": "open"' in line or '"kind": "close"' in line:
                        try:
                            self.trades.append(json.loads(line))
                        except ValueError:
                            pass
        except OSError:
            pass

    # ------------------------------------------------------------ journal / lock
    def journal(self, kind: str, **payload) -> None:
        """Append-only JSONL diagnostics (fills, closes, halts, market-health alarms)."""
        try:
            self.journal_path.parent.mkdir(parents=True, exist_ok=True)
            rec = {"ts": round(time.time(), 3), "kind": kind, "pair": self.active.key if self.active else None, **payload}
            if kind in ("open", "close"):
                self.trades.append(json.loads(json.dumps(rec, default=str)))
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
        self._assign_policies()

    def _assign_policies(self) -> None:
        """Which symbols may OPEN: configured (and auto_symbols.include) always; auto ones per auto_symbols.trade."""
        a = self.cfg.auto_symbols
        include = {str(x).upper() for x in a.include}
        ranked: Optional[set] = None
        if a.trade == "ranked" and any(c.sym.auto for c in self.ctxs):
            ranked = set()
            path = Path(self.cfg.data_dir) / "rank.json"
            try:
                data = json.loads(path.read_text())
                age_h = (time.time() - float(data.get("ts", 0))) / 3600
                if age_h > a.rank_max_age_h:
                    self.event(f"WARNING {path} is {age_h:.0f}h old - run `arb rank --save` again", logging.WARNING)
                for pair in {c.pair for c in self.ctxs}:
                    rows = [r for r in data.get("rows", []) if r.get("pair") == pair.key and r.get("maker") == pair.maker
                            and float(r.get("score") or 0) > 0]
                    rows.sort(key=lambda r: -float(r["score"]))
                    ranked |= {(pair.key, r["symbol"]) for r in rows[:a.ranked_top_n]}
            except (OSError, ValueError) as exc:
                self.event(f"WARNING auto_symbols.trade=ranked but {path} unusable ({exc}); auto symbols monitor only",
                           logging.WARNING)
        for c in self.ctxs:
            if not c.sym.auto or c.sym.key.upper() in include:
                c.policy, c.policy_why = ("filter" if a.filter_configured else "trade"), ""
            elif a.trade == "filtered":
                c.policy, c.policy_why = "filter", ""
            elif a.trade == "ranked" and ranked is not None and (c.pair.key, c.sym.key) in ranked:
                c.policy, c.policy_why = "filter", "ranked"
            else:
                c.policy = "monitor"
                c.policy_why = "auto symbol, not in rank top N" if a.trade == "ranked" else "auto symbol (trade: none)"

    def liquidity_ok(self, c: Ctx, now: float) -> tuple[bool, str]:
        a = self.cfg.auto_symbols
        if c.liq_since is None or now - c.liq_since < 30:
            return False, "liquidity filter warming up"
        if a.max_book_spread_pct is not None and (c.liq_spread is None or c.liq_spread > a.max_book_spread_pct):
            return False, f"book spread {c.liq_spread or 0:.3f}% > {a.max_book_spread_pct:g}%"
        if a.min_depth_usdc is not None and (c.liq_depth is None or c.liq_depth < a.min_depth_usdc):
            return False, f"depth {c.liq_depth or 0:.0f} < {a.min_depth_usdc:.0f} USDC"
        return True, ""

    def _observe_liquidity(self, c: Ctx, now: float, mb, tb) -> None:
        levels = c.sym.risk.depth_levels
        spreads, depths = [], []
        for b in (mb, tb):
            if b.best_bid is None or b.best_ask is None or not b.mid:
                return
            spreads.append(float((b.best_ask.price - b.best_bid.price) / b.mid * 100))
            for side in (b.bids, b.asks):
                depths.append(float(sum(lv.price * lv.size for lv in side[:levels])))
        sp, dp = max(spreads), min(depths)
        dt = now - c.liq_last if c.liq_last else 0.0
        c.liq_last = now
        if c.liq_since is None or c.liq_spread is None or dt > 60:
            c.liq_since, c.liq_spread, c.liq_depth = now, sp, dp
            return
        k = min(1.0, dt / 60.0)
        c.liq_spread += (sp - c.liq_spread) * k
        c.liq_depth += (dp - c.liq_depth) * k

    def active_ctxs(self) -> list[Ctx]:
        # monitor-only auto symbols are not traded, reconciled or counted while flat (they only show prices)
        return [c for c in self.ctxs if self.active and c.pair == self.active and not c.claim_block
                and not (c.policy == "monitor" and not self.position(c).grids)]

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
            if c.policy == "monitor" and not self.position(c).grids:
                continue  # monitor-only auto symbol: never opens, leave it to other instances
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
        deltas: list[tuple] = []  # (ctx, maker delta, taker delta, tolerance)
        rows = []
        for c in self.active_ctxs():
            sp = self.position(c)
            am = pm.get(c.maker_sym).qty if c.maker_sym in pm else ZERO
            at = pt.get(c.taker_sym).qty if c.taker_sym in pt else ZERO
            tol = Decimal(str(c.sym.risk.reconcile_tolerance_mult)) * max(M.market(c.maker_sym).min_qty, T.market(c.taker_sym).min_qty)
            dm, dt = am - sp.expected_maker_qty(), at - sp.expected_taker_qty()
            if initial and c.sym.auto and sp.is_flat() and (am or at):
                # auto-added symbol with a position this bot never opened (manual / other program): leave it alone
                c.claim_block = f"exchange position not opened by this bot ({M.name} {am}, {T.name} {at})"
                c.status = f"skipped: {c.claim_block}"
                self.event(f"WARNING {c.sym.key}: {c.claim_block} - symbol skipped", logging.WARNING)
                continue
            if am or at or not sp.is_flat():
                rows.append(dict(symbol=c.sym.key, maker=am, maker_exp=sp.expected_maker_qty(), taker=at,
                                 taker_exp=sp.expected_taker_qty(), ok=abs(dm) <= tol and abs(dt) <= tol))
            if abs(dm) > tol or abs(dt) > tol:
                deltas.append((c, dm, dt, tol))
                problems.append(f"{c.sym.key}: {M.name} {am} (expected {sp.expected_maker_qty()}), "
                                f"{T.name} {at} (expected {sp.expected_taker_qty()})")
        self.last_check = dict(ts=time.time(), ok=not problems, rows=rows, problems=problems)
        if initial and problems:
            self.journal("startup_position_mismatch", problems=problems, rows=rows)
            msg = "exchange positions do not match local state:\n  " + "\n  ".join(problems)
            if not self.cfg.allow_preexisting_positions:
                raise ConfigError(msg + "\nUse dedicated accounts, flatten manually, or set trading.allow_preexisting_positions: true.")
            self.event("WARNING " + msg.replace("\n", " "), logging.WARNING)
        if problems and not initial:
            self.mismatch_strikes += 1
            self.event(f"position mismatch ({self.mismatch_strikes}): {'; '.join(problems)}", logging.WARNING)
            if self.mismatch_strikes >= 2 and not self.cfg.allow_preexisting_positions:
                why = self._adopt_mismatches(deltas)
                if why:
                    self.halt("positions do not match local state: " + "; ".join(problems) + f" [{why}]")
                else:
                    self.mismatch_strikes = 0
                    problems = []
        elif not problems:
            self.mismatch_strikes = 0
        self._last_mismatch = {c.key: (dm, dt) for c, dm, dt, _ in deltas} if problems else {}
        self.reconciled_ok = not problems
        return not problems

    def _adopt_mismatches(self, deltas: list[tuple]) -> str:
        """v5.2 auto-align (opt-in). Adopt every mismatch as residual if ALL are adoptable; else return why not.
        Adoptable: only one leg differs (the other matches local state), the same delta was seen on the
        previous check, its value is <= risk.auto_adopt_max_usdc and the hourly budget is not used up."""
        assert self.active is not None
        now = time.time()
        plan = []
        for c, dm, dt, tol in deltas:
            rk = c.sym.risk
            if rk.auto_adopt_max_usdc <= 0:
                return "auto-align off (risk.auto_adopt_max_usdc: 0)"
            if abs(dm) > tol and abs(dt) > tol:
                return f"{c.sym.key}: both legs differ"
            if self._last_mismatch.get(c.key) != (dm, dt):
                return f"{c.sym.key}: mismatch not identical on two checks"
            maker_leg = abs(dm) > tol
            vname, sym, d = (c.pair.maker, c.maker_sym, dm) if maker_leg else (c.pair.taker, c.taker_sym, dt)
            b = self.venues[vname].book(sym)
            px = b.mid if b is not None and b.mid else None
            if px is None:
                return f"{c.sym.key}: no price on {vname}"
            value = float(abs(d) * px)
            if value > rk.auto_adopt_max_usdc:
                return f"{c.sym.key}: {value:.2f} USDC > auto_adopt_max_usdc {rk.auto_adopt_max_usdc:g}"
            hist = self._adoptions.setdefault(c.key, deque())
            while hist and now - hist[0] > 3600:
                hist.popleft()
            if len(hist) >= rk.auto_adopt_max_per_hour:
                return f"{c.sym.key}: auto_adopt_max_per_hour {rk.auto_adopt_max_per_hour} used up"
            plan.append((c, maker_leg, vname, sym, d, value))
        for c, maker_leg, vname, sym, d, value in plan:
            sp = self.position(c)
            attr = "residual_maker" if maker_leg else "residual_taker"
            setattr(sp, attr, getattr(sp, attr) + d)
            self.venues[vname].drop_retired(sym)  # a later push for those orders must not be counted again
            self._adoptions[c.key].append(now)
            self.journal("adopt_mismatch", symbol=c.sym.key, venue=vname, delta=d, value_usdc=round(value, 4),
                         residual_maker=sp.residual_maker, residual_taker=sp.residual_taker)
            self.event(f"WARNING {c.sym.key}: {vname} position differs from local state by {d} ({value:.2f} USDC)"
                       f" - adopted as residual (auto-align)", logging.WARNING)
        self.positions.save()
        return ""

    def _book_late_fills(self) -> None:
        """v5.2: fills of orders the executor had already finished are booked as residual of their leg."""
        if self.active is None:
            return
        for vname in (self.active.maker, self.active.taker):
            v = self.venues.get(vname)
            if v is None:
                continue
            for f in v.drain_late_fills():
                signed = f["qty"] if f["is_buy"] else -f["qty"]
                ctx = next((c for c in self.active_ctxs() if
                            (vname == c.pair.maker and f["symbol"] == c.maker_sym) or
                            (vname == c.pair.taker and f["symbol"] == c.taker_sym)), None)
                self.journal("late_fill", venue=vname, symbol=f["symbol"], qty=signed, price=f["price"],
                             client_id=f["client_id"], booked=ctx is not None)
                if ctx is None:
                    self.event(f"WARNING late fill {f['symbol']} {signed} on {vname} for an untracked symbol", logging.WARNING)
                    continue
                sp = self.position(ctx)
                if vname == ctx.pair.maker:
                    sp.residual_maker += signed
                else:
                    sp.residual_taker += signed
                self.positions.save()
                self.reconcile_due = True  # check positions and settle the residual soon
                self.event(f"WARNING {ctx.sym.key}: late fill {signed} on {vname} (order {f['client_id']} had already "
                           f"finished) - booked as residual", logging.WARNING)

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
        await self._drain_markouts()
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
            t0 = time.perf_counter()
            for c in self.ctxs:
                try:
                    self.compute(c, now)
                except Exception as exc:  # noqa: BLE001
                    c.status = f"error: {exc}"
                    log.exception("compute %s", c.key)
            for k in [k for k, t in self.exec_tasks.items() if t.done()]:
                self.exec_tasks.pop(k)
                self.inflight.pop(k, None)
            try:
                self._book_late_fills()
            except Exception:  # noqa: BLE001
                log.exception("late fill booking")
            self.guards.tick(now)
            self.pause_text = self.guards.global_reason(now)
            if self.live and self.pause_text and not self.halted:
                for c in self.active_ctxs():  # v5.6: signals missed while every open is paused
                    if c.gap is not None and c.gap <= 0 and not (self.cfg.close_only or c.sym.close_only):
                        self._skip(c, now, "global pause", None, None, None, self.pause_text)
            if self.live and not self.halted and not self.stopping and not self.reconcile_due and not self.pause_text:
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
            ms = (time.perf_counter() - t0) * 1000
            self.ticks += 1
            self.tick_ms = ms if self.ticks == 1 else self.tick_ms * 0.95 + ms * 0.05
            self.tick_max_ms = max(self.tick_max_ms * 0.999, ms)
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
                            self.equity0.setdefault(n, float(self.balances[n].equity))
                        except Exception as exc:  # noqa: BLE001
                            log.debug("balance %s: %s", n, exc)
                self.guards.on_balances(now)
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
        while not self.stopping:
            await asyncio.sleep(self.cfg.sampler_interval_s)
            day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            path = self.sample_dir / f"{day}.csv"
            if path.exists():
                with path.open() as fh:
                    if fh.readline().strip().split(",") != SAMPLE_COLS:  # older layout: keep it, write a v5 file
                        path = self.sample_dir / f"{day}_v5.csv"
            new = not path.exists()
            rows = [r for r in (self.sample_row(c) for c in self.ctxs) if r]
            if rows:
                with path.open("a", newline="") as f:
                    w = csv.writer(f)
                    if new:
                        w.writerow(SAMPLE_COLS)
                    w.writerows(rows)

    def sample_row(self, c: Ctx) -> Optional[list]:
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        mb, tb = M.book(c.maker_sym), T.book(c.taker_sym)
        if not (mb and tb and mb.mid and tb.mid):
            return None
        rk, st, ex = c.sym.risk, c.sym.strategy, c.sym.execution
        ot = ex.open_mode == "taker_taker"
        tick = M.price_tick_at(c.maker_sym, mb.mid)

        def other(d: str, cur):  # the edge of the other open mode (both are recorded for `arb rank --mode`)
            if c.qty <= 0:
                return None
            return sg.build_quote(d, mb, tb, c.qty, c.basis_value, ex.maker_price_mode, tick, rk.depth_levels,
                                  rk.depth_band_pct, maker_as_taker=not ot)
        qs, qb = c.q_sell, c.q_buy
        os_, ob_ = other(sg.SELL_MAKER, qs), other(sg.BUY_MAKER, qb)
        mt_s, mt_b, tt_s, tt_b = (os_, ob_, qs, qb) if ot else (qs, qb, os_, ob_)
        tt_dev = [_f(tt_s.dev if tt_s else None), _f(tt_b.dev if tt_b else None)]
        from arb.config import us_regular_hours
        return [f"{time.time():.3f}", c.pair.key, c.pair.maker, c.pair.taker, c.sym.key, str(mb.mid), str(tb.mid),
                f"{sg.basis_value(mb.mid, tb.mid):.5f}", f"{c.basis_value:.5f}", int(c.ready),
                _f(mt_s.raw if mt_s else None), _f(mt_s.dev if mt_s else None),
                _f(mt_b.raw if mt_b else None), _f(mt_b.dev if mt_b else None), str(c.qty),
                ex.open_mode, f"{effective_t1(st):.4f}",
                f"{self._fees(c, 'maker_taker'):.5f}", f"{self._fees(c, 'taker_taker'):.5f}",
                _f(self._funding(c, sg.SELL_MAKER)), _f(self._funding(c, sg.BUY_MAKER)), *tt_dev,
                _f(c.liq_spread), "" if c.liq_depth is None else f"{c.liq_depth:.0f}", int(us_regular_hours()),
                c.sym.category or ("configured" if not c.sym.auto else "")]

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
        self.guards.observe(c, now, tm)
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
        ot = ex.open_mode == "taker_taker"  # open quotes priced as immediate fills on both books
        c.q_sell = sg.build_quote(sg.SELL_MAKER, mb, tb, c.qty, c.basis_value, ex.maker_price_mode, tick, rk.depth_levels,
                                  rk.depth_band_pct, maker_as_taker=ot)
        c.q_buy = sg.build_quote(sg.BUY_MAKER, mb, tb, c.qty, c.basis_value, ex.maker_price_mode, tick, rk.depth_levels,
                                 rk.depth_band_pct, maker_as_taker=ot)
        self._observe_liquidity(c, now, mb, tb)
        self._preview_need(c, now)
        warm = now - c.fresh_since
        if c.ready and warm < rk.warmup_fresh_s:
            c.ready, reason = False, f"warmup {warm:.0f}/{rk.warmup_fresh_s:.0f}s fresh data"
        c.status = "ready" if c.ready else reason
        if c.claim_block:
            c.status = f"skipped: {c.claim_block}"

    # ------------------------------------------------------------ decide
    def _fees(self, c: Ctx, open_mode: Optional[str] = None, close_mode: Optional[str] = None) -> float:
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        ex = c.sym.execution
        tt = (close_mode or ex.close_mode) == "taker_taker"
        ot = (open_mode or ex.open_mode) == "taker_taker"
        return sg.roundtrip_fee_pct(M.maker_fee_pct(c.maker_sym), T.taker_fee_pct(c.taker_sym), tt, M.taker_fee_pct(c.maker_sym),
                                    open_taker_taker=ot)

    def _preview_need(self, c: Ctx, now: float) -> None:
        """Open trigger of the next grid per direction (monitor + dashboard sorting + samples)."""
        st = c.sym.strategy
        sp = self.position(c)
        level = min(len(sp.grids) + 1, st.max_grids)
        base = sg.open_level(effective_t1(st, now), st.grid_step_pct, level) + self._fees(c)
        c.need_sell = base + self._funding(c, sg.SELL_MAKER)
        c.need_buy = base + self._funding(c, sg.BUY_MAKER)
        gaps = []
        for q, need in ((c.q_sell, c.need_sell), (c.q_buy, c.need_buy)):
            lock = sp.direction if sp.grids else None  # v5.6: residual dust without grids does not lock a direction
            if q is not None and q.ok and (not lock or lock == sg.POSITION_OF[q.direction]):
                gaps.append(need - q.dev)
        c.gap = min(gaps) if gaps else None

    def _funding_hourly(self, c: Ctx, direction: str) -> Optional[float]:
        """Adverse funding of holding `direction`, percent per hour (None if a rate is unknown)."""
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        rm, rt = M.funding_hourly.get(c.maker_sym), T.funding_hourly.get(c.taker_sym)
        if rm is None or rt is None:
            return None
        return sg.adverse_funding_pct(direction, rm, rt, 1.0)

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
        if now < c.paused_until:
            c.status = f"paused {c.paused_until - now:.0f}s" + (f": {c.pause_reason}" if c.pause_reason else "")
            return None
        if now < c.cooldown_until:
            return None
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        st, rk, ex = c.sym.strategy, c.sym.risk, c.sym.execution
        sp = self.position(c)
        t1 = effective_t1(st, now)
        min_qty = max(M.market(c.maker_sym).min_qty, T.market(c.taker_sym).min_qty)
        sp.merge_dust(min_qty)
        if not sp.grids:
            c.unclosable, c.unclosable_since = "", None
        lock = sp.direction if sp.grids else None  # v5.6: residual dust without grids does not lock a direction
        basis_fn = (lambda: c.basis.ema or 0.0) if c.basis else (lambda: 0.0)
        tt = ex.close_mode == "taker_taker"
        thr = ex.maker_price_mode == "threshold"
        cands: list[tuple[float, ExecPlan]] = []  # (gap to trigger, plan); gap <= 0 means signal now

        # ---------------- close (highest grid first)
        if sp.grids:
            top = sp.grids[-1]
            cdir = sg.CLOSE_DIRECTION[sp.direction]
            # v5.3: maker fills of an earlier close whose hedge is still missing (close-sign maker residual) are
            # hedged together with this close; the maker order only covers what is still open on the maker venue
            pend = ZERO if tt else min(self._carry(c, sp, cdir), sp.total_qty)
            top_open = top.qty - pend
            qty = min(c.qty, self._rq(c, max(ZERO, top_open)))
            rem = top_open - qty
            if rem < min_qty or not self._sized_ok(c, rem):  # do not leave an untradable remainder
                qty = self._rq(c, max(ZERO, top_open))
            cl = sg.close_level(t1, st.grid_step_pct, st.first_close_ratio, top.level)
            c.next_close = cl
            mb, tb = M.book(c.maker_sym), T.book(c.taker_sym)
            why = ""
            if pend > 0 and qty < min_qty:
                why = f"close hedge pending: {pend} on {c.pair.taker} (paired off after the reconcile)"
            elif not self._taker_ok(c, qty + pend):
                why = (f"not closable: {qty + pend} {c.taker_sym} is below the {c.pair.taker} minimum order "
                       f"({T.market(c.taker_sym).min_qty} / {T.market(c.taker_sym).min_notional} USDC)")
            if why != c.unclosable:
                if why:
                    self.event(f"{c.sym.key}: {why}", logging.WARNING)
                c.unclosable = why
            if not why:
                c.unclosable_since = None
            elif c.unclosable_since is None:
                c.unclosable_since = now
            q = None
            if why:
                c.status = why
            else:
                q = sg.build_quote(cdir, mb, tb, qty, c.basis_value, ex.maker_price_mode if not thr else "join",
                                   M.price_tick_at(c.maker_sym, mb.mid), rk.depth_levels, rk.depth_band_pct, maker_as_taker=tt)
            floor = None
            if q is not None and st.min_close_profit_pct is not None:
                floor = top.close_floor(qty, now, st.close_floor_timeout_s, st.min_close_profit_pct)
            if q is not None and q.ok and qty >= min_qty:
                gap = (-q.dev) - cl  # close when -dev <= cl
                if floor is not None:
                    gap = max(gap, floor - q.raw)
                plan = ExecPlan("close", c.pair, c.sym, c.maker_sym, c.taker_sym, cdir, qty, top.level, -cl, basis_fn,
                                taker_taker=tt, required_raw=floor)
                plan.carry_in = pend
                if gap <= 0 and not thr:
                    if not self._stable(c):
                        c.status = "close signal, prices unstable"
                        return None
                    return plan
                if floor is not None and gap > 0 and -q.dev <= cl:
                    c.status = f"close held by profit floor (raw {q.raw:.3f}% < {floor:.3f}%)"
                cands.append((gap, plan))

        # ---------------- open
        topen: list[tuple[float, ExecPlan]] = []
        fees = self._fees(c)
        if self.cfg.close_only or c.sym.close_only:
            for q in (c.q_sell, c.q_buy):  # v5.6: only journaled (intended), so `why_idle` can tell it apart
                if q.ok and (not lock or lock == sg.POSITION_OF[q.direction]):
                    need = sg.open_level(t1, st.grid_step_pct, min(len(sp.grids) + 1, st.max_grids)) + fees + self._funding(c, q.direction)
                    if q.dev >= need:
                        self._skip(c, now, "close_only", q.direction, q.dev, need, log_it=False)
        else:
            for q in (c.q_sell, c.q_buy):
                if not q.ok:
                    continue
                pos_dir = sg.POSITION_OF[q.direction]
                funding = self._funding(c, q.direction)
                if lock and lock != pos_dir:
                    need1 = sg.open_level(t1, st.grid_step_pct, 1) + fees + funding
                    if q.dev >= need1:
                        self._skip(c, now, "opposite position held", q.direction, q.dev, need1,
                                   f"{lock} {sp.total_qty} in {len(sp.grids)} grid(s)")
                    continue
                if sp.grids and sp.grids[-1].qty < sp.grids[-1].target_qty - min_qty:
                    level, target = sp.grids[-1].level, sp.grids[-1].target_qty
                    remaining = target - sp.grids[-1].qty
                else:
                    level = len(sp.grids) + 1
                    if level > st.max_grids:
                        needx = sg.open_level(t1, st.grid_step_pct, level) + fees + funding
                        if q.dev >= needx:
                            self._skip(c, now, "max_grids reached", q.direction, q.dev, needx, f"max_grids {st.max_grids}")
                        continue
                    target = self._rq(c, Decimal(str(st.grid_value_usdc)) / max(M.book(c.maker_sym).mid, T.book(c.taker_sym).mid))
                    remaining = target
                need = sg.open_level(t1, st.grid_step_pct, level) + fees + funding
                if c.next_open is None or need < c.next_open:
                    c.next_open = need
                hourly_adverse = funding / max(rk.funding_horizon_hours, 1e-9)
                if rk.funding_gate_enabled and hourly_adverse > rk.max_adverse_funding_hourly_pct:
                    if q.dev >= need:
                        self._skip(c, now, "funding gate", q.direction, q.dev, need, f"{hourly_adverse:.4f}%/h adverse")
                    continue
                qty = self._rq(c, min(c.qty, remaining))
                if qty < min_qty:
                    if q.dev >= need:
                        self._skip(c, now, "size below minimum", q.direction, q.dev, need, f"{qty} < {min_qty}")
                    continue
                ot = ex.open_mode == "taker_taker"
                plan = ExecPlan("open", c.pair, c.sym, c.maker_sym, c.taker_sym, q.direction, qty, level, need, basis_fn,
                                target=target, taker_taker=ot)
                plan.carry_in = self._carry(c, sp, q.direction)
                if ot:
                    if need - q.dev <= 0:
                        topen.append((need - q.dev, plan))  # immediate IOC on both venues, never resting
                    continue
                cands.append((need - q.dev, plan))
            if topen:
                gap, plan = min(topen, key=lambda x: x[0])
                if not self._confirmed(c, st, now):
                    return None
                if not self._stable(c):
                    c.status = "open signal, prices unstable"
                    self._skip(c, now, "prices unstable", plan.direction, plan.required_dev - gap, plan.required_dev)
                    return None
                why = self._open_blocked(c, plan, now)
                if why:
                    c.status = f"blocked: {why}"
                    self._skip(c, now, "blocked", plan.direction, plan.required_dev - gap, plan.required_dev, why)
                    return None
                return plan
            if ex.open_mode == "taker_taker":
                c.signal_since = None  # no immediate signal this tick: persistence restarts
        if not cands:
            return None
        if thr:
            gap, plan = min(cands, key=lambda x: x[0])
            if ex.threshold_max_gap_pct is not None and gap > ex.threshold_max_gap_pct:
                return None
        else:
            opens = [x for x in cands if x[1].kind == "open" and x[0] <= 0]
            if not opens:
                c.signal_since = None
                return None
            gap, plan = min(opens, key=lambda x: x[0])
            if not self._confirmed(c, st, now):
                return None
        if not self._stable(c):
            c.status = f"{plan.kind} signal, prices unstable"
            if plan.kind == "open" and gap <= 0:
                self._skip(c, now, "prices unstable", plan.direction, plan.required_dev - gap, plan.required_dev)
            return None
        if plan.kind == "open":
            why = self._open_blocked(c, plan, now)
            if why:
                c.status = f"blocked: {why}"
                if gap <= 0:
                    self._skip(c, now, "blocked", plan.direction, plan.required_dev - gap, plan.required_dev, why)
                return None
        return plan

    def _skip(self, c: Ctx, now: float, reason: str, direction: Optional[str], dev: Optional[float],
              need: Optional[float], detail: str = "", log_it: bool = True) -> None:
        """v5.6: an open signal (dev >= need) that was NOT traded, with the reason. Journaled as `signal_skip` and
        logged at most once per risk.skip_log_s per symbol and reason (0 = off)."""
        iv = c.sym.risk.skip_log_s
        if iv <= 0 or not self.live:
            return
        key = reason + ("" if reason != "blocked" else ":" + detail.split(" ")[0])
        if now - c.skip_logged.get(key, 0.0) < iv:
            return
        c.skip_logged[key] = now
        self.journal("signal_skip", symbol=c.sym.key, direction=direction, reason=reason, detail=detail or None,
                     dev=None if dev is None else round(dev, 4), need=None if need is None else round(need, 4))
        if log_it:
            log.info("signal not taken: %s %s dev %s >= need %s: %s%s", c.sym.key, direction or "-",
                     "-" if dev is None else f"{dev:.3f}%", "-" if need is None else f"{need:.3f}%", reason,
                     f" ({detail})" if detail else "")

    def _confirmed(self, c: Ctx, st, now: float) -> bool:
        """Spread persistence: the open signal must hold continuously for signal_confirm_s."""
        if st.signal_confirm_s <= 0:
            return True
        if c.signal_since is None:
            c.signal_since = now
        held = now - c.signal_since
        if held < st.signal_confirm_s:
            c.status = f"signal confirming {held:.1f}/{st.signal_confirm_s:g}s"
            return False
        return True

    def _taker_ok(self, c: Ctx, q: Decimal) -> bool:
        """v5.3: the taker venue accepts q (exchange minimums, 5% price margin): a close the taker cannot hedge
        is not quoted (v5.2 filled it on the maker venue, skipped the hedge and flattened the maker again)."""
        T = self.venues[c.pair.taker]
        m, b = T.market(c.taker_sym), T.book(c.taker_sym)
        return bool(b and b.mid) and q > 0 and q >= m.min_qty and q * b.mid >= m.min_notional * Decimal("1.05")

    def _sized_ok(self, c: Ctx, q: Decimal) -> bool:
        """q meets the minimum order value on both venues."""
        for vname, sym in ((c.pair.maker, c.maker_sym), (c.pair.taker, c.taker_sym)):
            v = self.venues[vname]
            b = v.book(sym)
            if not b or not b.mid or q * b.mid < v.market(sym).min_notional:
                return False
        return True

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
        why = self.guards.open_block(c, now)
        if why:
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

    # ------------------------------------------------------------ v5.5 markouts
    @staticmethod
    def _fill_mids(res: ExecResult) -> dict:
        """Qty-weighted mids of both venues at the moments the maker fills were seen (hedge cost = taker avg vs
        taker mid at fill)."""
        fs = [f for f in res.fills if f.get("t_mid") and f.get("m_mid")]
        q = sum(f["qty"] for f in fs)
        if not fs or q <= 0:
            return {}
        return {"fills_n": len(res.fills), "m_mid_fill": sum(f["m_mid"] * f["qty"] for f in fs) / q,
                "t_mid_fill": sum(f["t_mid"] * f["qty"] for f in fs) / q,
                "fill_ts": round(fs[0]["t"], 3)}

    def _on_fill(self, plan: ExecPlan, fill: dict) -> None:
        hs = [h for h in (self.cfg.markout_horizons_s or []) if h > 0]
        if not hs or not fill.get("px"):
            return
        try:
            task = asyncio.get_running_loop().create_task(self._markout(plan, dict(fill), hs))
        except RuntimeError:
            return
        self._markout_tasks.add(task)
        task.add_done_callback(self._markout_tasks.discard)

    async def _markout(self, plan: ExecPlan, f: dict, hs: list[float]) -> None:
        """Mids of both venues h seconds after a maker fill, in bps from our side (positive = price moved in our
        favour): maker = s * (maker mid(t+h) - fill price), taker = s * (taker mid(t+h) - taker mid(t))."""
        M, T = self.venues[plan.pair.maker], self.venues[plan.pair.taker]
        s = 1.0 if sg.maker_is_buy(plan.direction) else -1.0
        px, m0, t0 = f["px"], f.get("m_mid"), f.get("t_mid")
        mo_m, mo_t, stale = {}, {}, []
        try:
            for h in hs:
                delay = f["t"] + h - time.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                key = f"{h:g}"
                mb, tb = M.book(plan.maker_sym), T.book(plan.taker_sym)
                if M.quote_age(plan.maker_sym) > 5 or T.quote_age(plan.taker_sym) > 5:
                    stale.append(key)
                if mb and mb.mid:
                    mo_m[key] = round(s * (float(mb.mid) - px) / px * 1e4, 3)
                if tb and tb.mid and t0:
                    mo_t[key] = round(s * (float(tb.mid) - t0) / t0 * 1e4, 3)
        except asyncio.CancelledError:
            pass  # shutdown: keep what was sampled
        from arb.config import us_regular_hours
        self.journal("markout", symbol=plan.sym.key, slice=plan.kind, direction=plan.direction, level=plan.level,
                     fill_ts=round(f["t"], 3), qty=f["qty"], px=px, m_mid0=m0, t_mid0=t0,
                     edge0_bps=round(s * (t0 - px) / px * 1e4, 3) if t0 else None,
                     half_spread_bps=round(s * (m0 - px) / px * 1e4, 3) if m0 else None,
                     mo_maker=mo_m, mo_taker=mo_t, stale=stale or None, rth=bool(us_regular_hours(f["t"])),
                     category=plan.sym.category or ("us_equity" if plan.sym.strategy.session == "us_equity" else ""))

    async def _drain_markouts(self) -> None:
        for t in list(self._markout_tasks):
            t.cancel()
        if self._markout_tasks:
            await asyncio.gather(*self._markout_tasks, return_exceptions=True)

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
                sp.add_open(sg.POSITION_OF[plan.direction], plan.level, hedged, plan.target or hedged, dev or 0.0, raw=raw,
                            maker_px=float(res.maker_avg) if res.maker_avg else None,
                            taker_px=float(res.taker_avg) if res.taker_avg else None)
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
        slip = self.guards.on_result(c, plan, res, dev)
        self.journal(plan.kind, symbol=c.sym.key, direction=plan.direction, level=plan.level, qty=plan.qty,
                     maker_filled=res.maker_filled, maker_avg=res.maker_avg, taker_filled=res.taker_filled,
                     taker_avg=res.taker_avg, hedged=hedged, carry_in=res.carry_in, raw_edge=raw, adj_edge=dev,
                     required=plan.required_dev, floor=plan.required_raw, gross_pnl_usdc=pnl, note=res.note,
                     requotes=res.requotes, hedge_orders=res.hedge_orders, zero_fill_hedges=res.zero_fill_hedges,
                     unwound=res.maker_unwound, error=res.error or None, slippage=slip,
                     hedge_latency_s=round(max(res.hedge_latencies), 3) if res.hedge_latencies else None,
                     residual_maker=sp.residual_maker, residual_taker=sp.residual_taker,
                     **self._fill_mids(res))
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

    async def _pair_off(self, c: Ctx, sp: SymbolPosition) -> bool:
        """v5.3: a close whose maker leg filled while its hedge could not be sent leaves a maker residual with the
        CLOSE sign. Flattening it on the maker venue re-opens the position that was just closed (v5.2 looped on
        PUMP this way); the taker leg of the grid is closed instead (IOC, reduce-only) and both are paired off.
        Returns True when this symbol's residual is handled here (also: too small, waits for the next close)."""
        if not sp.grids:
            return False
        cdir = sg.CLOSE_DIRECTION[sp.direction]
        r = min(self._carry(c, sp, cdir), sp.total_qty)
        if r <= 0:
            return False
        T = self.venues[c.pair.taker]
        q = T.round_qty(c.taker_sym, r)
        if self.guards.venue_paused(c.pair.taker, time.time()) or not self._taker_ok(c, q):
            return True  # never flattened on the maker venue; hedged together with the next close slice
        st = await self.executor._ioc(T, c.taker_sym, not sg.maker_is_buy(cdir), q, True,
                                      c.sym.execution.hedge_slippage_pct, c.sym.risk.max_quote_age_s)
        if st is None:
            return True
        if st.status.value == "unknown":
            self.halt(f"pair-off order on {c.pair.taker} has unknown outcome")
            return True
        if st.filled > 0:
            s_m = Decimal(1) if sg.maker_is_buy(cdir) else Decimal(-1)
            sp.residual_maker -= s_m * st.filled
            sp.reduce_close(st.filled)
            self.positions.save()
            self.journal("pair_off", symbol=c.sym.key, venue=c.pair.taker, qty=st.filled, price=st.avg_price or st.price,
                         residual_maker=sp.residual_maker, residual_taker=sp.residual_taker)
            self.event(f"residual {c.sym.key}: close hedge paired off on {c.pair.taker}: {st.filled}")
        return True

    async def _fix_residuals(self) -> None:
        """Flatten unhedged leftovers that became tradable (only after a clean reconcile)."""
        for c in self.active_ctxs():
            sp = self.position(c)
            if await self._dust_flatten(c, sp):
                if self.halted:
                    return
                continue
            if await self._pair_off(c, sp):
                if self.halted:
                    return
                continue
            for vname, sym, attr in ((c.pair.maker, c.maker_sym, "residual_maker"), (c.pair.taker, c.taker_sym, "residual_taker")):
                r = getattr(sp, attr)
                if r == 0:
                    continue
                v = self.venues[vname]
                if self.guards.venue_paused(vname, time.time()):
                    continue  # venue in rate-limit / API backoff
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


    # ------------------------------------------------------------ v5.6 dust clean-up
    async def _dust_flatten(self, c: Ctx, sp: SymbolPosition) -> bool:
        """v5.6: a grid that has been unclosable for risk.dust_grace_s (below an exchange minimum on a leg, or a
        close hedge that can never be sent) and whose legs are each worth <= risk.dust_max_usdc is flattened on
        both venues with taker orders: a leg below the venue minimum is first topped up to the minimum (same
        side), then closed completely with a reduce-only order. Afterwards the local state is set to the
        exchange positions. Returns True when this symbol was handled here."""
        rk = c.sym.risk
        now = time.time()
        if not (rk.dust_flatten and sp.grids and c.unclosable and c.unclosable_since is not None):
            return False
        if now - c.unclosable_since < rk.dust_grace_s or now < c.dust_next or c.key in self.exec_tasks:
            return False
        legs = []
        for vname, sym in ((c.pair.maker, c.maker_sym), (c.pair.taker, c.taker_sym)):
            v = self.venues[vname]
            if self.guards.venue_paused(vname, now):
                return False
            pos = self.exch_positions.get(vname, {}).get(sym)
            q = pos.qty if pos is not None else ZERO
            b = v.book(sym)
            if q and not (b and b.is_valid() and b.mid):
                return False
            value = float(abs(q) * b.mid) if q else 0.0
            if value > rk.dust_max_usdc:
                note = f"{c.sym.key}: {c.unclosable} - {vname} leg {q} ({value:.2f} USDC) > dust_max_usdc {rk.dust_max_usdc:g}, not cleaned up automatically"
                if note != c.dust_note:
                    self.event(note, logging.WARNING)
                    c.dust_note = note
                return False
            legs.append((vname, v, sym, q, value))
        before = {vname: str(q) for vname, _v, _s, q, _x in legs}
        local_before = dict(direction=sp.direction, grids=[str(g.qty) for g in sp.grids],
                            residual_maker=str(sp.residual_maker), residual_taker=str(sp.residual_taker))
        why = c.unclosable
        self.event(f"WARNING {c.sym.key}: dust clean-up ({why}); flattening "
                   + ", ".join(f"{n} {q} ({x:.2f} USDC)" for n, _v, _s, q, x in legs), logging.WARNING)
        orders: list[dict] = []
        for vname, v, sym, q, _x in legs:
            if q == 0:
                continue
            if await self._flatten_leg(c, v, sym, q, orders) is None:
                self.journal("dust_flatten", symbol=c.sym.key, ok=False, reason=why, before=before, orders=orders,
                             local_before=local_before, error="unknown order outcome (halted)")
                return True
        M, T = self.venues[c.pair.maker], self.venues[c.pair.taker]
        try:
            pm, pt = await M.positions(), await T.positions()
        except Exception as exc:  # noqa: BLE001 - the next reconcile finds out; no new attempt for a while
            c.dust_next = now + max(600.0, rk.failure_pause_s)
            self.journal("dust_flatten", symbol=c.sym.key, ok=False, reason=why, before=before, orders=orders,
                         local_before=local_before, error=f"positions: {exc}")
            self.reconcile_due = True
            log.warning("dust clean-up %s: positions failed: %s", c.sym.key, exc)
            return True
        self.exch_positions[M.name], self.exch_positions[T.name] = pm, pt
        am = pm.get(c.maker_sym).qty if c.maker_sym in pm else ZERO
        at = pt.get(c.taker_sym).qty if c.taker_sym in pt else ZERO
        # local state := what the exchanges hold now (no grid; anything left is residual, settled as usual)
        sp.grids.clear()
        sp.direction = None
        sp.residual_maker, sp.residual_taker = am, at
        sp.updated = time.time()
        self.positions.save()
        c.unclosable, c.unclosable_since, c.dust_note = "", None, ""
        ok = am == 0 and at == 0
        self.journal("dust_flatten", symbol=c.sym.key, ok=ok, reason=why, before=before, orders=orders,
                     local_before=local_before, after={M.name: str(am), T.name: str(at)})
        if ok:
            self.event(f"{c.sym.key}: dust clean-up done, both venues flat ({len(orders)} order(s))")
        else:
            c.dust_next = now + max(600.0, rk.failure_pause_s)
            self.event(f"WARNING {c.sym.key}: dust clean-up incomplete: {M.name} {am}, {T.name} {at} "
                       f"(kept as residual; next attempt in {max(600.0, rk.failure_pause_s):.0f}s)", logging.WARNING)
        return True

    async def _flatten_leg(self, c: Ctx, v: Venue, sym: str, q: Decimal, orders: list) -> Optional[bool]:
        """Close position q on venue v completely. Below the venue minimum: top up on the same side first.
        None = an order had an unknown outcome (engine halted)."""
        rk, ex = c.sym.risk, c.sym.execution
        m = v.market(sym)
        long_ = q > 0

        def tradable(x: Decimal, reduce: bool) -> bool:
            b = v.book(sym)
            if not (b and b.mid) or x < m.min_qty or x <= 0:
                return False
            return x * b.mid >= m.min_notional * Decimal("1.05") or (reduce and v.reduce_only_any_notional)

        async def send(is_buy: bool, qty: Decimal, reduce: bool, step: str):
            st = await self.executor._ioc(v, sym, is_buy, qty, reduce, ex.hedge_slippage_pct, rk.max_quote_age_s)
            if st is None:
                orders.append(dict(venue=v.name, step=step, qty=str(qty), status="no fresh book"))
                return None, False
            orders.append(dict(venue=v.name, step=step, side="buy" if is_buy else "sell", qty=str(qty), reduce_only=reduce,
                               filled=str(st.filled), price=str(st.avg_price or st.price), status=st.status.value,
                               reason=getattr(st, "reason", None) or None))
            if st.status.value == "unknown":
                self.halt(f"dust clean-up order on {v.name} ({sym}) has unknown outcome")
                return st, True
            return st, False

        size = abs(q)
        for _ in range(2):  # top up (same side) until a reduce-only close of everything is accepted
            if tradable(size, True):
                break
            b = v.book(sym)
            add = ceil_to_step(max(m.min_qty, m.min_notional * Decimal("1.1") / b.mid), m.size_step)
            if float(add * b.mid) > max(2 * rk.dust_max_usdc, 3 * float(m.min_notional)):
                orders.append(dict(venue=v.name, step="top-up", qty=str(add), status="refused: top-up too large"))
                return False
            st, unknown = await send(long_, add, False, "top-up")
            if unknown:
                return None
            if st is None or st.filled <= 0:
                return False
            size += st.filled
        if not tradable(size, True):
            return False
        st, unknown = await send(not long_, v.round_qty(sym, size), True, "close")
        if unknown:
            return None
        return st is not None and st.filled > 0


def _f(x) -> str:
    if x is None:
        return "-"
    try:
        return f"{float(x):.4f}"
    except (TypeError, ValueError):
        return str(x)
