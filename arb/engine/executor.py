"""Maker -> taker execution of one slice.

1. Post-only order on the maker venue at the passive price (join/improve).
2. Every fill increment is hedged immediately on the taker venue with an IOC
   order bounded by best price +/- hedge slippage (retried with wider bounds).
3. The maker order is cancelled on timeout, when the edge at our resting price
   falls below the requirement minus `cancel_adverse_pct`, when the taker book
   goes stale, or re-quoted when someone improves the best price.
4. If hedging fails after all attempts (only confirmed zero-fill IOCs use up the
   retry budget) the slice reports an unresolved exposure: the engine cancels all
   makers and locks the instance for manual intervention. With `emergency_unwind`
   the filled maker quantity is first flattened on the maker venue (IOC).

maker_price_mode=threshold (ported from arcus-maker-lighter-taker): the maker
order rests at the exact price that yields the required basis-adjusted edge (and
the close-profit floor) against the taker book, clamped to the nearest passive
tick; it is re-priced when that fair price moves more than
`requote_threshold_pct` or the order is older than `max_order_age_s`, at most
`max_requotes_per_s` times per second (risk cancels bypass the limit).
Partial maker fills are accumulated until the hedge reaches the venue minimums
and `hedge_min_notional_usdc`; smaller dust is carried into the next slice of the
same direction (`ExecPlan.carry_in`).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Optional

from arb.config import Pair, SymbolConfig
from arb.engine import signals as sg
from arb.models import OrderState, OrderStatus
from arb.venues.base import Venue

log = logging.getLogger("arb.exec")
ZERO = Decimal("0")


class ExecutionHalt(RuntimeError):
    """Unresolvable state (unknown order outcome, failed hedge). Engine must stop trading."""


@dataclass
class ExecPlan:
    kind: str  # open | close
    pair: Pair
    sym: SymbolConfig
    maker_sym: str
    taker_sym: str
    direction: str  # sell_maker | buy_maker (direction of THIS execution)
    qty: Decimal
    level: int
    required_dev: float  # execute while dev(direction) >= required_dev
    basis_fn: Callable[[], float]
    taker_taker: bool = False
    required_raw: Optional[float] = None  # close-profit floor on the RAW (not basis-adjusted) edge
    carry_in: Decimal = ZERO  # earlier unhedged maker dust of the same sign, hedged together with this slice
    target: Optional[Decimal] = None

    @property
    def reduce_only(self) -> bool:
        return self.kind == "close"


@dataclass
class ExecResult:
    maker_filled: Decimal = ZERO
    maker_cost: Decimal = ZERO
    taker_filled: Decimal = ZERO
    taker_cost: Decimal = ZERO
    maker_unwound: Decimal = ZERO
    maker_unwound_cost: Decimal = ZERO
    orders_placed: int = 0
    note: str = ""
    error: str = ""
    halted: bool = False
    carry_in: Decimal = ZERO
    hedge_orders: int = 0
    zero_fill_hedges: int = 0
    requotes: int = 0
    events: list[str] = field(default_factory=list)
    hedge_latencies: list[float] = field(default_factory=list)  # taker IOC submit -> final, seconds

    @property
    def maker_avg(self) -> Optional[Decimal]:
        return self.maker_cost / self.maker_filled if self.maker_filled > 0 else None

    @property
    def taker_avg(self) -> Optional[Decimal]:
        return self.taker_cost / self.taker_filled if self.taker_filled > 0 else None

    @property
    def maker_net(self) -> Decimal:
        return self.maker_filled - self.maker_unwound

    @property
    def hedged(self) -> Decimal:
        return min(self.maker_net + self.carry_in, self.taker_filled)

    def realized_dev(self, direction: str, basis: float) -> Optional[float]:
        if self.maker_avg is None or self.taker_avg is None:
            return None
        return sg.adjusted_edge(direction, sg.raw_edge(direction, self.maker_avg, self.taker_avg), basis)


class Executor:
    def __init__(self, venues: dict[str, Venue], stop_flag: Callable[[], bool] = lambda: False):
        self.venues = venues
        self.stop_flag = stop_flag

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _tradable(v: Venue, sym: str, qty: Decimal, price: Decimal) -> bool:
        m = v.market(sym)
        return qty >= m.min_qty and qty * price >= m.min_notional and qty > 0

    def _fresh(self, v: Venue, sym: str, max_age: float):
        b = v.book(sym)
        if b is None or not b.is_valid() or v.quote_age(sym) > max_age:
            return None
        return b

    def _hedge_ok(self, plan: ExecPlan, qty: Decimal, px: Decimal) -> bool:
        T = self.venues[plan.pair.taker]
        if not self._tradable(T, plan.taker_sym, qty, px):
            return False
        # v5.3: a close hedges the grid it closes - waiting for more fills (hedge_min_notional_usdc) cannot help
        # there, only the exchange minimums apply
        return plan.kind == "close" or qty * px >= Decimal(str(plan.sym.execution.hedge_min_notional_usdc))

    def _taker_vwap(self, plan: ExecPlan, qty: Decimal) -> Optional[Decimal]:
        rk = plan.sym.risk
        tb = self._fresh(self.venues[plan.pair.taker], plan.taker_sym, rk.max_quote_age_s)
        if tb is None:
            return None
        f = sg.taker_fill(tb, not sg.maker_is_buy(plan.direction), qty, rk.depth_levels, rk.depth_band_pct)
        return f.vwap if f else None

    def _edge_at(self, plan: ExecPlan, maker_px: Decimal, qty: Decimal) -> Optional[float]:
        """Basis-adjusted edge at maker_px, or -inf when the raw close-profit floor is violated."""
        vwap = self._taker_vwap(plan, qty)
        if vwap is None:
            return None
        raw = sg.raw_edge(plan.direction, maker_px, vwap)
        if plan.required_raw is not None and raw < plan.required_raw - 1e-9:
            return float("-inf")
        return sg.adjusted_edge(plan.direction, raw, plan.basis_fn())

    def _required_raw(self, plan: ExecPlan) -> float:
        b = plan.basis_fn()
        need = plan.required_dev + b if plan.direction == sg.SELL_MAKER else plan.required_dev - b
        return max(need, plan.required_raw) if plan.required_raw is not None else need

    def threshold_price(self, plan: ExecPlan, qty: Decimal) -> Optional[Decimal]:
        """Passive maker price that exactly meets the required edge vs the current taker VWAP."""
        M = self.venues[plan.pair.maker]
        mbook = self._fresh(M, plan.maker_sym, plan.sym.risk.max_quote_age_s)
        vwap = self._taker_vwap(plan, qty)
        if mbook is None or vwap is None or not mbook.best_bid or not mbook.best_ask:
            return None
        r = Decimal(str(self._required_raw(plan))) / 100
        mb = sg.maker_is_buy(plan.direction)
        if mb:
            px = M.round_price(plan.maker_sym, vwap / (1 + r), up=False)
            ceiling = mbook.best_ask.price - M.price_tick_at(plan.maker_sym, mbook.best_ask.price)
            if px > ceiling:
                px = M.round_price(plan.maker_sym, ceiling, up=False)
        else:
            px = M.round_price(plan.maker_sym, vwap * (1 + r), up=True)
            floor = mbook.best_bid.price + M.price_tick_at(plan.maker_sym, mbook.best_bid.price)
            if px < floor:
                px = M.round_price(plan.maker_sym, floor, up=True)
        return px if px > 0 else None

    # ---------------------------------------------------------------- hedge
    async def _ioc(self, v: Venue, sym: str, is_buy: bool, qty: Decimal, reduce_only: bool,
                   slippage_pct: float, max_age: float) -> Optional[OrderState]:
        b = self._fresh(v, sym, max_age * 2)
        if b is None:
            return None
        best = b.best_ask.price if is_buy else b.best_bid.price
        mult = Decimal(str(slippage_pct)) / 100
        limit = v.round_price(sym, best * (1 + mult) if is_buy else best * (1 - mult), up=is_buy)
        st = await v.place_ioc(sym, is_buy, qty, limit, reduce_only)
        v.forget(st)
        return st

    async def _hedge(self, plan: ExecPlan, qty: Decimal, res: ExecResult) -> Decimal:
        """Hedge `qty` on the taker venue. Returns quantity that could NOT be hedged."""
        T = self.venues[plan.pair.taker]
        ex = plan.sym.execution
        taker_buy = not sg.maker_is_buy(plan.direction)
        remaining = T.round_qty(plan.taker_sym, qty)
        dust = qty - remaining
        zero = orders = stale = 0
        # retry budget: only confirmed zero-filled terminal IOCs count; positive partials keep going
        while remaining > 0 and zero < ex.hedge_max_attempts and orders < ex.hedge_max_orders:
            t0 = time.monotonic()
            st = await self._ioc(T, plan.taker_sym, taker_buy, remaining, plan.reduce_only,
                                 ex.hedge_slippage_pct + zero * ex.hedge_slippage_step_pct, plan.sym.risk.max_quote_age_s)
            if st is not None:
                res.hedge_latencies.append(time.monotonic() - t0)
            if st is None:  # taker book stale: nothing was sent
                stale += 1
                if stale > 20:
                    res.events.append("hedge: taker book stale")
                    break
                await asyncio.sleep(0.3)
                continue
            orders += 1
            res.hedge_orders += 1
            if st.status == OrderStatus.UNKNOWN:
                res.halted = True
                res.error = f"hedge order {st.client_id} on {T.name} has unknown outcome"
                raise ExecutionHalt(res.error)
            if st.filled > 0:
                res.taker_filled += st.filled
                res.taker_cost += st.filled * (st.avg_price or st.price)
                remaining -= st.filled
            else:
                zero += 1
                res.zero_fill_hedges += 1
            if st.status == OrderStatus.REJECTED:
                res.events.append(f"hedge rejected: {st.reason[:120]}")
            ref = T.book(plan.taker_sym)
            px = ref.mid if ref and ref.mid else st.price
            if remaining <= 0 or not self._tradable(T, plan.taker_sym, remaining, px):
                break
        return remaining + dust

    async def _emergency_unwind(self, plan: ExecPlan, qty: Decimal, res: ExecResult) -> Decimal:
        """Flatten `qty` of the maker fill on the maker venue. Returns unresolved qty."""
        M = self.venues[plan.pair.maker]
        ex = plan.sym.execution
        remaining = M.round_qty(plan.maker_sym, qty)
        unresolved_dust = qty - remaining
        worst = ex.hedge_slippage_pct + (ex.hedge_max_attempts - 1) * ex.hedge_slippage_step_pct
        for attempt in range(2):
            if remaining <= 0:
                break
            st = await self._ioc(M, plan.maker_sym, not sg.maker_is_buy(plan.direction), remaining,
                                 plan.kind == "open", worst * (attempt + 1), plan.sym.risk.max_quote_age_s)
            if st is None or st.status == OrderStatus.UNKNOWN:
                break
            res.maker_unwound += st.filled
            res.maker_unwound_cost += st.filled * (st.avg_price or st.price)
            remaining -= st.filled
        return remaining + unresolved_dust

    async def _settle_unhedged(self, plan: ExecPlan, unhedged: Decimal, res: ExecResult) -> None:
        M, T = self.venues[plan.pair.maker], self.venues[plan.pair.taker]
        b = T.book(plan.taker_sym)
        px = (b.mid if b and b.mid else None) or Decimal("1")
        if unhedged <= 0 or not self._tradable(T, plan.taker_sym, T.round_qty(plan.taker_sym, unhedged), px):
            return  # dust: tracked as residual by the engine
        res.events.append(f"hedge incomplete, {unhedged} unhedged")
        if plan.sym.execution.emergency_unwind:
            left = await self._emergency_unwind(plan, unhedged, res)
            mb = M.book(plan.maker_sym)
            mpx = (mb.mid if mb and mb.mid else None) or px
            if left > 0 and self._tradable(M, plan.maker_sym, M.round_qty(plan.maker_sym, left), mpx):
                res.halted = True
                res.error = f"unhedged {left} {plan.maker_sym} on {M.name} after emergency unwind"
        else:
            res.halted = True
            res.error = (f"unhedged {unhedged} {plan.maker_sym} on {M.name} after {res.zero_fill_hedges} zero-fill hedges "
                         f"- manual intervention required")

    # ---------------------------------------------------------------- main
    async def run(self, plan: ExecPlan) -> ExecResult:
        res = ExecResult()
        try:
            if plan.taker_taker:
                await self._run_taker_taker(plan, res)
            else:
                await self._run_maker_taker(plan, res)
        except ExecutionHalt as exc:
            res.halted = True
            res.error = res.error or str(exc)
        except Exception as exc:  # noqa: BLE001
            log.exception("execution error")
            res.error = f"{type(exc).__name__}: {exc}"
            res.halted = True  # unexpected exceptions leave unknown state; reconcile before continuing
        return res

    async def _run_taker_taker(self, plan: ExecPlan, res: ExecResult) -> None:
        M = self.venues[plan.pair.maker]
        ex = plan.sym.execution
        st = await self._ioc(M, plan.maker_sym, sg.maker_is_buy(plan.direction), plan.qty, plan.reduce_only,
                             ex.hedge_slippage_pct, plan.sym.risk.max_quote_age_s)
        if st is None:
            res.note = "maker-venue book stale"
            return
        res.orders_placed += 1
        if st.status == OrderStatus.UNKNOWN:
            raise ExecutionHalt(f"IOC {st.client_id} on {M.name} has unknown outcome")
        res.maker_filled += st.filled
        res.maker_cost += st.filled * (st.avg_price or st.price)
        res.carry_in = plan.carry_in
        if st.filled > 0:
            left = await self._hedge(plan, st.filled + plan.carry_in, res)
            await self._settle_unhedged(plan, left, res)

    async def _run_maker_taker(self, plan: ExecPlan, res: ExecResult) -> None:
        M, T = self.venues[plan.pair.maker], self.venues[plan.pair.taker]
        ex, rk = plan.sym.execution, plan.sym.risk
        mb = sg.maker_is_buy(plan.direction)
        thr = ex.maker_price_mode == "threshold"
        loop = asyncio.get_running_loop()
        start = loop.time()
        deadline = start + ex.maker_timeout_s
        min_gap = 1.0 / ex.max_requotes_per_s
        requotes = 0
        res.carry_in = plan.carry_in
        pending = plan.carry_in  # maker fills (plus carried dust) not yet hedged
        order: Optional[OrderState] = None
        seen = ZERO
        seen_cost = ZERO
        placed_at = 0.0
        last_replace = -1e9
        anchor: Optional[Decimal] = None  # threshold mode: fair price the resting order was placed at
        stop_reason = ""
        last_refresh = 0.0

        def absorb(o: OrderState) -> None:
            nonlocal seen, seen_cost, pending
            if o.filled > seen:
                inc = o.filled - seen
                cost_now = o.filled * (o.avg_price or o.price)
                seen = o.filled
                pending += inc
                res.maker_filled += inc
                res.maker_cost += cost_now - seen_cost
                seen_cost = cost_now

        async def finalize(o: OrderState) -> None:
            if not o.status.final:
                try:
                    await M.cancel(o)
                except Exception as exc:  # noqa: BLE001
                    log.warning("cancel failed: %s", exc)
                # a replacement is only placed after the cancel reached a terminal state
                await M.wait_final(o, ex.order_final_timeout_s, poll=ex.order_poll_interval_s)
                if not o.status.final and not o.exchange_id:
                    # never confirmed by the exchange (no push, no REST row yet): keep looking it up a while
                    # longer (the venue resolves it as canceled when it never reached the book)
                    await M.wait_final(o, max(0.0, M.unacked_resolve_s() - (time.time() - o.created)),
                                       poll=max(3.0, ex.order_poll_interval_s))
            absorb(o)
            if not o.status.final:
                raise ExecutionHalt(f"maker order {o.client_id} on {M.name} not final after cancel ({o.status.value})")
            M.forget(o)

        def ref_px() -> Decimal:
            tb = T.book(plan.taker_sym)
            return (tb.mid if tb and tb.mid else None) or (order.price if order else Decimal("1"))

        async def hedge_pending() -> None:
            nonlocal pending
            if pending <= 0:
                return
            px = ref_px()
            q = T.round_qty(plan.taker_sym, pending)
            if q <= 0 or not self._hedge_ok(plan, q, px):
                return  # accumulate more fills (dust policy); leftovers become carried residual
            before = res.taker_filled
            left = await self._hedge(plan, q, res)
            pending -= (res.taker_filled - before)
            if left > 0 and self._tradable(T, plan.taker_sym, T.round_qty(plan.taker_sym, left), px):
                await self._settle_unhedged(plan, left, res)
                pending = max(ZERO, pending - (res.maker_unwound))
                if res.halted:
                    raise ExecutionHalt(res.error)
                raise _StopSlice("hedge failed")

        def price_now(remaining: Decimal) -> Optional[Decimal]:
            if thr:
                return self.threshold_price(plan, remaining)
            mbook = self._fresh(M, plan.maker_sym, rk.max_quote_age_s)
            if mbook is None:
                return None
            ref = mbook.best_bid.price if mb else mbook.best_ask.price
            px = sg.maker_quote(mbook, mb, ex.maker_price_mode, M.price_tick_at(plan.maker_sym, ref))
            return None if px is None else M.round_price(plan.maker_sym, px, up=not mb)  # never more aggressive

        try:
            while True:
                now = loop.time()
                if order is None:
                    remaining = M.round_qty(plan.maker_sym, plan.qty - res.maker_filled)
                    if self.stop_flag():
                        stop_reason = "stopping"
                        break
                    if now >= deadline or (not thr and requotes > ex.max_requotes):
                        stop_reason = "timeout" if now >= deadline else "max requotes"
                        break
                    if self._fresh(M, plan.maker_sym, rk.max_quote_age_s) is None:
                        stop_reason = "maker book stale"
                        break
                    if remaining <= 0:
                        stop_reason = "nothing left"
                        break
                    px = price_now(remaining)
                    if px is None or not self._tradable(M, plan.maker_sym, remaining, px):
                        stop_reason = "below min size" if px is not None else "taker depth/stale"
                        break
                    edge = self._edge_at(plan, px, remaining)
                    if edge is None or edge < plan.required_dev - 1e-9:
                        stop_reason = "edge gone" if edge is not None else "taker depth/stale"
                        break
                    wait = last_replace + min_gap - loop.time()
                    if wait > 0:  # replacement rate limit per symbol
                        await asyncio.sleep(wait)
                    order = await M.place_post_only(plan.maker_sym, mb, remaining, px, plan.reduce_only)
                    res.orders_placed += 1
                    seen = ZERO
                    seen_cost = ZERO
                    placed_at = last_replace = loop.time()
                    anchor = px
                    if order.status == OrderStatus.REJECTED:
                        res.events.append(f"maker rejected: {order.reason[:120]}")
                        M.forget(order)
                        order = None
                        requotes += 1
                        await asyncio.sleep(0.25)
                        continue
                    if order.status == OrderStatus.UNKNOWN:
                        await M.wait_final(order, ex.order_final_timeout_s, poll=ex.order_poll_interval_s)
                        if order.status in (OrderStatus.UNKNOWN, OrderStatus.PENDING) and not order.exchange_id:
                            # v5.3: send result lost (e.g. connection reset): keep looking the order up until the
                            # venue finds it or resolves it as never placed, instead of halting at once
                            left = M.unacked_resolve_s() - (time.time() - order.created)
                            if left > 0:
                                await M.wait_final(order, left, poll=max(3.0, ex.order_poll_interval_s))
                        if order.status in (OrderStatus.UNKNOWN, OrderStatus.PENDING):
                            raise ExecutionHalt(f"maker order {order.client_id} on {M.name} outcome unknown")
                    continue

                got = await M.wait_order(order, min(ex.order_poll_interval_s, 0.2) if thr else ex.order_poll_interval_s)
                now = loop.time()
                private_ok = getattr(M, "private_connected", True) and M.connected
                if not got and (not private_ok or now - last_refresh > 5 * ex.order_poll_interval_s):
                    last_refresh = now
                    try:
                        await M.refresh_order(order)
                    except Exception as exc:  # noqa: BLE001
                        log.debug("refresh failed: %s", exc)
                absorb(order)
                if not order.status.final:
                    await hedge_pending()
                    absorb(order)  # v5.2: fills that arrived while the hedge was running

                if order.status.final:
                    # absorbed just above with nothing awaited in between: forgetting cannot lose a fill
                    # (v5.1 forgot an order that finished during the hedge without reading its last fill)
                    M.forget(order)
                    if order.status == OrderStatus.CANCELED and "post-only" in (order.reason or ""):
                        requotes += 1
                    order = None
                    await hedge_pending()
                    continue

                # ---------- cancel / requote conditions (risk cancels bypass the replacement throttle)
                cancel_reason = ""
                throttled = now - last_replace < min_gap
                if self.stop_flag():
                    cancel_reason = "stopping"
                elif now >= deadline:
                    cancel_reason = "timeout"
                elif self._fresh(M, plan.maker_sym, rk.max_quote_age_s) is None:
                    cancel_reason = "maker book stale"
                else:
                    rem = max(order.remaining, M.market(plan.maker_sym).min_qty)
                    edge = self._edge_at(plan, order.price, rem)
                    if edge is None:
                        cancel_reason = "taker depth/stale"
                    elif edge < plan.required_dev - ex.cancel_adverse_pct:
                        # threshold mode re-prices at once (risk cancel, not throttled); other modes stop the slice
                        cancel_reason = "requote" if thr else f"adverse ({edge:.3f}% < {plan.required_dev - ex.cancel_adverse_pct:.3f}%)"
                    elif thr and not throttled:
                        fair = self.threshold_price(plan, rem)
                        moved = fair is not None and anchor and abs(fair / anchor - 1) * 100 > Decimal(str(ex.requote_threshold_pct))
                        if moved or now - placed_at >= ex.max_order_age_s or (edge < plan.required_dev - 1e-9):
                            cancel_reason = "requote"
                    elif not thr and ex.requote_when_not_best and not throttled and now - placed_at >= ex.requote_min_interval_s:
                        mbook = self._fresh(M, plan.maker_sym, rk.max_quote_age_s)
                        if mbook is not None:
                            best = mbook.best_bid.price if mb else mbook.best_ask.price
                            if (mb and best > order.price) or (not mb and best < order.price):
                                cancel_reason = "requote"
                if cancel_reason:
                    await finalize(order)
                    await hedge_pending()
                    order = None
                    if cancel_reason == "requote":
                        requotes += 1
                        res.requotes += 1
                        continue
                    stop_reason = cancel_reason
                    break
        except _StopSlice as s:
            stop_reason = str(s)
        finally:
            if order is not None and not order.status.final:
                await finalize(order)
        # final hedge of whatever is still pending (dust below the hedge minimum stays as carried residual)
        if pending > 0:
            px = ref_px()
            if self._hedge_ok(plan, T.round_qty(plan.taker_sym, pending), px):
                before = res.taker_filled
                left = await self._hedge(plan, pending, res)
                pending -= res.taker_filled - before
                await self._settle_unhedged(plan, left, res)
        res.note = stop_reason
        if res.halted and not res.error:
            res.error = "unresolved exposure"


class _StopSlice(Exception):
    pass
