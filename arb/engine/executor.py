"""Maker -> taker execution of one slice.

1. Post-only order on the maker venue at the passive price (join/improve).
2. Every fill increment is hedged immediately on the taker venue with an IOC
   order bounded by best price +/- hedge slippage (retried with wider bounds).
3. The maker order is cancelled on timeout, when the edge at our resting price
   falls below the requirement minus `cancel_adverse_pct`, when the taker book
   goes stale, or re-quoted when someone improves the best price.
4. If hedging fails after all attempts the filled maker quantity is unwound on
   the maker venue (IOC) when `emergency_unwind` is enabled; whatever cannot be
   resolved is reported so the engine halts.
"""
from __future__ import annotations

import asyncio
import logging
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
    events: list[str] = field(default_factory=list)

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
        return min(self.maker_net, self.taker_filled)

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

    def _edge_at(self, plan: ExecPlan, maker_px: Decimal, qty: Decimal) -> Optional[float]:
        rk = plan.sym.risk
        tb = self._fresh(self.venues[plan.pair.taker], plan.taker_sym, rk.max_quote_age_s)
        if tb is None:
            return None
        f = sg.taker_fill(tb, not sg.maker_is_buy(plan.direction), qty, rk.depth_levels, rk.depth_band_pct)
        if f is None:
            return None
        return sg.adjusted_edge(plan.direction, sg.raw_edge(plan.direction, maker_px, f.vwap), plan.basis_fn())

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
        for attempt in range(ex.hedge_max_attempts):
            if remaining <= 0:
                break
            st = await self._ioc(T, plan.taker_sym, taker_buy, remaining, plan.reduce_only,
                                 ex.hedge_slippage_pct + attempt * ex.hedge_slippage_step_pct, plan.sym.risk.max_quote_age_s)
            if st is None:
                await asyncio.sleep(0.3)
                continue
            if st.status == OrderStatus.UNKNOWN:
                res.halted = True
                res.error = f"hedge order {st.client_id} on {T.name} has unknown outcome"
                raise ExecutionHalt(res.error)
            if st.filled > 0:
                res.taker_filled += st.filled
                res.taker_cost += st.filled * (st.avg_price or st.price)
                remaining -= st.filled
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
            res.error = f"unhedged {unhedged} {plan.maker_sym} on {M.name} (emergency_unwind disabled)"

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
        if st.filled > 0:
            left = await self._hedge(plan, st.filled, res)
            await self._settle_unhedged(plan, left, res)

    async def _run_maker_taker(self, plan: ExecPlan, res: ExecResult) -> None:
        M, T = self.venues[plan.pair.maker], self.venues[plan.pair.taker]
        ex, rk = plan.sym.execution, plan.sym.risk
        mb = sg.maker_is_buy(plan.direction)
        loop = asyncio.get_running_loop()
        start = loop.time()
        deadline = start + ex.maker_timeout_s
        requotes = 0
        pending = ZERO  # maker fills not yet hedged
        order: Optional[OrderState] = None
        seen = ZERO
        seen_cost = ZERO
        placed_at = 0.0
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
                await M.wait_final(o, ex.order_final_timeout_s, poll=ex.order_poll_interval_s)
            absorb(o)
            if not o.status.final:
                raise ExecutionHalt(f"maker order {o.client_id} on {M.name} not final after cancel ({o.status.value})")
            M.forget(o)

        async def hedge_pending(force: bool) -> None:
            nonlocal pending
            if pending <= 0:
                return
            tb = T.book(plan.taker_sym)
            px = (tb.mid if tb and tb.mid else None) or (order.price if order else Decimal("1"))
            q = T.round_qty(plan.taker_sym, pending)
            if q <= 0 or not self._tradable(T, plan.taker_sym, q, px):
                return  # wait for more fills (or leave as residual at the end)
            before = res.taker_filled
            left = await self._hedge(plan, q, res)
            pending -= (res.taker_filled - before)
            if left > 0 and self._tradable(T, plan.taker_sym, T.round_qty(plan.taker_sym, left), px):
                await self._settle_unhedged(plan, left, res)
                pending = max(ZERO, pending - (res.maker_unwound))
                if res.halted:
                    raise ExecutionHalt(res.error)
                raise _StopSlice("hedge failed")

        try:
            while True:
                now = loop.time()
                if order is None:
                    remaining = M.round_qty(plan.maker_sym, plan.qty - res.maker_filled)
                    if self.stop_flag():
                        stop_reason = "stopping"
                        break
                    if now >= deadline or requotes > ex.max_requotes:
                        stop_reason = "timeout" if now >= deadline else "max requotes"
                        break
                    mbook = self._fresh(M, plan.maker_sym, rk.max_quote_age_s)
                    if mbook is None:
                        stop_reason = "maker book stale"
                        break
                    ref = mbook.best_bid.price if mb else mbook.best_ask.price
                    tick = M.price_tick_at(plan.maker_sym, ref)
                    px = sg.maker_quote(mbook, mb, ex.maker_price_mode, tick)
                    if px is None or remaining <= 0 or not self._tradable(M, plan.maker_sym, remaining, px):
                        stop_reason = "nothing left" if remaining <= 0 else "below min size"
                        break
                    px = M.round_price(plan.maker_sym, px, up=not mb)  # never more aggressive
                    edge = self._edge_at(plan, px, remaining)
                    if edge is None or edge < plan.required_dev:
                        stop_reason = "edge gone" if edge is not None else "taker depth/stale"
                        break
                    order = await M.place_post_only(plan.maker_sym, mb, remaining, px, plan.reduce_only)
                    res.orders_placed += 1
                    seen = ZERO
                    seen_cost = ZERO
                    placed_at = loop.time()
                    if order.status == OrderStatus.REJECTED:
                        res.events.append(f"maker rejected: {order.reason[:120]}")
                        M.forget(order)
                        order = None
                        requotes += 1
                        await asyncio.sleep(0.25)
                        continue
                    if order.status == OrderStatus.UNKNOWN:
                        await M.wait_final(order, ex.order_final_timeout_s, poll=ex.order_poll_interval_s)
                        if order.status in (OrderStatus.UNKNOWN, OrderStatus.PENDING):
                            raise ExecutionHalt(f"maker order {order.client_id} on {M.name} outcome unknown")
                    continue

                got = await M.wait_order(order, ex.order_poll_interval_s)
                now = loop.time()
                private_ok = getattr(M, "private_connected", True) and M.connected
                if not got and (not private_ok or now - last_refresh > 5 * ex.order_poll_interval_s):
                    last_refresh = now
                    try:
                        await M.refresh_order(order)
                    except Exception as exc:  # noqa: BLE001
                        log.debug("refresh failed: %s", exc)
                absorb(order)
                await hedge_pending(force=False)

                if order.status.final:
                    M.forget(order)
                    if order.status == OrderStatus.CANCELED and "post-only" in (order.reason or ""):
                        requotes += 1
                    order = None
                    continue

                # ---------- cancel / requote conditions
                cancel_reason = ""
                if self.stop_flag():
                    cancel_reason = "stopping"
                elif now >= deadline:
                    cancel_reason = "timeout"
                else:
                    rem = max(order.remaining, M.market(plan.maker_sym).min_qty)
                    edge = self._edge_at(plan, order.price, rem)
                    if edge is None:
                        cancel_reason = "taker depth/stale"
                    elif edge < plan.required_dev - ex.cancel_adverse_pct:
                        cancel_reason = f"adverse ({edge:.3f}% < {plan.required_dev - ex.cancel_adverse_pct:.3f}%)"
                    elif ex.requote_when_not_best and now - placed_at >= ex.requote_min_interval_s:
                        mbook = self._fresh(M, plan.maker_sym, rk.max_quote_age_s)
                        if mbook is not None:
                            best = mbook.best_bid.price if mb else mbook.best_ask.price
                            if (mb and best > order.price) or (not mb and best < order.price):
                                cancel_reason = "requote"
                if cancel_reason:
                    await finalize(order)
                    await hedge_pending(force=True)
                    order = None
                    if cancel_reason == "requote":
                        requotes += 1
                        continue
                    stop_reason = cancel_reason
                    break
        except _StopSlice as s:
            stop_reason = str(s)
        finally:
            if order is not None and not order.status.final:
                await finalize(order)
        # final hedge of whatever is still pending
        if pending > 0:
            tb = T.book(plan.taker_sym)
            px = (tb.mid if tb and tb.mid else None) or Decimal("1")
            if self._tradable(T, plan.taker_sym, T.round_qty(plan.taker_sym, pending), px):
                before = res.taker_filled
                left = await self._hedge(plan, pending, res)
                pending -= res.taker_filled - before
                await self._settle_unhedged(plan, left, res)
        res.note = stop_reason
        if res.halted and not res.error:
            res.error = "unresolved exposure"


class _StopSlice(Exception):
    pass
