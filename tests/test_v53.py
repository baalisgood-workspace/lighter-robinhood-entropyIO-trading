"""v5.3: closes below the taker minimum are not quoted, close-sign maker residuals are paired off on the taker venue
(never re-opened on the maker venue), and an order whose send result was lost is looked up before halting."""
import asyncio
import time
from decimal import Decimal as D

from arb.engine import signals as sg
from arb.models import OrderState, OrderStatus
from arb.util import new_client_index
from arb.venues.arcus import ArcusRejected

from conftest import FakeVenue
from test_engine import make_engine
from test_executor import plan, run
from test_v52 import _arcus


def _closing(tmp_path, fake_pair, grid="1", residual="0", **kw):
    """Short-maker grid with a close signal (maker venue cheap, taker venue rich); no further opens."""
    M, T = fake_pair
    M.set_book("X", "99.90", "100.00")
    T.set_book("X", "100.90", "101.00")
    eng, c = make_engine(tmp_path, fake_pair, max_grids=1, order_value_usdc=100, **kw)
    sp = eng.position(c)
    sp.add_open(sg.POSITION_OF[sg.SELL_MAKER], 1, D(grid), D(grid), 0.5)
    sp.residual_maker = D(residual)
    now = time.time()
    eng.compute(c, now)
    return eng, c, sp, now


def test_close_below_taker_minimum_is_not_quoted(tmp_path, fake_pair):
    eng, c, sp, now = _closing(tmp_path, fake_pair)
    _M, T = fake_pair
    T.markets["X"].min_notional = D("200")  # grid 1 @ ~101 = 101 USDC < 200 (the PUMP case: 8.3 < 10)
    assert eng.decide(c, now) is None
    assert "not closable" in c.status and c.unclosable
    T.markets["X"].min_notional = D("1")
    p = eng.decide(c, now)
    assert p and p.kind == "close" and p.direction == sg.BUY_MAKER and not c.unclosable


def test_close_takes_whole_grid_when_remainder_below_minimum(tmp_path, fake_pair):
    eng, c, sp, now = _closing(tmp_path, fake_pair, grid="1.5")
    _M, T = fake_pair
    T.markets["X"].min_notional = D("60")  # slice 0.99 would leave 0.51 (~51 USDC) that no venue accepts
    p = eng.decide(c, now)
    assert p and p.kind == "close" and p.qty == D("1.5")


def test_close_carries_pending_close_hedge(tmp_path, fake_pair):
    eng, c, sp, now = _closing(tmp_path, fake_pair, residual="0.4")  # 0.4 already bought back on the maker venue
    p = eng.decide(c, now)
    assert p and p.kind == "close" and p.qty == D("0.6") and p.carry_in == D("0.4")


def test_fully_closed_maker_leg_waits_for_pair_off(tmp_path, fake_pair):
    eng, c, sp, now = _closing(tmp_path, fake_pair, residual="1")
    assert eng.decide(c, now) is None and "close hedge pending" in c.status


def test_pair_off_closes_taker_leg_instead_of_reopening_maker(tmp_path, fake_pair):
    eng, c, sp, now = _closing(tmp_path, fake_pair, residual="1")
    M, T = fake_pair
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log  # v5.2 sold 1 on the maker venue again (re-opened the closed grid)
    assert len(T.ioc_log) == 1 and not T.ioc_log[0].is_buy and T.ioc_log[0].reduce_only
    assert not sp.grids and sp.residual_maker == 0 and sp.is_flat()


def test_pair_off_below_taker_minimum_never_flattens_maker(tmp_path, fake_pair):
    eng, c, sp, now = _closing(tmp_path, fake_pair, residual="1")
    M, T = fake_pair
    T.markets["X"].min_notional = D("1000")
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log and not T.ioc_log and sp.residual_maker == D("1") and sp.grids


def test_close_hedge_ignores_hedge_min_notional(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("0.05"))]  # ~5 USDC < hedge_min_notional_usdc 10, above the exchange minimum (1)
    r = run(M, T, plan(qty="0.05", kind="close", direction=sg.BUY_MAKER, required=-5.0))
    assert r.maker_filled == D("0.05") and r.hedged == D("0.05")
    M2, T2 = FakeVenue("lighter_rh"), FakeVenue("lighter")
    M2.set_book("X", "100.90", "101.00")
    T2.set_book("X", "99.90", "100.00")
    M2.maker_fills = [(0.05, D("0.05"))]
    r2 = run(M2, T2, plan(qty="0.05"))
    assert r2.maker_filled == D("0.05") and r2.taker_filled == 0  # opens still accumulate (carried dust)


def test_arcus_lost_submit_resolved_by_lookup():
    async def go():
        v = _arcus()

        async def empty(method, path, **kw):
            return {"orders": []}
        v._request = empty
        mk = OrderState("66", "arcus", "ENA-USD", True, D("100"), D("0.2"), True, True, status=OrderStatus.UNKNOWN)
        io = OrderState("67", "arcus", "ENA-USD", True, D("100"), D("0.2"), False, True, status=OrderStatus.UNKNOWN)
        for st in (mk, io):
            v._track(st)
            await v.refresh_order(st)
            assert st.status == OrderStatus.UNKNOWN  # young: keep looking
            st.created = time.time() - 31
            await v.refresh_order(st)
        assert mk.status == OrderStatus.CANCELED and mk.filled == 0  # post-only: retired, watched for late fills
        assert io.status == OrderStatus.REJECTED
        assert v.unacked_resolve_s() > 30

        async def failing(method, path, **kw):
            raise ArcusRejected("rate limited")
        v._request = failing
        st = OrderState("68", "arcus", "ENA-USD", True, D("100"), D("0.2"), True, True, status=OrderStatus.UNKNOWN)
        st.created = time.time() - 60
        v._track(st)
        await v.refresh_order(st)
        assert st.status == OrderStatus.UNKNOWN  # lookup failed: no conclusion
    asyncio.run(go())


class LostAckMaker(FakeVenue):
    """The first post-only order's send result is lost (UNKNOWN, no exchange id); the venue resolves it later."""

    def __init__(self, name, resolves=True):
        super().__init__(name)
        self.lost = None
        self.resolves = resolves

    def unacked_resolve_s(self):
        return 0.5

    async def place_post_only(self, symbol, is_buy, qty, price, reduce_only):
        if self.lost is None:
            st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, price, True, reduce_only,
                            status=OrderStatus.UNKNOWN)
            self._track(st)
            self.placed.append(st)
            self.lost = st
            return st
        return await super().place_post_only(symbol, is_buy, qty, price, reduce_only)

    async def refresh_order(self, st):
        if st is self.lost and self.resolves and time.time() - st.created > 0.3:
            st.apply(status=OrderStatus.CANCELED, reason="not found on exchange (never reached the book)")
            self._notify(st.client_id)
        return st


def _lost_pair(resolves):
    M, T = LostAckMaker("lighter_rh", resolves), FakeVenue("lighter")
    M.set_book("X", "100.90", "101.00")
    T.set_book("X", "99.90", "100.00")
    return M, T


def test_lost_maker_ack_is_looked_up_instead_of_halting():
    M, T = _lost_pair(True)
    M.maker_fills = [(0.05, D("1"))]
    r = run(M, T, plan(maker_timeout_s=3, order_final_timeout_s=0.1))
    assert not r.halted and len(M.placed) == 2 and r.maker_filled == D("1") and r.hedged == D("1")


def test_lost_maker_ack_never_resolved_still_halts():
    M, T = _lost_pair(False)
    r = run(M, T, plan(maker_timeout_s=3, order_final_timeout_s=0.1))
    assert r.halted and "outcome unknown" in r.error
