"""v5.2: no maker fill may be lost (late fills, fills during the hedge, orders that never reached the book)."""
import asyncio
import time
from decimal import Decimal as D

from arb.config import VenueConfig, parse_config
from arb.models import Balance, MarketInfo, OrderState, OrderStatus, Position
from arb.engine import signals as sg
from arb.venues.arcus import ArcusVenue
from arb.venues.lighter import LighterVenue

from conftest import FakeVenue
from test_engine import make_engine
from test_executor import plan, run


class SlowTaker(FakeVenue):
    async def place_ioc(self, symbol, is_buy, qty, limit_price, reduce_only):
        await asyncio.sleep(0.15)  # the maker order completes while the hedge is in flight
        return await super().place_ioc(symbol, is_buy, qty, limit_price, reduce_only)


def test_fill_arriving_during_hedge_is_not_lost():
    M, T = FakeVenue("lighter_rh"), SlowTaker("lighter")
    M.set_book("X", "100.90", "101.00")
    T.set_book("X", "99.90", "100.00")
    M.maker_fills = [(0.05, D("0.6")), (0.1, D("0.4"))]
    r = run(M, T, plan(maker_timeout_s=1.5))
    assert r.maker_filled == D("1") and r.taker_filled == D("1") and r.hedged == D("1") and not r.halted
    assert len(M.placed) == 1  # v5.1 re-quoted the "missing" 0.4 although it had filled


def test_late_fill_after_forget_is_reported():
    async def go():
        v = FakeVenue("arcus")
        st = OrderState("9", "arcus", "X", False, D("5"), D("1"), True, False, status=OrderStatus.OPEN)
        v._track(st)
        st.apply(status=OrderStatus.CANCELED)
        v.forget(st)
        assert "9" not in v.orders and v._lookup("9") is st
        st.apply(filled=D("0.7"))
        v._notify("9")
        v._notify("9")  # repeated push: counted once
        fills = v.drain_late_fills()
        assert len(fills) == 1 and fills[0]["qty"] == D("0.7") and not fills[0]["is_buy"]
        v.drop_retired("X")
        assert v._lookup("9") is None
    asyncio.run(go())


def test_rejected_orders_are_not_retired():
    v = FakeVenue("arcus")
    st = OrderState("8", "arcus", "X", True, D("1"), D("1"), True, False, status=OrderStatus.REJECTED)
    v._track(st)
    v.forget(st)
    assert v._lookup("8") is None


def _arcus():
    cfg = parse_config({"venues": {}, "symbols": {"S": {"venues": {"lighter": "S", "arcus": "ENA-USD"}}}}).venues["arcus"]
    v = ArcusVenue(cfg, env={"ARCUS_ACCOUNT_ADDRESS": "0x" + "ab" * 20, "ARCUS_ACCOUNT_INDEX": "0"})
    v.markets["ENA-USD"] = MarketInfo("arcus", "ENA-USD", D("0.1"), D("0.1"), D("5"), D("0.0001"),
                                      extra={"market_id": 7, "status": "ONLINE"})
    return v


def test_arcus_cancel_push_before_fill_push():
    """The ENA case: CANCELED (0 filled) arrives first, the 69.7 fill only after the executor forgot the order."""
    async def go():
        v = _arcus()
        assert v.cfg.verify_cancel_rest is True
        calls = []

        async def fake_refresh(st):
            calls.append(st.client_id)
            return st
        v.refresh_order = fake_refresh
        v.cfg.verify_cancel_delays_s = [0.01]
        st = OrderState("55", "arcus", "ENA-USD", False, D("504.6"), D("0.198"), True, False, status=OrderStatus.OPEN)
        v._track(st)
        env = {"id": v.address, "accountIndex": 0}
        v._on_ws({"type": "channel_data", "channel": "orders", **env, "contents": {"orders": [
            {"orderId": "o-55", "clientId": "55", "status": "CANCELED", "originalSize": "504.6", "remainingSize": "504.6"}]}})
        assert st.status == OrderStatus.CANCELED and st.filled == 0
        v.forget(st)  # executor: final, nothing filled
        v._on_ws({"type": "channel_data", "channel": "userFills", **env, "contents": {"fills": [
            {"tradeId": "t9", "orderId": "o-55", "size": "69.7", "price": "0.198"}]}})
        fills = v.drain_late_fills()
        assert len(fills) == 1 and fills[0]["qty"] == D("69.7") and fills[0]["symbol"] == "ENA-USD"
        await asyncio.sleep(0.05)
        assert calls == ["55"]  # canceled maker orders are re-read from REST as well
    asyncio.run(go())


def test_lighter_order_that_never_reached_the_book_is_canceled():
    async def go():
        v = LighterVenue(VenueConfig(name="lighter_rh", rest_url="https://x", ws_url="wss://x", unknown_lookup_s=10), env={})

        async def nothing(st):
            return None
        v._find_order = nothing
        st = OrderState("77", "lighter_rh", "OPENAI", False, D("0.0091"), D("1572"), True, True)
        v._track(st)
        await v.refresh_order(st)
        assert not st.status.final  # young: keep looking
        st.created = time.time() - 30
        await v.refresh_order(st)
        assert st.status == OrderStatus.CANCELED and st.filled == 0
    asyncio.run(go())


def test_engine_books_late_fill_as_residual(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    M, _T = fake_pair
    M.late_fills.append({"ts": time.time(), "venue": M.name, "symbol": "X", "is_buy": False, "qty": D("0.7"),
                         "price": D("101"), "client_id": "1", "post_only": True})
    eng._book_late_fills()
    sp = eng.position(c)
    assert sp.residual_maker == D("-0.7") and sp.residual_taker == 0 and eng.reconcile_due
    assert not M.late_fills


def _reconcile_with(eng, M, T, maker_qty, taker_qty):
    async def pm():
        return {"X": Position(M.name, "X", D(maker_qty))} if maker_qty else {}

    async def pt():
        return {"X": Position(T.name, "X", D(taker_qty))} if taker_qty else {}
    M.positions, T.positions = pm, pt
    return asyncio.run(eng.reconcile())


def _grid(eng, c):
    sp = eng.position(c)
    sp.add_open(sg.POSITION_OF[sg.SELL_MAKER], 1, D("1"), D("1"), 0.5)
    return sp


def test_reconcile_halts_by_default(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    M, T = fake_pair
    _grid(eng, c)
    _reconcile_with(eng, M, T, "-1.5", "1")
    _reconcile_with(eng, M, T, "-1.5", "1")
    assert eng.halted and "auto-align off" in eng.halted


def test_reconcile_auto_align_single_leg(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, risk={"auto_adopt_max_usdc": 100})
    M, T = fake_pair
    sp = _grid(eng, c)
    assert not _reconcile_with(eng, M, T, "-1.5", "1")
    assert _reconcile_with(eng, M, T, "-1.5", "1") and not eng.halted
    assert sp.residual_maker == D("-0.5") and eng.mismatch_strikes == 0
    assert _reconcile_with(eng, M, T, "-1.5", "1")  # consistent now


def test_reconcile_auto_align_refuses_large_or_two_legs(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, risk={"auto_adopt_max_usdc": 20})
    M, T = fake_pair
    _grid(eng, c)
    _reconcile_with(eng, M, T, "-2", "1")
    _reconcile_with(eng, M, T, "-2", "1")  # 1 x ~101 USDC > 20
    assert eng.halted and "auto_adopt_max_usdc" in eng.halted
    eng2, c2 = make_engine(tmp_path / "b", fake_pair, risk={"auto_adopt_max_usdc": 1000})
    _grid(eng2, c2)
    _reconcile_with(eng2, M, T, "-1.5", "1.5")
    _reconcile_with(eng2, M, T, "-1.5", "1.5")
    assert eng2.halted and "both legs" in eng2.halted
