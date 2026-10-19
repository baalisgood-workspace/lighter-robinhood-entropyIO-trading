from decimal import Decimal as D

from arb.config import BasisConfig
from arb.engine import signals as sg
from arb.engine.basis import BasisState
from arb.engine.state import SymbolPosition
from arb.models import Level, OrderBook, OrderState, OrderStatus
from arb.venues.entropy import hl_round_price


def test_hl_round_price():
    assert hl_round_price(D("1523.456"), 3, up=False) == D("1523.4")
    assert hl_round_price(D("1523.456"), 3, up=True) == D("1523.5")
    assert hl_round_price(D("12345.67"), 2, up=False) == D("12345")
    assert hl_round_price(D("0.0123456789"), 0, up=False) == D("0.012345")
    assert hl_round_price(D("9999.95"), 3, up=True) == D("10000")


def test_order_state_monotonic():
    st = OrderState("1", "v", "X", True, D("1"), D("10"), True, False)
    st.apply(filled=D("0.5"), status=OrderStatus.OPEN)
    st.apply(filled=D("0.3"))
    assert st.filled == D("0.5")
    st.apply(status=OrderStatus.CANCELED)
    st.apply(status=OrderStatus.OPEN)
    assert st.status == OrderStatus.CANCELED


def test_basis_warmup_gap_reset():
    cfg = BasisConfig(ema_seconds=100, warmup_seconds=50, drift_seconds=20, max_drift_pct=5, max_gap_seconds=5,
                      recovery_seconds=10, reset_after_seconds=60, sample_interval_seconds=1)
    b = BasisState(cfg)
    t = 1000.0
    for i in range(60):
        b.observe(t + i, 1.0)
    assert b.ready(t + 59)[0]
    b.observe(t + 59 + 8, 1.0)  # gap > max_gap -> recovery
    ok, why = b.ready(t + 67)
    assert not ok and "recovering" in why
    for i in range(1, 12):
        b.observe(t + 67 + i, 1.0)
    assert b.ready(t + 78)[0]
    b.observe(t + 78 + 100, 2.0)  # long gap -> reset, warmup again
    assert b.ema == 2.0 and not b.ready(t + 178)[0]


def _book(bids, asks):
    return OrderBook("v", "X", bids=[Level(D(p), D(s)) for p, s in bids], asks=[Level(D(p), D(s)) for p, s in asks], ts_local=1)


def test_levels_and_edges():
    assert sg.open_level(0.3, 0.15, 1) == 0.3
    assert abs(sg.open_level(0.3, 0.15, 3) - 0.6) < 1e-12
    assert sg.close_level(0.3, 0.15, 0.5, 1) == 0.15
    assert abs(sg.close_level(0.3, 0.15, 0.5, 3) - 0.45) < 1e-12
    assert abs(sg.raw_edge(sg.SELL_MAKER, D("101"), D("100")) - 1.0) < 1e-9
    assert abs(sg.adjusted_edge(sg.SELL_MAKER, 1.0, 0.4) - 0.6) < 1e-9
    assert abs(sg.adjusted_edge(sg.BUY_MAKER, -0.5, 0.4) + 0.1) < 1e-9


def test_taker_fill_vwap_and_band():
    b = _book([("99", "1")], [("100", "1"), ("100.05", "1"), ("103", "5")])
    f = sg.taker_fill(b, True, D("2"), 5, 0.10)
    assert f.vwap == D("100.025") and f.worst == D("100.05")
    assert sg.taker_fill(b, True, D("3"), 5, 0.10) is None  # 103 is outside the band
    assert sg.taker_fill(b, True, D("3"), 5, 5.0) is not None


def test_build_quote_with_basis():
    m = _book([("100.9", "5")], [("101.0", "5")])
    t = _book([("99.9", "5")], [("100.0", "5")])
    q = sg.build_quote(sg.SELL_MAKER, m, t, D("1"), 0.5, "join", D("0.01"), 5, 0.1)
    assert q.ok and abs(q.raw - 1.0) < 1e-9 and abs(q.dev - 0.5) < 1e-9


def test_position_grids():
    sp = SymbolPosition("a+b", "X")
    sp.add_open(sg.SHORT_MAKER, 1, D("1"), D("2"), 0.4)
    sp.add_open(sg.SHORT_MAKER, 1, D("1"), D("2"), 0.6)
    sp.add_open(sg.SHORT_MAKER, 2, D("2"), D("2"), 0.8)
    assert len(sp.grids) == 2 and sp.total_qty == D("4")
    assert sp.expected_maker_qty() == D("-4") and sp.expected_taker_qty() == D("4")
    sp.reduce_close(D("3"))
    assert sp.total_qty == D("1") and len(sp.grids) == 1
    sp.reduce_close(D("1"))
    assert sp.is_flat() and sp.direction is None


def test_report_summarize():
    from arb.report import summarize
    rows = [{"pair": "a+b", "maker": "b", "taker": "a", "symbol": "X", "basis_ready": "1", "basis_ema": "0.1",
             "dev_sell_maker": str(v), "dev_buy_maker": "-0.5"} for v in (0.1, 0.2, 0.4, 0.5)]
    rows.append({**rows[0], "basis_ready": "0", "dev_sell_maker": "9"})
    s = summarize(rows, 0.3)[0]
    assert s["n"] == 4 and s["max"] == 0.5 and s["pct_above"] == 50.0
