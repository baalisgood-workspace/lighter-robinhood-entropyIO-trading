"""v5.8: hold to profit - frozen_close (profitable closes while the basis is not ready) and the stop loss."""
import json
import time
from decimal import Decimal as D

import pytest

from arb.engine import signals as sg
from arb.engine.executor import ExecResult
from arb.engine.state import Grid
from test_engine import make_engine


def test_entry_raw_any_age():
    g = Grid(level=1, qty=D("2"), target_qty=D("2"))
    now = time.time()
    g.lots = [[D("1"), 0.30, now - 99999], [D("1"), 0.10, now - 10]]
    assert g.entry_raw(D("1")) == pytest.approx(0.30)
    assert g.entry_raw(D("2")) == pytest.approx(0.20)
    assert g.close_floor(D("2"), now, 600, 0.01) == pytest.approx(0.01 - 0.10)  # unchanged


def _opened(tmp_path, fake_pair, **strategy):
    """Sell maker 101.0 / buy taker 100.9 (raw open ~0.099%), then a close quote that loses ~0.02%."""
    M, T = fake_pair
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.3, first_close_ratio=0.5,
                         min_close_profit_pct=0.01, **strategy)
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    eng.apply_result(c, p, ExecResult(maker_filled=p.qty, maker_cost=p.qty * D("101.0"), taker_filled=p.qty,
                                      taker_cost=p.qty * D("100.9")))
    M.set_book("X", "100.00", "100.10")
    T.set_book("X", "99.88", "99.98")  # close raw -0.12%: entry 0.099 + -0.12 < 0
    c.mids_m.clear(); c.mids_t.clear()
    return eng, c, now


def _freeze(eng, c, now):
    eng.compute(c, now)
    c.ready, c.status = False, "basis drift 0.400% > 0.3%"


def test_frozen_holds_losing_grid_then_closes_in_profit(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c, now = _opened(tmp_path, fake_pair, frozen_close=True)
    _freeze(eng, c, now)
    assert eng.decide(c, now) is None and "holding" in c.status
    T.set_book("X", "100.00", "100.10")  # close raw 0%: entry 0.099% + 0 >= 0.01 floor
    c.mids_m.clear(); c.mids_t.clear()
    _freeze(eng, c, now)
    p = eng.decide(c, now)
    assert p and p.kind == "close" and p.required_raw == pytest.approx(0.01 - c_entry(eng, c))


def c_entry(eng, c):
    return eng.position(c).grids[-1].entry_raw(D("1"))


def test_frozen_without_flag_does_nothing(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c, now = _opened(tmp_path, fake_pair)
    T.set_book("X", "100.00", "100.10")
    c.mids_m.clear(); c.mids_t.clear()
    _freeze(eng, c, now)
    assert eng.decide(c, now) is None


def test_frozen_never_opens(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, frozen_close=True, stop_loss_pct=1.0)
    now = time.time()
    eng.compute(c, now)
    c.ready = False
    assert eng.decide(c, now) is None  # strong open signal, but no grid and no basis


def test_stop_loss_closes_at_any_spread(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c, now = _opened(tmp_path, fake_pair, stop_loss_pct=1.0, close_floor_timeout_s=172800)
    eng.compute(c, now)
    assert eng.decide(c, now) is None  # -0.02% loss: held by the floor
    T.set_book("X", "98.80", "98.90")  # close raw ~ -1.2%: loses ~1.1% >= 1.0
    c.mids_m.clear(); c.mids_t.clear()
    _freeze(eng, c, now)  # also works while frozen
    p = eng.decide(c, now)
    assert p and p.kind == "close" and p.direction == sg.BUY_MAKER and "stop loss" in c.status
    assert p.required_raw < -2.0  # one more stop width of room
    ev = [json.loads(x) for x in eng.journal_path.read_text().splitlines()]
    assert sum(e["kind"] == "stop_loss" for e in ev) == 1
    eng.decide(c, now)
    ev = [json.loads(x) for x in eng.journal_path.read_text().splitlines()]
    assert sum(e["kind"] == "stop_loss" for e in ev) == 1  # reported once per grid


def test_stop_loss_config_validation(tmp_path, fake_pair):
    from arb.config import ConfigError
    with pytest.raises(ConfigError):
        make_engine(tmp_path, fake_pair, stop_loss_pct=0)


def test_lighter_entropy_config_loads():
    from pathlib import Path
    from arb.config import load_config
    cfg = load_config(Path(__file__).resolve().parents[1] / "config" / "lighter_entropy.yaml")
    st = cfg.symbols["ANTHROPIC"].strategy
    assert st.frozen_close and st.stop_loss_pct is None and st.close_floor_timeout_s >= 365 * 86400
    assert cfg.symbols["ANTHROPIC"].close_only and not cfg.symbols["SNDK"].close_only
    assert cfg.symbols["SNDK"].risk.min_liq_distance_pct == 15
    assert {"NVDA", "DRAM", "NBIS", "CRCL"} <= set(cfg.symbols)


# ------------------------------------------------------------------ v5.8.5 margin guard
def test_liq_price_parsing():
    from arb.models import liq_price
    assert liq_price("1650.5") == D("1650.5")
    assert liq_price(None) is None and liq_price("") is None and liq_price("0") is None and liq_price("-1") is None


def test_near_liquidation_blocks_opens_and_warns(tmp_path, fake_pair):
    from arb.models import Position
    M, T = fake_pair  # maker mid 100.95, taker mid 99.95
    eng, c = make_engine(tmp_path, fake_pair, risk={"min_liq_distance_pct": 15})
    now = time.time()
    eng.exch_positions = {"lighter_rh": {"X": Position("lighter_rh", "X", D("-1"), D("100"), D("130"))},
                          "lighter": {"X": Position("lighter", "X", D("1"), D("100"), D("70"))}}
    eng.compute(c, now)
    assert eng.decide(c, now).kind == "open"  # ~29-30% away: fine
    eng.exch_positions["lighter_rh"]["X"] = Position("lighter_rh", "X", D("-1"), D("100"), D("110"))  # ~9% away
    eng.compute(c, now)
    assert eng.decide(c, now) is None and "距强平" in c.warn
    ev = [json.loads(x) for x in eng.journal_path.read_text().splitlines()]
    assert any(e["kind"] == "warning" and e["key"].startswith("liq:") for e in ev)


def test_margin_guard_off_by_default(tmp_path, fake_pair):
    from arb.models import Position
    eng, c = make_engine(tmp_path, fake_pair)
    now = time.time()
    eng.exch_positions = {"lighter_rh": {"X": Position("lighter_rh", "X", D("-1"), D("100"), D("101.5"))}}
    eng.compute(c, now)
    assert eng.decide(c, now).kind == "open"


# ------------------------------------------------------------------ v5.8.6 hedging in a fast market
def _hedge_plan(**ex):
    from test_ported import plan
    return plan(**ex)


def test_rescue_hedges_with_wider_slippage(fake_pair):
    """A taker book that ran away: the buy hedge only fills once its limit is 0.5% above the (stale) best ask."""
    from test_ported import run
    from arb.models import OrderState, OrderStatus
    M, T = fake_pair  # maker sells at 101.00, hedge buys on the taker, ask 100.00
    M.maker_fills = [(0.05, D("1"))]
    real = T.place_ioc

    async def ioc(symbol, is_buy, qty, limit, reduce_only):
        if limit < D("100.50"):
            st = OrderState("z", T.name, symbol, is_buy, qty, limit, post_only=False, reduce_only=reduce_only)
            st.status = OrderStatus.CANCELED
            T.ioc_log.append(st)
            return st
        return await real(symbol, is_buy, qty, limit, reduce_only)
    T.place_ioc = ioc
    r = run(M, T, _hedge_plan(hedge_max_attempts=4))
    assert not r.halted and r.taker_filled == D("1")
    assert r.zero_fill_hedges == 5 and any("rescue 2/3" in e for e in r.events)  # 4 regular + rescue 1 at ~0.47%
    assert max(o.price for o in T.ioc_log) <= D("101.00")  # never beyond the 1% rescue cap


def test_rescue_still_halts_when_nothing_fills(fake_pair):
    from test_ported import run
    M, T = fake_pair
    M.maker_fills = [(0.05, D("1"))]
    T.ioc_ratio = D("0")
    r = run(M, T, _hedge_plan(hedge_max_attempts=4))
    assert r.halted and r.zero_fill_hedges == 7 and "manual intervention" in r.error
    worst = max(abs(o.price - D("100.00")) / D("100.00") * 100 for o in T.ioc_log)  # buy hedge vs ask 100.00
    assert worst <= D("1.0") + D("0.01")


def test_retry_waits_for_a_newer_book(fake_pair):
    import asyncio
    import time as _t
    from arb.engine.executor import Executor
    M, T = fake_pair
    T.books["X"].ts_local = _t.time() - 5  # last update 5s ago
    t0 = _t.monotonic()
    asyncio.run(Executor._await_book_after(T, "X", _t.time(), 0.2))
    assert 0.18 <= _t.monotonic() - t0 < 0.5  # waited, gave up at the timeout
    T.books["X"].ts_local = _t.time() + 1
    t0 = _t.monotonic()
    asyncio.run(Executor._await_book_after(T, "X", _t.time(), 0.2))
    assert _t.monotonic() - t0 < 0.05  # newer book: no wait


# ------------------------------------------------------------------ v5.8.7 cancel confirmation
def _slow_cancel(M, ignore: int):
    """The first `ignore` cancels do nothing (venue has not reported the order index yet)."""
    real = M.cancel
    calls = {"n": 0}
    M.cfg.unknown_lookup_s = 0  # the fake never reports an exchange id: do not wait for the lookup

    async def cancel(st):
        calls["n"] += 1
        if calls["n"] <= ignore:
            raise RuntimeError("cannot cancel, exchange order index unknown")
        await real(st)
    M.cancel = cancel
    return calls


def test_cancel_is_retried_instead_of_halting(fake_pair):
    from test_ported import run
    M, T = fake_pair
    M.maker_fills = []  # never fills: the slice ends with a cancel
    calls = _slow_cancel(M, ignore=1)
    r = run(M, T, _hedge_plan(maker_timeout_s=0.2, order_final_timeout_s=0.2, cancel_confirm_s=5))
    assert not r.halted, r.error
    assert calls["n"] == 2 and all(o.status.final for o in M.placed)


def test_cancel_still_halts_after_the_budget(fake_pair):
    from test_ported import run
    M, T = fake_pair
    M.maker_fills = []
    calls = _slow_cancel(M, ignore=100)
    r = run(M, T, _hedge_plan(maker_timeout_s=0.2, order_final_timeout_s=0.2, cancel_confirm_s=0.5))
    assert r.halted and "not final after cancel" in r.error
    assert calls["n"] >= 2  # re-cancelled before giving up (the halt cleanup may cancel again)
