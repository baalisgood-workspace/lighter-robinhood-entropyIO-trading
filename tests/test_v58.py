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
