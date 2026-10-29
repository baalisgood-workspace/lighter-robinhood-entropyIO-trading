"""v5.7: autotune.mode apply (thresholds moved by the bot, kept across restarts) and the funding-carry switch."""
import asyncio
import json
import time
from decimal import Decimal as D

import pytest

from arb import tune
from arb.config import ConfigError
from arb.engine import signals as sg

from test_engine import make_engine
from test_v56 import _journal
from test_v561 import _cfg, _closes, _write


# ---------------------------------------------------------------- autotune apply
def test_apply_mode_is_valid_and_default_stays_advise(tmp_path):
    assert _cfg(tmp_path, mode="apply").autotune.mode == "apply"
    assert _cfg(tmp_path).autotune.mode == "advise" and not _cfg(tmp_path).autotune.apply_close_only
    with pytest.raises(ConfigError):
        _cfg(tmp_path, mode="auto")


def test_engine_applies_and_persists_threshold(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.3)
    eng.cfg.autotune.mode = "apply"
    _write(eng.journal_path, _closes("X", 40, 5.0, t1=0.3))
    rows = asyncio.run(eng.tune_once())
    assert rows[0]["action"] == "lower"
    assert abs(c.sym.strategy.initial_threshold_pct - 0.29) < 1e-9
    j = _journal(eng, "tune_apply")
    assert j and j[-1]["old_t1"] == 0.3 and j[-1]["new_t1"] == 0.29
    saved = json.loads((tmp_path / tune.APPLIED_FILE).read_text())
    assert saved["X"]["all"]["t1"] == 0.29 and saved["X"]["all"]["config_t1"] == 0.3
    # no closes at the new T1 yet: hold, nothing moves again
    asyncio.run(eng.tune_once())
    assert abs(c.sym.strategy.initial_threshold_pct - 0.29) < 1e-9
    # restart with the same config file value: the applied T1 is used again
    eng2, c2 = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.3)
    assert abs(c2.sym.strategy.initial_threshold_pct - 0.29) < 1e-9
    assert any("tune applied earlier: X: T1 0.3% -> 0.29%" in e for e in eng2.events)


def test_manual_config_edit_wins_over_applied(tmp_path, fake_pair):
    tune.write_applied(tmp_path / tune.APPLIED_FILE, {"X": {"all": {"t1": 0.29, "config_t1": 0.3, "ts": 1}}})
    _, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.2)  # the user edited the file to 0.2
    assert c.sym.strategy.initial_threshold_pct == 0.2


def test_second_step_keeps_config_origin(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.3)
    p = tmp_path / tune.APPLIED_FILE
    tune.record_applied(p, c.sym, "all", 0.3, 0.29, time.time())
    tune.record_applied(p, c.sym, "all", 0.29, 0.28, time.time())
    assert tune.read_applied(p)["X"]["all"] == {"t1": 0.28, "config_t1": 0.3, "ts": pytest.approx(time.time(), abs=5)}


def test_close_only_applied_only_when_allowed(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.25)
    eng.cfg.autotune.mode = "apply"
    eng.cfg.autotune.bounds = {"X": {"min": 0.1, "max": 0.25}}
    _write(eng.journal_path, _closes("X", 40, -5.0, t1=0.25))
    rows = asyncio.run(eng.tune_once())
    assert rows[0]["action"] == "close_only" and not c.sym.close_only
    eng.cfg.autotune.apply_close_only = True
    asyncio.run(eng.tune_once())
    assert c.sym.close_only
    _, c2 = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.25)
    assert c2.sym.close_only


def test_advise_mode_changes_nothing(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.3)
    _write(eng.journal_path, _closes("X", 40, 5.0, t1=0.3))
    asyncio.run(eng.tune_once())
    assert c.sym.strategy.initial_threshold_pct == 0.3 and not (tmp_path / tune.APPLIED_FILE).exists()


# ---------------------------------------------------------------- funding carry
def test_earned_funding_sign():
    # short maker / long taker earns when the maker rate is higher (longs pay shorts)
    assert sg.funding_earned_hourly_pct(sg.SELL_MAKER, 0.0001, 0.0) == pytest.approx(0.01)
    assert sg.funding_earned_hourly_pct(sg.BUY_MAKER, 0.0001, 0.0) == pytest.approx(-0.01)
    assert sg.funding_earned_hourly_pct(sg.BUY_MAKER, None, 0.0) is None


def test_carry_need_credit_and_floor():
    assert sg.carry_need(0.30, 0.01, 0.0, 0.005, 8, 0.0) == pytest.approx(0.22)
    assert sg.carry_need(0.30, 0.10, 0.02, 0.005, 8, 0.03) == pytest.approx(0.05)  # floor: fees + min_open
    assert sg.carry_need(0.30, 0.004, 0.0, 0.005, 8, 0.0) == 0.30  # below the activation rate
    assert sg.carry_need(0.01, 0.01, 0.02, 0.005, 8, 0.0) == 0.01  # never raised


def _books(M, T, maker_mid):
    M.set_book("X", f"{maker_mid - 0.05:.2f}", f"{maker_mid + 0.05:.2f}")
    T.set_book("X", "99.95", "100.05")


def test_carry_lowers_trigger_only_when_switched_on(tmp_path, fake_pair):
    M, T = fake_pair
    _books(M, T, 100.30)  # sell-maker dev ~0.20 %
    M.funding_hourly["X"], T.funding_hourly["X"] = 0.0001, 0.0  # short maker earns 0.01 %/h
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.30)
    now = time.time()
    eng.compute(c, now)
    assert eng.decide(c, now) is None and c.need_sell == pytest.approx(0.30)
    eng2, c2 = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.30, funding_carry=True)
    eng2.compute(c2, now)
    assert c2.need_sell == pytest.approx(0.22) and c2.need_buy == pytest.approx(0.31)  # buy side pays funding
    p = eng2.decide(c2, now)
    assert p and p.kind == "open" and p.direction == sg.SELL_MAKER and p.required_dev == pytest.approx(0.22)


def test_carry_off_under_close_only(tmp_path, fake_pair):
    M, T = fake_pair
    _books(M, T, 100.30)
    M.funding_hourly["X"], T.funding_hourly["X"] = 0.0001, 0.0
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.30, funding_carry=True, trading={"close_only": True})
    eng.compute(c, time.time())
    assert c.need_sell == pytest.approx(0.30)


def _held(tmp_path, fake_pair, earn, age_h=1.0, **kw):
    M, T = fake_pair
    _books(M, T, 100.00)  # spread back at the basis: grid 1 would close (close level 0.15)
    M.funding_hourly["X"], T.funding_hourly["X"] = earn / 100, 0.0
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.30, funding_carry=True, max_grids=1, **kw)
    sp = eng.position(c)
    sp.add_open(sg.SHORT_MAKER, 1, D("1"), D("1"), 0.3)
    sp.grids[0].opened_ts = time.time() - age_h * 3600
    return eng, c


def test_carry_holds_close_while_funding_pays(tmp_path, fake_pair):
    eng, c = _held(tmp_path, fake_pair, earn=0.01)
    now = time.time()
    eng.compute(c, now)
    assert eng.decide(c, now) is None and c.status.startswith("funding carry: holding")
    assert _journal(eng, "carry_hold")[-1]["hold"] is True


@pytest.mark.parametrize("earn,age_h", [(0.0005, 1.0), (0.01, 80.0)])  # funding faded / held too long
def test_carry_closes_when_funding_fades_or_too_old(tmp_path, fake_pair, earn, age_h):
    eng, c = _held(tmp_path, fake_pair, earn=earn, age_h=age_h)
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    assert p and p.kind == "close"


def test_carry_validation(tmp_path, fake_pair):
    with pytest.raises(ConfigError):
        make_engine(tmp_path, fake_pair, funding_carry=True, carry_exit_hourly_pct=0.01, carry_min_hourly_pct=0.005)
