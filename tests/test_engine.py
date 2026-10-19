import time
from decimal import Decimal as D

from arb.config import parse_config
from arb.engine import signals as sg
from arb.engine.engine import Engine
from arb.engine.executor import ExecResult
from arb.models import Balance


def make_engine(tmp_path, fake_pair, **strategy):
    M, T = fake_pair
    raw = {"venues": {"lighter": {"enabled": True}, "lighter_rh": {"enabled": True}},
           "market_data": {"watch_all_venues": False}, "paths": {"data_dir": str(tmp_path)},
           "defaults": {"basis": {"enabled": False}, "strategy": strategy},
           "symbols": {"X": {"venues": {"lighter": "X", "lighter_rh": "X"}}}}
    cfg = parse_config(raw)
    eng = Engine(cfg, {"lighter_rh": M, "lighter": T}, live=True)
    eng.build_contexts()
    now = time.time()
    for n in ("lighter", "lighter_rh"):
        eng.balances[n] = Balance(n, D("5000"), D("5000"), ts=now)
    return eng, eng.ctxs[0]


def test_open_signal(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    now = time.time()
    eng.compute(c, now)
    assert c.q_sell.ok and c.q_sell.dev > 0.9
    p = eng.decide(c, now)
    assert p and p.kind == "open" and p.direction == sg.SELL_MAKER and p.level == 1


def test_no_signal_below_threshold(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=1.5)
    now = time.time()
    eng.compute(c, now)
    assert eng.decide(c, now) is None and abs(c.next_open - 1.5) < 1e-9


def test_apply_result_then_close(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c = make_engine(tmp_path, fake_pair)
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    res = ExecResult(maker_filled=p.qty, maker_cost=p.qty * D("101"), taker_filled=p.qty, taker_cost=p.qty * D("100"))
    eng.apply_result(c, p, res)
    sp = eng.position(c)
    assert sp.direction == sg.SHORT_MAKER and sp.total_qty == p.qty
    # spread collapses / inverts -> close signal (buy back on maker, sell on taker)
    M.set_book("X", "99.90", "100.00")
    T.set_book("X", "100.10", "100.20")
    c.mids_m.clear()
    c.mids_t.clear()  # the jump itself would trip the stability filter
    eng.compute(c, now)
    cp = eng.decide(c, now)
    assert cp, c.status and cp.kind == "close" and cp.direction == sg.BUY_MAKER and cp.reduce_only


def test_close_only_blocks_open(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    eng.cfg.close_only = True
    now = time.time()
    eng.compute(c, now)
    assert eng.decide(c, now) is None


def test_parallel_instances_do_not_share_a_symbol(tmp_path, fake_pair):
    import asyncio

    import pytest

    from arb.config import ConfigError
    M, T = fake_pair
    raw = {"venues": {"lighter": {"enabled": True}, "lighter_rh": {"enabled": True}},
           "paths": {"data_dir": str(tmp_path)}, "defaults": {"basis": {"enabled": False}},
           "symbols": {"X": {"venues": {"lighter": "X", "lighter_rh": "X"}}}}
    cfg = parse_config(raw)

    async def go():
        e1 = Engine(cfg, {"lighter_rh": M, "lighter": T}, live=True, data_dir=tmp_path / "lighter+lighter_rh")
        e1.build_contexts()
        await e1.preflight()
        assert e1.active_ctxs()
        e2 = Engine(cfg, {"lighter_rh": M, "lighter": T}, live=True, data_dir=tmp_path / "copy")
        e2.build_contexts()
        with pytest.raises(ConfigError, match="owned by another"):
            await e2.preflight()
        await e1.shutdown()
        e3 = Engine(cfg, {"lighter_rh": M, "lighter": T}, live=True, data_dir=tmp_path / "copy")
        e3.build_contexts()
        await e3.preflight()  # released on shutdown
        await e3.shutdown()
    asyncio.run(go())
