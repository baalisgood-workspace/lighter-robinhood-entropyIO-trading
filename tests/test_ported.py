"""Tests for the improvements ported from arcus-maker-lighter-taker."""
import asyncio
import json
import time
from decimal import Decimal as D

import pytest

from arb.config import ConfigError, ExecutionConfig, Pair, RiskConfig, SymbolConfig
from arb.engine import signals as sg
from arb.engine.executor import ExecPlan, ExecResult, Executor
from arb.engine.state import Grid
from test_engine import make_engine


def plan(qty="1", required=0.3, kind="open", direction=sg.SELL_MAKER, risk=None, **ex):
    e = dict(maker_timeout_s=0.6, order_poll_interval_s=0.05, order_final_timeout_s=0.3, requote_min_interval_s=100)
    e.update(ex)
    sym = SymbolConfig(key="X", venues={"lighter": "X", "lighter_rh": "X"}, execution=ExecutionConfig(**e),
                       risk=RiskConfig(**(risk or {})))
    return ExecPlan(kind, Pair("lighter", "lighter_rh", "lighter_rh"), sym, "X", "X", direction, D(qty), 1, required, lambda: 0.0)


def run(M, T, p):
    return asyncio.run(Executor({"lighter_rh": M, "lighter": T}).run(p))


# ------------------------------------------------------------------ threshold quotes
def test_threshold_price_sell_and_buy(fake_pair):
    M, T = fake_pair  # maker 100.90/101.00, taker 99.90/100.00
    ex = Executor({"lighter_rh": M, "lighter": T})
    p = plan(required=0.3, maker_price_mode="threshold")
    # sell maker, hedge buys taker at 100.00 -> 100.00 * 1.003 = 100.30, passive (> bid 100.90?) no -> clamp to bid+tick
    assert ex.threshold_price(p, D("1")) == D("100.91")
    M.set_book("X", "99.00", "101.00")
    assert ex.threshold_price(p, D("1")) == D("100.30")
    b = plan(required=0.3, direction=sg.BUY_MAKER, maker_price_mode="threshold")
    # buy maker, hedge sells taker at 99.90 -> 99.90/1.003 = 99.601.. -> round down 99.60
    assert ex.threshold_price(b, D("1")) == D("99.60")


def test_threshold_mode_rests_and_requotes_on_move(fake_pair):
    M, T = fake_pair
    M.set_book("X", "99.00", "101.00")
    p = plan(required=0.3, maker_price_mode="threshold", maker_timeout_s=1.0, max_requotes_per_s=20,
             requote_threshold_pct=0.006)

    async def go():
        ex = Executor({"lighter_rh": M, "lighter": T})
        task = asyncio.create_task(ex.run(p))
        await asyncio.sleep(0.3)
        T.set_book("X", "100.40", "100.50")  # taker moved up 0.5% -> fair sell price moves
        return await task
    r = asyncio.run(go())
    prices = [o.price for o in M.placed]
    assert prices[0] == D("100.30") and D("100.81") in prices and r.requotes >= 1
    assert all(o.status.final for o in M.placed)


# ------------------------------------------------------------------ hedge retry budget / dust
def test_partial_hedges_do_not_consume_retry_budget(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("1"))]
    T.ioc_ratio = D("0.5")  # every IOC fills half of what is left
    r = run(M, T, plan(hedge_max_attempts=1, hedge_max_orders=12, hedge_min_notional_usdc=0))
    assert r.hedge_orders >= 6 and r.zero_fill_hedges <= 1  # partials did not use up the budget of 1


def test_zero_fill_budget_then_manual_intervention(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("1"))]
    T.ioc_ratio = D("0")
    r = run(M, T, plan(hedge_max_attempts=3))
    assert r.halted and "manual intervention" in r.error and r.zero_fill_hedges == 3
    assert not M.ioc_log  # no automatic unwind by default


def test_dust_accumulates_until_hedge_minimum(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("0.05")), (0.05, D("0.05")), (0.05, D("0.05"))]  # $5 each at ~100
    r = run(M, T, plan(hedge_min_notional_usdc=9))
    assert r.taker_filled == D("0.10") and r.maker_filled == D("0.15")  # 0.05 left as dust
    assert all(o.qty >= D("0.10") for o in T.ioc_log)


def test_carry_in_hedged_with_next_slice(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("1"))]
    p = plan(hedge_min_notional_usdc=10)
    p.carry_in = D("0.05")
    r = run(M, T, p)
    assert r.taker_filled == D("1.05") and r.hedged == D("1.05")


# ------------------------------------------------------------------ close-profit floor
def test_grid_close_floor():
    g = Grid(level=1, qty=D("2"), target_qty=D("2"))
    now = time.time()
    g.lots = [[D("1"), 0.30, now - 1000], [D("1"), 0.10, now - 10]]
    assert g.close_floor(D("1"), now, 600, 0.01) is None  # FIFO: the oldest lot is expired
    assert g.close_floor(D("2"), now, 600, 0.01) == pytest.approx(0.01 - 0.10)
    g.take_lots(D("1.5"))
    assert g.lots == [[D("0.5"), 0.10, now - 10]]


def test_close_held_by_profit_floor(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.3, first_close_ratio=0.5)
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    # opening sold maker at 101 / bought taker at 100.9 -> raw open edge only ~0.1%
    res = ExecResult(maker_filled=p.qty, maker_cost=p.qty * D("101.0"), taker_filled=p.qty, taker_cost=p.qty * D("100.9"))
    eng.apply_result(c, p, res)
    # close quote: buy maker at 100.00 (join bid), sell taker at 99.88 -> raw close -0.12% >= -0.15 close level,
    # but open raw 0.099% + close -0.12% < 0.01% floor
    M.set_book("X", "100.00", "100.10")
    T.set_book("X", "99.88", "99.98")
    c.mids_m.clear(); c.mids_t.clear()
    eng.compute(c, now)
    assert eng.decide(c, now) is None and "profit floor" in c.status
    eng.cfg.symbols["X"].strategy.min_close_profit_pct = None
    cp = eng.decide(c, now)
    assert cp and cp.kind == "close"


# ------------------------------------------------------------------ readiness warmup
def test_fresh_warmup_and_reset_on_reconnect(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c = make_engine(tmp_path, fake_pair, risk={"warmup_fresh_s": 60})
    now = time.time()
    eng.compute(c, now)
    assert not c.ready and "warmup" in c.status
    for k in range(1, 14):
        eng.compute(c, now + 5 * k)
    assert c.ready
    M.on_connected()  # reconnect -> new generation -> warmup restarts
    M.set_book("X", "100.90", "101.00")
    eng.compute(c, now + 66)
    assert not c.ready


# ------------------------------------------------------------------ halt lock / capital
def test_halt_writes_lock_and_preflight_refuses(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    eng.halt("test failure")
    eng.halt("second cause")
    lock = json.loads(eng.lock_path.read_text())
    assert lock["reason"] == "test failure"  # first cause persisted
    eng2, _ = make_engine(tmp_path, fake_pair)
    eng2.data_dir = eng.data_dir
    eng2.lock_path = eng.lock_path
    with pytest.raises(ConfigError, match="manual intervention"):
        asyncio.run(eng2.preflight())
    events = [json.loads(x) for x in eng.journal_path.read_text().splitlines()]
    assert any(e["kind"] == "halt" for e in events)


def test_leverage_cap_blocks_open(tmp_path, fake_pair):
    from arb.models import Balance
    eng, c = make_engine(tmp_path, fake_pair, risk={"max_leverage": 0.01})
    now = time.time()
    eng.balances["lighter"] = Balance("lighter", D("5000"), D("5000"), ts=now)
    eng.compute(c, now)
    assert eng.decide(c, now) is None and "leverage cap" in c.status


def test_parallel_slices_config(tmp_path, fake_pair):
    eng, _ = make_engine(tmp_path, fake_pair, trading={"max_parallel_slices": 4})
    assert eng.cfg.max_parallel_slices == 4
