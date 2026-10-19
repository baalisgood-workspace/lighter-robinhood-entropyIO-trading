import asyncio
from decimal import Decimal as D

from arb.config import ExecutionConfig, Pair, SymbolConfig
from arb.engine import signals as sg
from arb.engine.executor import ExecPlan, Executor


def plan(qty="1", required=0.3, kind="open", direction=sg.SELL_MAKER, **ex):
    e = dict(maker_timeout_s=0.6, order_poll_interval_s=0.05, order_final_timeout_s=0.3, requote_min_interval_s=100)
    e.update(ex)
    sym = SymbolConfig(key="X", venues={"lighter": "X", "lighter_rh": "X"}, execution=ExecutionConfig(**e))
    return ExecPlan(kind, Pair("lighter", "lighter_rh", "lighter_rh"), sym, "X", "X", direction, D(qty), 1, required, lambda: 0.0)


def run(M, T, p):
    return asyncio.run(Executor({"lighter_rh": M, "lighter": T}).run(p))


def test_full_fill_and_hedge(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("1"))]
    r = run(M, T, plan())
    assert r.maker_filled == D("1") and r.taker_filled == D("1") and r.hedged == D("1")
    assert not r.halted and T.ioc_log[0].is_buy  # maker sells -> taker buys
    assert r.realized_dev(sg.SELL_MAKER, 0.0) > 0.5


def test_partial_fill_then_timeout(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("0.4"))]
    r = run(M, T, plan())
    assert r.maker_filled == D("0.4") and r.taker_filled == D("0.4")
    assert r.note == "timeout" and M.canceled and not r.halted


def test_incremental_hedging(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("0.3")), (0.1, D("0.3")), (0.1, D("0.4"))]
    r = run(M, T, plan())
    assert r.hedged == D("1") and len(T.ioc_log) >= 2


def test_no_edge_no_order(fake_pair):
    M, T = fake_pair
    r = run(M, T, plan(required=2.0))
    assert r.orders_placed == 0 and r.note == "edge gone"


def test_hedge_failure_emergency_unwind(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("1"))]
    T.ioc_ratio = D("0")
    r = run(M, T, plan(emergency_unwind=True))
    assert r.taker_filled == 0 and r.maker_unwound == D("1") and not r.halted
    assert M.ioc_log and M.ioc_log[0].is_buy and M.ioc_log[0].reduce_only  # buy back the short, reduce-only


def test_hedge_and_unwind_failure_halts(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("1"))]
    T.ioc_ratio = D("0")
    M.ioc_ratio = D("0")
    r = run(M, T, plan(emergency_unwind=True))
    assert r.halted and "unhedged" in r.error


def test_taker_taker_close(fake_pair):
    M, T = fake_pair
    p = plan(kind="close", direction=sg.BUY_MAKER)
    p.taker_taker = True
    r = run(M, T, p)
    assert r.maker_filled == D("1") and r.taker_filled == D("1")
    assert M.ioc_log[0].reduce_only and T.ioc_log[0].reduce_only and not T.ioc_log[0].is_buy
