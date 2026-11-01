"""v5.7.4: raw_edge uses one base in both directions, so a round trip at an unchanged spread earns nothing."""
import time
from decimal import Decimal as D

import pytest

from arb import tune
from arb.engine import signals as sg
from arb.engine.state import SymbolPosition

from test_v561 import _cfg, _closes, _write


@pytest.mark.parametrize("m,t", [("1373.7", "1428.67"), ("1800", "1745")])
def test_round_trip_at_same_prices_has_no_edge(m, t):
    m, t = D(m), D(t)
    for first, second in ((sg.BUY_MAKER, sg.SELL_MAKER), (sg.SELL_MAKER, sg.BUY_MAKER)):
        assert sg.raw_edge(first, m, t) + sg.raw_edge(second, m, t) == pytest.approx(0.0)


def test_real_openai_round_trip_loses():
    # 10-31 fills: buy maker 1373.7 / sell taker 1428.67, then sell maker 1373.8 / buy taker 1429.13
    qty, px = 0.014, 1429.13
    true = qty * ((1373.8 - 1373.7) + (1428.67 - 1429.13))
    pct = sg.raw_edge(sg.BUY_MAKER, D("1373.7"), D("1428.67")) + sg.raw_edge(sg.SELL_MAKER, D("1373.8"), D("1429.13"))
    assert true < 0 and pct / 100 * qty * px == pytest.approx(true, abs=1e-3)


def test_old_saved_total_is_dropped():
    sp = SymbolPosition(pair_key="p", symbol="X", realized_edge_usdc=10.11)
    assert SymbolPosition.from_json(sp.to_json()).realized_edge_usdc == 10.11
    old = sp.to_json()
    del old["edge_v"]
    assert SymbolPosition.from_json(old).realized_edge_usdc == 0.0


def test_tune_ignores_old_journal_rows(tmp_path):
    cfg = _cfg(tmp_path)
    old = _closes("LIT", 40, 5.0)
    for r in old:
        del r["edge_v"]
    new = _closes("LIT", 3, 5.0)
    p = tmp_path / "events.jsonl"
    _write(p, old + new)
    assert len(tune.load_closes(cfg, p, time.time() - 86400)) == 3
