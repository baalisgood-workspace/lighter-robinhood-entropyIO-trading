"""v5.5: markouts - both venues' mids recorded after every maker fill, aggregated by `arb markout`."""
import asyncio
import json
import time
from decimal import Decimal as D

import pytest

from arb.engine import signals as sg
from arb.engine.executor import Executor
from arb.report import markout_rows

from test_engine import make_engine
from test_executor import plan


def test_executor_records_fill_with_mids(fake_pair):
    M, T = fake_pair
    M.maker_fills = [(0.05, D("0.4")), (0.1, D("0.6"))]
    seen = []
    ex = Executor({"lighter_rh": M, "lighter": T})
    ex.on_fill = lambda p, f: seen.append(f)
    r = asyncio.run(ex.run(plan()))
    assert r.maker_filled == D("1") and len(r.fills) == 2 and seen == r.fills
    f = r.fills[0]
    assert f["qty"] == pytest.approx(0.4) and f["px"] > 0 and f["m_mid"] and f["t_mid"] and f["t"] <= time.time()


def test_engine_markout_record(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c = make_engine(tmp_path, fake_pair)
    eng.cfg.markout_horizons_s = [0.05, 0.1]
    p = plan(direction=sg.SELL_MAKER)
    M.set_book("X", "99.90", "100.00")
    T.set_book("X", "100.90", "101.00")

    async def go():
        eng._on_fill(p, {"t": time.time(), "qty": 1.0, "px": 100.0, "m_mid": 99.95, "t_mid": 100.95})
        await asyncio.sleep(0.07)
        M.set_book("X", "100.90", "101.00")  # maker venue rallies after we sold: adverse
        await asyncio.gather(*list(eng._markout_tasks))
    asyncio.run(go())
    recs = [json.loads(x) for x in (tmp_path / "events.jsonl").read_text().splitlines()]
    mo = [r for r in recs if r["kind"] == "markout"][0]
    assert mo["slice"] == "open" and mo["symbol"] == "X"
    assert mo["edge0_bps"] == pytest.approx(-95.0)  # sold at 100 while the hedge venue mid was 100.95
    assert mo["mo_maker"]["0.05"] == pytest.approx(5.0) and mo["mo_maker"]["0.1"] == pytest.approx(-95.0)
    assert mo["mo_taker"]["0.1"] == pytest.approx(0.0)


def test_markout_report_aggregates(tmp_path):
    p = tmp_path / "events.jsonl"
    now = time.time()
    rows = [
        {"ts": now, "kind": "markout", "symbol": "SKHY", "slice": "open", "qty": 1, "px": 100, "edge0_bps": 10,
         "half_spread_bps": 2, "mo_maker": {"1": -1, "30": -4}, "mo_taker": {"1": -2, "30": -1}, "rth": False},
        {"ts": now, "kind": "markout", "symbol": "SKHY", "slice": "open", "qty": 3, "px": 100, "edge0_bps": 6,
         "half_spread_bps": 2, "mo_maker": {"1": 1, "30": 0}, "mo_taker": {"1": 0, "30": 1}, "rth": True},
        {"ts": now, "kind": "open", "symbol": "SKHY", "direction": "buy_maker", "t_mid_fill": 100.0, "taker_avg": 99.98,
         "hedged": 4},
        {"ts": now - 7200, "kind": "markout", "symbol": "ENA", "slice": "close", "qty": 10, "px": 1, "edge0_bps": 1,
         "mo_maker": {"1": 0}, "mo_taker": {"1": 0}},
    ]
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    out, hs = markout_rows(p)
    assert hs == [1.0, 30.0] and [r["key"] for r in out] == ["SKHY", "ENA"]
    sk = out[0]
    assert sk["n"] == 2 and sk["notional"] == 400 and sk["edge0"] == pytest.approx(7.0)
    assert sk["hedge"] == pytest.approx(2.0)  # sold the hedge 2 bp below the mid
    assert sk["mM"][30.0] == pytest.approx(-1.0) and sk["mT"][30.0] == pytest.approx(0.5)
    assert sk["net"][30.0] == pytest.approx(-1.0 - 0.5 - 2.0)
    out, _ = markout_rows(p, hours=1)
    assert [r["key"] for r in out] == ["SKHY"]
    out, _ = markout_rows(p, by="session")
    assert {r["key"] for r in out} >= {"SKHY RTH", "SKHY off"}
    out, _ = markout_rows(p, by="class")
    assert {r["key"] for r in out} == {"us_equity", "crypto"}


def test_markout_config_validation():
    from arb.config import parse_config
    sym = {"S": {"venues": {"lighter": "S", "arcus": "ENA-USD"}}}
    assert parse_config({"venues": {}, "symbols": sym}).markout_horizons_s == [1.0, 5.0, 30.0, 60.0]
    assert parse_config({"venues": {}, "symbols": sym, "markout_horizons_s": []}).markout_horizons_s == []
    with pytest.raises(Exception, match="markout"):
        parse_config({"venues": {}, "symbols": sym, "markout_horizons_s": [0]})
