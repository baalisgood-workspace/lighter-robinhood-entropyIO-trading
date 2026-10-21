"""v5: auto symbols, trade policies / liquidity filter / rank, taker->taker open, CLI mode overrides, dashboard sort."""
import asyncio
import json
import time
import types
from decimal import Decimal as D

import pytest

from arb.autosym import classify, expand
from arb.config import ConfigError, parse_config
from arb.engine import signals as sg
from arb.engine.engine import SAMPLE_COLS, Engine
from arb.engine.executor import Executor
from arb.models import Balance
from arb.report import rank

LISTING = {"lighter": {"AAPL": "AAPL", "BTC": "BTC", "OPENAI": "OPENAI", "XAU": "XAU", "ONLY": "ONLY", "SNDK": "SNDK", "ZZZ": "ZZZ"},
           "arcus": {"AAPL": "AAPL-USD", "BTC": "BTC-USD", "XAU": "XAU-USD", "SNDK": "SNDK-USD", "ZZZ": "ZZZ-USD"},
           "entropy": {"OPENAI": "io:OAI", "SNDK": "io:SNDK"}}
BASE = {"venues": {"lighter": {"enabled": True}, "arcus": {"enabled": True}},
        "symbols": {"SNDK": {"venues": {"lighter": "SNDK", "arcus": "SNDK-USD"}}}}


# ------------------------------------------------------------------ classification / expansion
def test_classify():
    assert classify("AAPL") == "us_equity" and classify("BTC") == "crypto" and classify("OPENAI") == "other"
    assert classify("XAU") == "other"  # metal CFD, not a US session
    assert classify("NEWCO", "EQUITIES") == "us_equity" and classify("NEWCOIN", "CRYPTO") == "crypto"
    assert classify("NEWCO") == "other"
    assert classify("BTC", extra={"other": ["btc"]}) == "other"


def test_expand_adds_templated_symbols_and_keeps_configured():
    raw, added = expand(BASE, LISTING, {"ZZZ": "EQUITIES"})
    s = raw["symbols"]
    assert s["SNDK"] == BASE["symbols"]["SNDK"]  # configured untouched (entropy not added to it)
    assert set(added) == {"AAPL", "BTC", "OPENAI", "XAU", "ZZZ"} and "ONLY" not in s
    assert s["AAPL"]["venues"] == {"lighter": "AAPL", "arcus": "AAPL-USD"} and s["AAPL"]["auto"]
    assert s["AAPL"]["strategy"]["session"] == "us_equity" and s["AAPL"]["strategy"]["offhours_threshold_pct"] == 0.60
    assert s["OPENAI"]["venues"] == {"lighter": "OPENAI", "entropy": "io:OAI"} and s["OPENAI"]["category"] == "other"
    assert s["ZZZ"]["category"] == "us_equity"
    cfg = parse_config(raw)
    assert cfg.symbols["AAPL"].auto and not cfg.symbols["SNDK"].auto
    assert cfg.symbols["BTC"].strategy.initial_threshold_pct == 0.20


def test_expand_filters():
    raw = {**BASE, "auto_symbols": {"exclude": ["xau"], "include": ["ZZZ"], "max_symbols": 2,
                                    "templates": {"crypto": {"strategy": {"initial_threshold_pct": 0.5}}}}}
    out, added = expand(raw, LISTING)
    assert added == ["ZZZ", "AAPL"]  # include first, then alphabetical, capped
    out, added = expand({**BASE, "auto_symbols": {"templates": {"crypto": {"strategy": {"initial_threshold_pct": 0.5}}}}},
                        LISTING, only_venues=["lighter", "arcus"])
    assert "OPENAI" not in added and out["symbols"]["BTC"]["strategy"]["initial_threshold_pct"] == 0.5
    out, _ = expand({"symbols": "auto", "venues": BASE["venues"]}, LISTING)
    assert "SNDK" in out["symbols"] and out["symbols"]["SNDK"]["auto"]


def test_auto_config_validation():
    with pytest.raises(ConfigError):
        parse_config({**BASE, "auto_symbols": {"trade": "everything"}})
    with pytest.raises(ConfigError):
        parse_config({**BASE, "ui": {"sort": "random"}})
    with pytest.raises(ConfigError):
        parse_config({**BASE, "defaults": {"execution": {"open_mode": "x"}}})


def test_cli_mode_overrides():
    from arb.cli import _apply_cli_modes
    cfg = parse_config(BASE)
    assert cfg.active_pair().maker == "arcus"
    _apply_cli_modes(cfg, types.SimpleNamespace(maker="lighter", open_mode="taker_taker", close_mode="taker_taker"))
    assert cfg.active_pair().maker == "lighter"
    assert cfg.symbols["SNDK"].execution.open_mode == "taker_taker" == cfg.symbols["SNDK"].execution.close_mode
    with pytest.raises(ConfigError):
        _apply_cli_modes(cfg, types.SimpleNamespace(maker="entropy", open_mode=None, close_mode=None))


def test_fees_open_taker_taker():
    assert sg.roundtrip_fee_pct(0.0, 0.02, False, 0.05) == pytest.approx(0.04)
    assert sg.roundtrip_fee_pct(0.0, 0.02, False, 0.05, open_taker_taker=True) == pytest.approx(0.09)
    assert sg.roundtrip_fee_pct(0.0, 0.02, True, 0.05, open_taker_taker=True) == pytest.approx(0.14)


# ------------------------------------------------------------------ engine helpers
def engine(tmp_path, fake_pair, sym_extra=None, auto_symbols=None, execution=None, strategy=None):
    M, T = fake_pair
    raw = {"venues": {"lighter": {"enabled": True}, "lighter_rh": {"enabled": True}},
           "market_data": {"watch_all_venues": False}, "paths": {"data_dir": str(tmp_path)},
           "auto_symbols": auto_symbols or {},
           "defaults": {"basis": {"enabled": False}, "risk": {"warmup_fresh_s": 0}, "execution": execution or {},
                        "strategy": strategy or {}},
           "symbols": {"X": {"venues": {"lighter": "X", "lighter_rh": "X"}, **(sym_extra or {})}}}
    eng = Engine(parse_config(raw), {"lighter_rh": M, "lighter": T}, live=True)
    eng.build_contexts()
    now = time.time()
    for n in ("lighter", "lighter_rh"):
        eng.balances[n] = Balance(n, D("5000"), D("5000"), ts=now)
    return eng, eng.ctxs[0]


def test_open_taker_taker_executes_iocs_on_both(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c = engine(tmp_path, fake_pair, execution={"open_mode": "taker_taker"})
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    assert p is not None and p.kind == "open" and p.taker_taker and p.direction == sg.SELL_MAKER
    res = asyncio.run(Executor({"lighter_rh": M, "lighter": T}).run(p))
    assert len(M.ioc_log) == 1 and len(T.ioc_log) >= 1 and not M.placed  # no resting order
    assert res.hedged == p.qty and M.ioc_log[0].reduce_only is False


def test_open_taker_taker_needs_edge_on_immediate_prices(tmp_path, fake_pair):
    M, T = fake_pair
    M.set_book("X", "100.20", "101.00")  # maker-venue bid only 0.2% above the taker ask
    eng, c = engine(tmp_path, fake_pair, execution={"open_mode": "taker_taker"})
    now = time.time()
    eng.compute(c, now)
    assert eng.decide(c, now) is None
    eng2, c2 = engine(tmp_path / "b", fake_pair)  # maker mode would still quote at the ask side
    eng2.compute(c2, now)
    assert eng2.decide(c2, now) is not None


def test_auto_symbol_monitor_only_and_filtered(tmp_path, fake_pair):
    eng, c = engine(tmp_path, fake_pair, sym_extra={"auto": True, "category": "crypto"})
    now = time.time()
    assert c.policy == "monitor"
    eng.compute(c, now)
    assert eng.decide(c, now) is None and "仅监控" in c.status
    eng, c = engine(tmp_path / "f", fake_pair, sym_extra={"auto": True}, auto_symbols={"trade": "filtered", "min_depth_usdc": 100})
    eng.compute(c, now)
    assert eng.decide(c, now) is None and "流动性" in c.status  # filter still warming up
    c.liq_since -= 60
    c.liq_spread = 0.1  # 99.9/100 book: 0.1% < 0.15
    eng.compute(c, now)
    assert eng.decide(c, now) is not None
    c.liq_spread = 0.5
    assert "盘口点差" in eng.guards.open_block(c, now) or "book spread" in eng.guards.open_block(c, now)


def test_include_whitelist_trades(tmp_path, fake_pair):
    eng, c = engine(tmp_path, fake_pair, sym_extra={"auto": True}, auto_symbols={"include": ["x"]})
    assert c.policy == "trade"


def test_ranked_policy_reads_rank_file(tmp_path, fake_pair):
    (tmp_path / "rank.json").write_text(json.dumps({"ts": time.time(), "rows": [
        {"pair": "lighter+lighter_rh", "maker": "lighter_rh", "symbol": "X", "score": 1.2}]}))
    eng, c = engine(tmp_path, fake_pair, sym_extra={"auto": True}, auto_symbols={"trade": "ranked"})
    assert c.policy == "filter" and c.policy_why == "ranked"
    (tmp_path / "rank.json").write_text(json.dumps({"ts": time.time(), "rows": [
        {"pair": "lighter+lighter_rh", "maker": "lighter", "symbol": "X", "score": 1.2}]}))  # other orientation
    eng, c = engine(tmp_path, fake_pair, sym_extra={"auto": True}, auto_symbols={"trade": "ranked"})
    assert c.policy == "monitor"


def test_offhours_open_false(tmp_path, fake_pair, monkeypatch):
    import arb.engine.guards as gm
    eng, c = engine(tmp_path, fake_pair, strategy={"session": "us_equity", "offhours_open": False})
    monkeypatch.setattr(gm, "us_regular_hours", lambda ts=None: False)
    assert eng.guards.open_block(c, time.time()) == "美股盘外不开仓"
    monkeypatch.setattr(gm, "us_regular_hours", lambda ts=None: True)
    assert eng.guards.open_block(c, time.time()) == ""


def test_preview_need_and_sample_row(tmp_path, fake_pair):
    eng, c = engine(tmp_path, fake_pair)
    eng.live = False
    eng.compute(c, time.time())
    assert c.need_sell == pytest.approx(0.30) and c.gap is not None and c.gap < 0
    row = eng.sample_row(c)
    assert len(row) == len(SAMPLE_COLS)
    d = dict(zip(SAMPLE_COLS, row))
    assert d["open_mode"] == "maker_taker" and float(d["tt_dev_sell"]) < float(d["dev_sell_maker"]) + 1e-9


# ------------------------------------------------------------------ rank
def _rows(sym, devs, start=1_000_000.0, step=5.0, **extra):
    out = []
    for i, d in enumerate(devs):
        out.append({"ts": str(start + i * step), "pair": "lighter+arcus", "maker": "arcus", "taker": "lighter", "symbol": sym,
                    "basis_ready": "1", "dev_sell_maker": str(d), "dev_buy_maker": "-1", "tt_dev_sell": str(d - 0.2),
                    "tt_dev_buy": "-1", "t1": "0.30", "fee_mt": "0.05", "fee_tt": "0.1", "fund_sell": "0", "fund_buy": "0",
                    "book_spread_pct": "0.05", "depth_usdc": "1000", "rth": "1", "category": "us_equity", **extra})
    return out


def test_rank_counts_episodes_with_hysteresis():
    base = [0.0] * 200
    devs = list(base)
    for i in (50, 51, 52, 100, 150):  # 3 episodes (50-52 is one); 0.34 vs need 0.35 within hysteresis keeps it open
        devs[i] = 0.40
    devs[53] = 0.34
    devs[54] = 0.40
    rows = _rows("A", devs * 3) + _rows("B", [0.0] * 600) + _rows("C", [0.5] * 10)
    res = {r["symbol"]: r for r in rank(rows)}
    a = res["A"]
    # 3 x 200 samples x 5 s ~ 50 min < 1h minimum coverage -> score 0 with a note
    assert a["episodes"] == 9 and a["score"] == 0 and "data" in a["note"]
    rows = _rows("A", devs * 6)
    a = rank(rows)[0]
    assert a["episodes"] == 18 and a["score"] > 0 and a["avg_peak_excess"] == pytest.approx(0.05)
    assert rank(rows, "taker_taker")[0]["episodes"] == 0  # tt edge 0.2 lower, need 0.4
    cfg = types.SimpleNamespace(auto_symbols=types.SimpleNamespace(max_book_spread_pct=0.01, min_depth_usdc=300),
                                symbols={}, venues={})
    assert rank(rows, cfg=cfg)[0]["score"] == 0  # spread above the filter


def test_rank_cli_save(tmp_path):
    import csv
    from arb.report import print_rank
    d = tmp_path / "monitor" / "spread_samples"
    d.mkdir(parents=True)
    rows = _rows("A", ([0.0] * 50 + [0.4] * 3) * 20)
    with (d / "2026-10-21_v5.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    assert print_rank(tmp_path, "maker_taker", ["2026-10-21"], None, 10, True) == 0
    data = json.loads((tmp_path / "rank.json").read_text())
    assert data["rows"][0]["symbol"] == "A" and data["rows"][0]["episodes"] == 20


# ------------------------------------------------------------------ dashboard
def test_dashboard_sorts_and_folds(tmp_path):
    from rich.console import Console
    from conftest import FakeVenue
    from arb.ui import _ordered_ctxs, build_view
    syms = [f"S{i}" for i in range(30)]
    M, T = FakeVenue("lighter_rh", syms), FakeVenue("lighter", syms)
    for i, s in enumerate(syms):
        M.set_book(s, f"{100.0 + i * 0.03:.2f}", f"{100.1 + i * 0.03:.2f}")
        T.set_book(s, "99.90", "100.00")
    raw = {"venues": {"lighter": {"enabled": True}, "lighter_rh": {"enabled": True}}, "paths": {"data_dir": str(tmp_path)},
           "market_data": {"watch_all_venues": False},
           "defaults": {"basis": {"enabled": False}, "risk": {"warmup_fresh_s": 0}},
           "symbols": {s: {"venues": {"lighter": s, "lighter_rh": s}, "auto": True} for s in syms}}
    eng = Engine(parse_config(raw), {"lighter_rh": M, "lighter": T}, live=True)
    eng.build_contexts()
    for c in eng.ctxs:
        eng.compute(c, time.time())
    order = [c.sym.key for c in _ordered_ctxs(eng)]
    assert order[0] == "S29" and order[-1] in ("S0", "S1", "S2")  # biggest edge first
    con = Console(record=True, width=220, height=60, force_terminal=True)
    con.print(build_view(eng, 60))
    out = con.export_text()
    assert "行未显示" in out and "·自动" in out and "v5" in out and "可开仓 0/30" in out and "仅监控" in out


def test_monitor_only_not_reconciled_and_foreign_position_skipped(tmp_path, fake_pair):
    from arb.models import Position
    M, T = fake_pair
    eng, c = engine(tmp_path, fake_pair, sym_extra={"auto": True})
    assert c.policy == "monitor" and eng.active_ctxs() == []
    eng, c = engine(tmp_path / "f", fake_pair, sym_extra={"auto": True}, auto_symbols={"trade": "filtered"})
    assert eng.active_ctxs() == [c]

    async def pos():
        return {"X": Position("lighter", "X", D("1"), D("100"))}
    T.positions = pos
    assert asyncio.run(eng.reconcile(initial=True)) is True  # no startup mismatch error
    assert "not opened by this bot" in c.claim_block and eng.active_ctxs() == []
