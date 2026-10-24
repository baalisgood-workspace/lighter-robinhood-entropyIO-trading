"""v5.6.1: dust clean-up result from the fills (stale position reads), residual-only dust clean-up, wider
slippage on retry, threshold attribution in the journal and the advise-only threshold tuner."""
import asyncio
import json
import time
from decimal import Decimal as D

import pytest

from arb.config import ConfigError, parse_config
from arb.engine import signals as sg
from arb.engine.state import SymbolPosition
from arb import tune
from arb.models import Position

from test_engine import make_engine
from test_v56 import PosVenue, _dust, _journal, _pair


class StaleVenue(PosVenue):
    """positions() returns the snapshot taken before the last `stale` reads (exchange lagging the fills)."""

    def __init__(self, name, stale=0):
        super().__init__(name)
        self.stale = stale
        self.snap = None

    async def positions(self):
        if self.stale > 0:
            self.stale -= 1
            if self.snap is None:
                self.snap = dict(self.pos)
            return {s: Position(self.name, s, q) for s, q in self.snap.items() if q}
        return await super().positions()


def _stale_dust(tmp_path, stale):
    M, T = PosVenue("lighter_rh"), StaleVenue("lighter")
    M.set_book("X", "99.90", "100.00")
    T.set_book("X", "100.90", "101.00")
    eng, c = make_engine(tmp_path, (M, T), max_grids=1, order_value_usdc=100,
                         risk={"dust_max_usdc": 150, "dust_grace_s": 60})
    sp = eng.position(c)
    sp.add_open(sg.SHORT_MAKER, 1, D("1"), D("1"), 0.5)
    sp.residual_maker = D("1")
    M.pos["X"], T.pos["X"] = D("0"), D("1")
    T.markets["X"].min_notional = D("200")
    now = time.time()
    eng.compute(c, now)
    eng.decide(c, now)
    asyncio.run(eng.reconcile())
    c.unclosable_since -= 61
    T.stale, T.snap = stale, dict(T.pos)  # what the exchange reports until it catches up: the pre-clean-up position
    return eng, c, sp, M, T


def test_dust_result_from_fills_when_exchange_lags(tmp_path):
    # the ANTHROPIC case: a position read right after the fills still showed the old taker position
    eng, c, sp, M, T = _stale_dust(tmp_path, stale=2)
    asyncio.run(eng._fix_residuals())
    assert not T.pos.get("X") and sp.is_flat()
    j = _journal(eng, "dust_flatten")[-1]
    assert j["ok"] and j["source"] == "exchange" and j["after"] == {"lighter_rh": "0", "lighter": "0"}


def test_dust_result_computed_when_exchange_never_catches_up(tmp_path):
    eng, c, sp, M, T = _stale_dust(tmp_path, stale=99)
    asyncio.run(eng._fix_residuals())
    assert sp.is_flat()  # trusted the fills, not the stale 1.0
    j = _journal(eng, "dust_flatten")[-1]
    assert j["ok"] and j["source"] == "computed" and j["exchange_seen"]["lighter"] == "1"


def _resid(tmp_path, m_pos="-0.04", r="-0.04", **risk):
    M, T = _pair()
    eng, c = make_engine(tmp_path, (M, T), risk={"dust_max_usdc": 25, "dust_residual_grace_s": 600, **risk})
    sp = eng.position(c)
    sp.residual_maker = D(r)  # the VVV case: no grid, -0.04 left on the maker venue (4 USDC < 10 USDC minimum)
    M.pos["X"] = D(m_pos)
    M.markets["X"].min_notional = D("10")
    asyncio.run(eng.reconcile())
    return eng, c, sp, M, T


def test_residual_dust_flattened_after_grace(tmp_path):
    eng, c, sp, M, T = _resid(tmp_path)
    asyncio.run(eng._fix_residuals())
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log  # first sighting / grace period
    c.resid_since -= 601
    asyncio.run(eng._fix_residuals())
    assert [(o.is_buy, o.reduce_only) for o in M.ioc_log] == [(False, False), (True, True)]  # top-up sell, close buy
    assert not M.pos.get("X") and sp.is_flat() and not T.ioc_log
    j = _journal(eng, "dust_flatten")[-1]
    assert j["mode"] == "residual" and j["ok"]


def test_residual_dust_grace_restarts_when_it_changes(tmp_path):
    eng, c, sp, M, T = _resid(tmp_path)
    asyncio.run(eng._fix_residuals())
    c.resid_since -= 601
    sp.residual_maker = D("-0.05")
    M.pos["X"] = D("-0.05")
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log and c.resid_seen == (D("-0.05"), D("0"))


def test_residual_dust_needs_matching_exchange_position(tmp_path):
    eng, c, sp, M, T = _resid(tmp_path, m_pos="-0.03")
    asyncio.run(eng._fix_residuals())
    c.resid_since -= 601
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log and sp.residual_maker == D("-0.04")


def test_residual_dust_too_large_or_disabled(tmp_path):
    eng, c, sp, M, T = _resid(tmp_path, dust_max_usdc=2)
    asyncio.run(eng._fix_residuals())
    c.resid_since -= 601
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log and "dust_max_usdc" in c.dust_note
    eng, c, sp, M, T = _resid(tmp_path / "b", dust_flatten=False)
    asyncio.run(eng._fix_residuals())
    assert c.resid_since is None


class ThinVenue(PosVenue):
    """The first reduce-only close does not fill (thin book), later ones do."""

    def __init__(self, name):
        super().__init__(name)
        self.misses = 1

    async def place_ioc(self, symbol, is_buy, qty, limit_price, reduce_only):
        if reduce_only and self.misses > 0:
            self.misses -= 1
            return await super().place_ioc(symbol, is_buy, D(0), limit_price, reduce_only)
        return await super().place_ioc(symbol, is_buy, qty, limit_price, reduce_only)


def test_close_retried_with_wider_slippage(tmp_path):
    M, T = ThinVenue("lighter_rh"), PosVenue("lighter")
    M.set_book("X", "99.90", "100.00")
    T.set_book("X", "100.90", "101.00")
    M.reduce_only_any_notional = True
    eng, c = make_engine(tmp_path, (M, T), risk={"dust_residual_grace_s": 0})
    sp = eng.position(c)
    sp.residual_maker = M.pos["X"] = D("0.04")
    asyncio.run(eng.reconcile())
    asyncio.run(eng._fix_residuals())
    asyncio.run(eng._fix_residuals())
    o = _journal(eng, "dust_flatten")[-1]["orders"]
    assert [x["step"] for x in o] == ["close", "close"] and o[1]["slippage_pct"] == 2 * o[0]["slippage_pct"]
    assert sp.is_flat()


def test_open_and_close_journal_threshold(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.3)
    sp = eng.position(c)
    sp.add_open(sg.SHORT_MAKER, 1, D("1"), D("1"), 0.5, t1=0.25, bucket="all")
    d = SymbolPosition.from_json(json.loads(json.dumps(sp.to_json())))
    assert d.grids[0].t1 == 0.25 and d.grids[0].bucket == "all"
    sp.add_open(sg.SHORT_MAKER, 1, D("1"), D("2"), 0.5, t1=0.30, bucket="all")
    assert sp.grids[0].t1 == 0.25  # first open of the grid wins


def _cfg(tmp_path, **at):
    return parse_config({"venues": {"lighter": {"enabled": True}, "lighter_rh": {"enabled": True}},
                         "paths": {"data_dir": str(tmp_path)}, "autotune": at,
                         "defaults": {"strategy": {"initial_threshold_pct": 0.12, "grid_step_pct": 0.04}},
                         "symbols": {"LIT": {"venues": {"lighter": "LIT", "lighter_rh": "LIT"}},
                                     "MU": {"venues": {"lighter": "MU", "lighter_rh": "MU"},
                                            "strategy": {"session": "us_equity", "initial_threshold_pct": 0.15,
                                                         "offhours_threshold_pct": 0.25}}}})


def _write(path, recs):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")


def _closes(sym, n, bps, t1=0.12, spread=1.0, fee=0.0, old=False, bucket="all", ts=None):
    now = time.time() if ts is None else ts
    out = []
    for i in range(n):
        b = bps + (spread if i % 2 else -spread)
        r = {"ts": now - 3600 + i, "kind": "close", "symbol": sym, "level": 1, "hedged": 1, "taker_avg": 100,
             "gross_pnl_usdc": b / 1e4 * 100, "required": -t1 * 0.5}
        if not old:
            r.update(open_t1=t1, open_bucket=bucket, fee_pct=fee)
        out.append(r)
    return out


def test_tuner_lower_raise_hold(tmp_path):
    cfg = _cfg(tmp_path)
    ev = tmp_path / "ev.jsonl"
    _write(ev, _closes("LIT", 40, 5.0))
    r = {x["symbol"]: x for x in tune.advise(cfg, ev)}["LIT"]
    assert r["action"] == "lower" and abs(r["target"] - 0.11) < 1e-9 and r["n"] == 40
    _write(ev, _closes("LIT", 40, 5.0, fee=0.06))  # 5 bps gross - 6 bps fees = losing
    r = {x["symbol"]: x for x in tune.advise(cfg, ev)}["LIT"]
    assert r["action"] == "raise" and abs(r["target"] - 0.13) < 1e-9 and r["mean_bps"] < 0
    _write(ev, _closes("LIT", 10, 5.0))
    r = {x["symbol"]: x for x in tune.advise(cfg, ev)}["LIT"]
    assert r["action"] == "hold" and "insufficient" in r["why"]
    _write(ev, _closes("LIT", 40, 0.5, spread=3))
    assert {x["symbol"]: x for x in tune.advise(cfg, ev)}["LIT"]["action"] == "hold"


def test_tuner_close_only_at_max_and_bounds(tmp_path):
    cfg = _cfg(tmp_path, bounds={"LIT": {"min": 0.05, "max": 0.12}})
    ev = tmp_path / "ev.jsonl"
    _write(ev, _closes("LIT", 40, -5.0))
    r = {x["symbol"]: x for x in tune.advise(cfg, ev)}["LIT"]
    assert r["action"] == "close_only" and (r["min"], r["max"]) == (0.05, 0.12)
    cfg = _cfg(tmp_path, bounds={"LIT": {"min": 0.15, "max": 0.3}})
    r = {x["symbol"]: x for x in tune.advise(cfg, ev)}["LIT"]
    assert r["action"] == "raise" and r["target"] == 0.15
    assert tune.bounds(_cfg(tmp_path), _cfg(tmp_path).symbols["LIT"], "all") == (0.08, 0.25)  # crypto default


def test_tuner_other_thresholds_and_sessions(tmp_path):
    cfg = _cfg(tmp_path)
    ev = tmp_path / "ev.jsonl"
    # old closes (no open_t1): T1 inferred from the close trigger; profitable at 0.10 with too few closes at 0.12
    _write(ev, _closes("LIT", 40, 6.0, t1=0.10, old=True) + _closes("LIT", 3, 6.0)
           + _closes("MU", 35, -8.0, t1=0.25, bucket="off") + _closes("MU", 35, 8.0, t1=0.15, bucket="rth"))
    rows = {(x["symbol"], x["bucket"]): x for x in tune.advise(cfg, ev)}
    lit = rows[("LIT", "all")]
    assert lit["action"] == "lower" and lit["inferred"] == 40 and lit["n"] == 3 and len(lit["history"]) == 2
    assert rows[("MU", "off")]["action"] == "raise" and rows[("MU", "rth")]["action"] == "lower"
    assert rows[("MU", "off")]["min"] == 0.20 and rows[("MU", "off")]["max"] == 0.40  # MU default bounds
    assert "off-hours" in tune.advice_text(rows[("MU", "off")])


def test_tuner_window(tmp_path):
    cfg = _cfg(tmp_path)
    ev = tmp_path / "ev.jsonl"
    _write(ev, _closes("LIT", 40, 5.0, ts=time.time() - 10 * 86400))
    assert {x["symbol"]: x for x in tune.advise(cfg, ev)}["LIT"]["n_all"] == 0


def test_autotune_config_validation(tmp_path):
    with pytest.raises(ConfigError, match="advise"):
        _cfg(tmp_path, mode="auto")
    with pytest.raises(ConfigError, match="bounds.LIT"):
        _cfg(tmp_path, bounds={"LIT": {"min": 0.3, "max": 0.1}})
    with pytest.raises(ConfigError, match="unknown keys"):
        _cfg(tmp_path, foo=1)
    assert _cfg(tmp_path).autotune.enabled and _cfg(tmp_path).autotune.mode == "advise"


def test_engine_tune_once_journals_advice(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, initial_threshold_pct=0.3)
    _write(eng.journal_path, _closes("X", 40, 5.0, t1=0.3))
    rows = asyncio.run(eng.tune_once())
    assert rows[0]["action"] == "lower"
    j = _journal(eng, "tune_advice")
    assert j and j[-1]["rows"][0]["symbol"] == "X" and j[-1]["mode"] == "advise"
    assert any("tune advice X: lower T1 0.3% -> 0.29%" in e for e in eng.events)
    n = len(eng.events)
    asyncio.run(eng.tune_once())
    assert len(eng.events) == n  # unchanged advice is journaled, not logged again
    assert c.sym.strategy.initial_threshold_pct == 0.3  # advise only


def test_tune_cli(tmp_path, capsys):
    import yaml
    from arb import cli
    cfgp = tmp_path / "c.yaml"
    cfgp.write_text(yaml.safe_dump({"venues": {"lighter": {"enabled": True}, "lighter_rh": {"enabled": True}},
                                    "paths": {"data_dir": str(tmp_path / "data")},
                                    "auto_symbols": {"enabled": False},
                                    "symbols": {"LIT": {"venues": {"lighter": "LIT", "lighter_rh": "LIT"},
                                                        "strategy": {"initial_threshold_pct": 0.12}}}}))
    ev = tmp_path / "data" / "lighter+lighter_rh" / "events.jsonl"
    _write(ev, _closes("LIT", 40, 5.0) + [{"ts": time.time(), "kind": "tune_advice", "rows": [
        {"symbol": "LIT", "bucket": "all", "t1": 0.12, "action": "lower", "target": 0.11, "why": "x"}]}])
    assert cli.main(["tune", "--config", str(cfgp)]) == 0
    out = capsys.readouterr().out
    assert "LIT" in out and "lower -> 0.11" in out
    assert cli.main(["tune", "--config", str(cfgp), "--log"]) == 0
    assert "LIT: lower T1 0.12% -> 0.11%" in capsys.readouterr().out
