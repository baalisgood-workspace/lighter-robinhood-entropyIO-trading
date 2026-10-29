"""v5.7.1: `arb points` - own fills that can earn Lighter RH points (not standard-to-standard) + points snapshots."""
import asyncio
import time

import pytest

from arb import points as pts

ME = 42288


def _t(ask, bid, maker_ask, usd, mf=0, tf=0, sym="ANTHROPIC", ts=None, **kw):
    t = {"type": "trade", "ask_account_id": ask, "bid_account_id": bid, "is_maker_ask": maker_ask, "usd_amount": str(usd),
         "market_symbol": sym, "market_id": 7, "timestamp": int((ts or time.time()) * 1000)}
    if mf:
        t["maker_fee"] = mf  # the API leaves zero fees out
    if tf:
        t["taker_fee"] = tf
    t.update(kw)
    return t


def test_classify_roles_and_reward():
    # we rest the ask (maker), a premium taker pays 350 -> rewarded
    c = pts.classify(_t(ME, 1, True, 100, tf=350), ME)
    assert c["role"] == "maker" and c["their_fee"] == 350 and c["my_fee"] == 0 and c["rewarded"]
    # standard taker hits our standard bid -> standard-to-standard, nothing
    c = pts.classify(_t(1, ME, False, 50), ME)
    assert c["role"] == "maker" and not c["rewarded"]
    # we take (IOC) against a premium maker (fee 102) -> rewarded
    c = pts.classify(_t(ME, 2, False, 80, mf=102), ME)
    assert c["role"] == "taker" and c["their_fee"] == 102 and c["rewarded"]
    # a rebated maker (negative fee) is a fee-paying tier too
    assert pts.classify(_t(ME, 3, False, 80, mf=-20), ME)["rewarded"]
    # we are premium ourselves -> always rewarded
    assert pts.classify(_t(1, ME, True, 10, tf=350), ME)["rewarded"]
    assert pts.classify(_t(ME, ME, True, 10, mf=1, tf=1), ME)["role"] == "self"
    assert not pts.classify(_t(ME, ME, True, 10, mf=1, tf=1), ME)["rewarded"]
    assert pts.classify(_t(1, 2, True, 10), ME) is None
    assert pts.classify(_t(ME, 1, True, 10, type="liquidation"), ME) is None


def test_summarize_and_symbol_fallback():
    trades = [_t(ME, 1, True, 100, tf=350), _t(1, ME, False, 300), _t(ME, 2, False, 100, mf=102, sym=None)]
    rows = pts.summarize(trades, ME, {7: "OPENAI"})
    assert rows["TOTAL"]["usd"] == 500 and rows["TOTAL"]["rew_usd"] == 200
    assert rows["TOTAL"]["maker_usd"] == 400 and rows["TOTAL"]["maker_rew_usd"] == 100
    assert rows["OPENAI"]["rew_usd"] == 100  # market_symbol missing -> market id map
    text = pts.render("lighter_rh", ME, 7, rows, 1234.5, None, None)
    assert "TOTAL" in text and "40%" in text and "1,234.50" in text and "25%" in text


def test_snapshots_points_per_volume(tmp_path):
    p = tmp_path / pts.HISTORY_FILE
    assert pts.last_snapshot(p, "lighter_rh") is None
    pts.append_snapshot(p, "lighter_rh", ME, 1000.0, time.time() - 7200)
    pts.append_snapshot(p, "lighter", 5, None, time.time())
    prev = pts.last_snapshot(p, "lighter_rh")
    assert prev["points"] == 1000.0
    text = pts.render("lighter_rh", ME, 7, {}, 1100.0, prev, 20000.0)
    assert "+100.00 since the snapshot 2.0h ago" in text and "50.000 points per 10k USDC" in text
    assert "not available" in pts.render("lighter", 5, 7, {}, None, None, None)


class _FakeLighter:
    def __init__(self, cfg):
        self.name, self.account_index, self._symbol_by_mid = cfg.name, str(ME), {7: "ANTHROPIC"}

    async def start(self, symbols, trading):
        assert trading is False  # read-only

    async def stop(self): ...

    async def my_trades(self, since_ts, max_pages=200):
        now = time.time()
        return [_t(ME, 1, True, 100, tf=350, ts=now - 60), _t(1, ME, False, 300, ts=now - 3 * 86400),
                _t(ME, 1, True, 999, ts=now - 20 * 86400)]  # last one before the window

    async def live_points(self):
        return 50.0 if self.name == "lighter_rh" else None


def test_points_command(tmp_path, monkeypatch, capsys):
    from arb import cli
    from arb.config import parse_config
    import arb.venues as venues
    cfg = parse_config({"venues": {"lighter": {"enabled": True}, "lighter_rh": {"enabled": True}},
                        "paths": {"data_dir": str(tmp_path)},
                        "symbols": {"ANTHROPIC": {"venues": {"lighter": "ANTHROPIC", "lighter_rh": "ANTHROPIC"}}}})
    monkeypatch.setattr(venues, "build_venue", lambda vc: _FakeLighter(vc))
    monkeypatch.setattr(cli, "_creds_present", lambda cfg, n: True)
    assert asyncio.run(cli._points(cfg, 7, True)) == 0
    out = capsys.readouterr().out
    assert "lighter_rh account 42288" in out and "live points total: 50.00" in out and "not available" in out
    assert out.count("TOTAL") == 2 and "400" in out  # 20-day-old fill outside the 7-day window
    hist = tmp_path / "lighter+lighter_rh" / pts.HISTORY_FILE
    assert len(hist.read_text().splitlines()) == 1  # only the venue with points
    assert asyncio.run(cli._points(cfg, 7, False)) == 0
    assert "since the snapshot" in capsys.readouterr().out
    assert len(hist.read_text().splitlines()) == 1
