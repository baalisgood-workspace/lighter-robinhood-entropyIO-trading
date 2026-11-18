"""v5.9 read-only reference monitor: symbol mapping, snapshot parsing, recording and ranking."""
import gzip
import math
import time
from decimal import Decimal as D

import pytest

from arb import refmon as rm
from arb.config import ConfigError


@pytest.mark.parametrize("venue,name,base", [
    ("binance", "BTCUSDT", "BTC"), ("binance", "NVDAUSDT", "NVDA"), ("binance", "BTCUSDC", None),
    ("aster", "1000PEPEUSDT", "1000PEPE"), ("bybit", "ANTHROPICUSDT", "ANTHROPIC"), ("bybit", "PAXGPERP", None),
    ("okx", "BTC-USDT-SWAP", "BTC"), ("okx", "BTC-USD-SWAP", None), ("ondo", "AAPL-USD.P", "AAPL"),
    ("hyperliquid", "kPEPE", "1000PEPE"), ("hyperliquid", "BTC", "BTC"), ("entropy", "io:OAI", "OPENAI"),
    ("arcus", "SNDK-USD", "SNDK"),
])
def test_normalize(venue, name, base):
    assert rm.normalize(venue, name) == base


def test_parsers():
    q = rm.parse_binance_like([{"symbol": "BTCUSDT", "bidPrice": "100", "bidQty": "2", "askPrice": "100.1", "askQty": "3"},
                               {"symbol": "XUSDT", "bidPrice": "0", "askPrice": "1"}],
                              {"BTCUSDT": "BTC", "XUSDT": "X"}, {"BTCUSDT": (0.0001, 4.0)})
    assert set(q) == {"BTC"} and q["BTC"].bid == 100 and q["BTC"].funding_h == 4.0
    q = rm.parse_okx([{"instId": "BTC-USDT-SWAP", "bidPx": "100", "bidSz": "50", "askPx": "101", "askSz": "10"}],
                     {"BTC-USDT-SWAP": "BTC"}, {"BTC-USDT-SWAP": 0.01})
    assert q["BTC"].bid_qty == pytest.approx(0.5) and q["BTC"].ask_qty == pytest.approx(0.1)
    q = rm.parse_bybit([{"symbol": "NVDAUSDT", "bid1Price": "180", "bid1Size": "1", "ask1Price": "180.1", "ask1Size": "2",
                         "fundingRate": "0.0001", "fundingIntervalHour": "8"}], {"NVDAUSDT": "NVDA"})
    assert q["NVDA"].funding_h == 8
    q = rm.parse_hyperliquid([{"universe": [{"name": "BTC"}, {"name": "OLD", "isDelisted": True}, {"name": "kPEPE"}]},
                              [{"impactPxs": ["99", "101"], "funding": "0.00001"}, {"midPx": "5"}, {"midPx": "0.01"}]])
    assert q["BTC"].bid == 99 and q["BTC"].ask == 101 and "OLD" not in q and q["1000PEPE"].bid == 0.01
    q = rm.parse_ondo([{"market": "AAPL-USD.P", "bid": "227.4", "ask": "227.6"}, {"market": "X-USD.P", "disabled": True,
                       "bid": "1", "ask": "2"}])
    assert set(q) == {"AAPL"}


def test_config(tmp_path):
    p = tmp_path / "r.yaml"
    p.write_text("ref_venues: [binance]\nfees: {binance: {taker: 0.04}}\n")
    cfg = rm.load_refmon_config(str(p))
    assert cfg.fee("binance") == (0.02, 0.04) and cfg.fee("lighter") == (0.0, 0.0)
    p.write_text("ref_venues: [kraken]\n")
    with pytest.raises(ConfigError):
        rm.load_refmon_config(str(p))
    assert rm.load_refmon_config("config/refmon.yaml").fee("lighter_rh") == (0.012, 0.035)


def test_small_rows_depth(fake_pair):
    M, _ = fake_pair  # lighter_rh book 100.90 / 101.00, size 100
    rows = rm.small_rows(time.time(), M, {"XX": "X"}, 0.1)
    assert len(rows) == 1 and rows[0][2] == "XX" and rows[0][3] == 100.9 and rows[0][7] == round(100.9 * 100)


def test_writer_roundtrip_and_rank(tmp_path):
    w = rm.Writer(tmp_path)
    t0 = 1793923200.0 + 3600  # 01:00 UTC: all samples on one UTC day
    rows = []
    for i in range(3000):  # 5s samples, ~4h: small venue oscillates 0.15% around a +0.05% premium
        t = t0 + 5 * i
        ref_mid = 100.0
        dev = 0.15 * math.sin(i / 20)
        mid = ref_mid * (1 + (0.05 + dev) / 100)
        rows.append([t, "binance", "BTC", ref_mid - 0.005, ref_mid + 0.005, 1, 1, None, None, None, None, 0.0])
        rows.append([t, "lighter", "BTC", mid - 0.005, mid + 0.005, 1, 1, 5000, 6000, None, None, 0.5])
        rows.append([t, "binance", "BAD", 1.0, 1.001, 1, 1, None, None, None, None, 0.0])
        rows.append([t, "lighter", "BAD", 1000.0, 1000.1, 1, 1, 5000, 5000, None, None, 0.5])  # 1000x units
    w.add(rows[:6000])
    w.flush()
    w.add(rows[6000:])
    w.flush()  # second gzip member appended to the same day file
    files = list(tmp_path.glob("*.csv.gz"))
    assert len(files) == 1
    with gzip.open(files[0], "rt") as fh:
        assert sum(1 for _ in fh) == len(rows) + 1  # one header
    series, n = rm.load_rows(tmp_path)
    cfg = rm.RefmonConfig(small_venues=["lighter"], ref_venues=["binance"], fees={"binance": {"taker": 0.05}})
    out = {r["symbol"]: r for r in rm.rank(series, cfg)}
    assert out["BAD"]["unit_mismatch"]
    btc = out["BTC"]
    assert not btc["unit_mismatch"] and btc["basis_med"] == pytest.approx(0.05, abs=0.02)
    assert btc["cost"] == pytest.approx(0.10) and btc["episodes_day"] > 0 and btc["net_med"] >= 0
    assert btc["depth_med"] == 5000


def test_print_rank_without_data(tmp_path, capsys):
    cfg = rm.RefmonConfig(out_dir=str(tmp_path))
    assert rm.print_rank(cfg, None, 10, None, None, None) == 1


# ---------------------------------------------------------------- v5.9.3: per-market fees, ages, depth, meta
def test_fee_priority():
    cfg = rm.RefmonConfig(fees={"aster": {"maker": 0, "taker": 0.04, "stock": {"maker": 0, "taker": 0.0125}},
                                "entropy": {"maker": 0.03, "taker": 0.09}},
                          fee_overrides={"aster": {"XMR": {"taker": 0.1}}})
    assert cfg.fee("aster", "BTC") == (0.0, 0.04)
    assert cfg.fee("aster", "NVDA", rm.MarketInfo(category="stock", src="config")) == (0.0, 0.0125)
    assert cfg.fee("aster", "XMR", rm.MarketInfo(maker=0.5, taker=0.5)) == (0.0, 0.1)  # override beats the API
    assert cfg.fee("ondo", "X", rm.MarketInfo(maker=0.02, taker=0.05)) == (0.02, 0.05)  # API beats config
    m, t = cfg.fee("entropy", "NVDA", rm.MarketInfo(fee_factor=0.1, src="growth mode"))
    assert m == pytest.approx(0.003) and t == pytest.approx(0.009)
    assert cfg.fee("arcus", "NVDA", rm.MarketInfo(category="stock")) == (0.0, 0.0225)  # built-in stock schedule
    assert rm.RefmonConfig().fee("arcus") == (0.015, 0.045) and rm.RefmonConfig().fee("ondo") == (0.02, 0.05)


def test_load_config_overrides_upper(tmp_path):
    p = tmp_path / "r.yaml"
    p.write_text("fee_overrides: {aster: {xmr: {taker: 0.1}}}\nref_depth_symbols: [BTC]\n")
    cfg = rm.load_refmon_config(str(p))
    assert cfg.fee("aster", "XMR")[1] == 0.1
    p.write_text("ref_depth_interval_s: 1\n")
    with pytest.raises(ConfigError):
        rm.load_refmon_config(str(p))


def test_market_info_parsers():
    a = rm.info_arcus([{"marketDisplayName": "BTC-USD", "effectiveMakerFeePpm": 150, "effectiveTakerFeePpm": 450,
                        "category": "CRYPTO", "minOrderNotional": "5"},
                       {"marketDisplayName": "NVDA-USD", "effectiveMakerFeePpm": 0, "effectiveTakerFeePpm": 225,
                        "category": "EQUITIES"}])
    assert (a["BTC"].maker, a["BTC"].taker, a["BTC"].category, a["BTC"].min_notional) == (0.015, 0.045, "crypto", 5.0)
    assert (a["NVDA"].maker, a["NVDA"].taker, a["NVDA"].category) == (0.0, 0.0225, "stock")
    e = rm.info_entropy([{"name": "io:OAI", "growthMode": "enabled"}, {"name": "io:PRL"},
                         {"name": "io:OLD", "isDelisted": True, "growthMode": "enabled"}])
    assert e["OPENAI"].fee_factor == 0.1 and e["PRL"].fee_factor == 1.0 and "OLD" not in e
    o = rm.info_ondo([{"market": "NVDA-USD.P", "makerFee": "0.0002", "takerFee": "0.0005", "tags": ["Stock"]},
                      {"market": "X-USD.P", "disabled": True}])
    assert o["NVDA"].maker == pytest.approx(0.02) and o["NVDA"].taker == pytest.approx(0.05)
    assert o["NVDA"].category == "stock" and "X" not in o


def test_quote_timestamps_and_okx_funding():
    q = rm.parse_binance_like([{"symbol": "BTCUSDT", "bidPrice": "1", "askPrice": "2", "time": 1795000000500}],
                              {"BTCUSDT": "BTC"}, {})
    assert q["BTC"].ts == pytest.approx(1795000000.5)
    f = rm.parse_okx_funding([{"instId": "BTC-USDT-SWAP", "fundingRate": "0.0001", "fundingTime": "1795017600000",
                               "nextFundingTime": "1795046400000"}])
    assert f["BTC-USDT-SWAP"] == (0.0001, 8.0)
    q = rm.parse_okx([{"instId": "BTC-USDT-SWAP", "bidPx": "1", "askPx": "2", "ts": "1795000001000"}],
                     {"BTC-USDT-SWAP": "BTC"}, {}, f)
    assert q["BTC"].ts == 1795000001.0 and q["BTC"].funding_h == 8.0


def test_book_depth():
    bids = [["100", "10"], ["99.95", "10"], ["99", "100"]]
    asks = [["100.1", "1"], ["100.2", "5"]]
    db, da = rm.book_depth(bids, asks, 0.1)  # mid 100.05, band +-0.10005
    assert db == round(100 * 10 + 99.95 * 10) and da == round(100.1 * 1)
    assert rm.book_depth([], asks, 0.1) is None
    assert rm.book_depth([["100", "2", "0", "3"]], [["100.1", "2", "0", "1"]], 0.1, mult=0.01) == (2, 2)


def test_meta_roundtrip_and_fee_lookup(tmp_path):
    cfg = rm.RefmonConfig(out_dir=str(tmp_path))
    meta = rm.build_meta(cfg, {"arcus": {"BTC", "NVDA"}, "okx": {"BTC"}},
                         {"arcus": rm.info_arcus([{"marketDisplayName": "NVDA-USD", "effectiveMakerFeePpm": 0,
                                                   "effectiveTakerFeePpm": 225, "category": "EQUITIES"}])})
    assert meta["arcus"]["NVDA"]["taker"] == 0.0225 and meta["arcus"]["BTC"]["fee_src"] == "default"
    rm.write_meta(tmp_path, 1793923200.0, meta, ["BTC"], 0.1)
    got = rm.load_meta(tmp_path)
    fee = rm.make_fee_lookup(cfg, got)
    assert fee("arcus", "NVDA") == (0.0, 0.0225) and fee("okx", "BTC") == (0.02, 0.05)
    assert fee("bybit", "BTC") == (0.02, 0.055)  # not in meta: config / default


def _osc(n=3000, t0=1793923200.0 + 3600, small_depth=5000, ref_depth=None):
    small, ref = [], []
    for i in range(n):
        t = t0 + 5 * i
        mid = 100.0 * (1 + (0.05 + 0.15 * math.sin(i / 20)) / 100)
        ref.append((t, 99.995, 100.005, ref_depth, ref_depth, 0.0))
        small.append((t, mid - 0.005, mid + 0.005, small_depth, small_depth, 0.5))
    return small, ref


def test_analyze_both_take_and_min_depth():
    small, ref = _osc()
    mk = rm.analyze_pair(small, ref, (0.0, 0.01), (0.02, 0.03))
    tk = rm.analyze_pair(small, ref, (0.0, 0.01), (0.02, 0.03), both_take=True)
    assert tk["cost"] == pytest.approx(0.08) and mk["cost"] == pytest.approx(0.06)
    assert 0 < tk["episodes_day"] <= mk["episodes_day"]
    thin = rm.analyze_pair(small, ref, (0.0, 0.0), (0.0, 0.05), min_depth=10000)
    assert thin["episodes_day"] == 0 and thin["shallow_day"] > 0
    assert rm.analyze_pair(small, ref, (0.0, 0.0), (0.0, 0.05), min_depth=1000)["episodes_day"] > 0
    small, ref = _osc(ref_depth=200)  # recorded reference depth below the minimum
    r = rm.analyze_pair(small, ref, (0.0, 0.0), (0.0, 0.05), min_depth=1000)
    assert r["episodes_day"] == 0 and r["ref_depth_med"] == 200
    small, ref = _osc()
    ref = [(t, b, a, db, da, 30.0) for t, b, a, db, da, _ in ref]  # stale reference quotes are skipped
    assert rm.analyze_pair(small, ref, (0.0, 0.0), (0.0, 0.05)) is None


def test_rank_small_vs_small():
    small, ref = _osc()
    series = {("lighter", "BTC"): small, ("arcus", "BTC"): ref, ("binance", "BTC"): ref}
    cfg = rm.RefmonConfig(small_venues=["lighter", "arcus"], ref_venues=["binance"])
    pairs = {(r["small"], r["ref"]) for r in rm.rank(series, cfg)}
    assert ("lighter", "arcus") in pairs and ("arcus", "lighter") in pairs and ("lighter", "binance") in pairs
    pairs = {(r["small"], r["ref"]) for r in rm.rank(series, cfg, both_take=True)}
    assert ("lighter", "arcus") in pairs and ("arcus", "lighter") not in pairs
