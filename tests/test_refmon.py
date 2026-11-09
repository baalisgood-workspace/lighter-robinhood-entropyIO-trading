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
