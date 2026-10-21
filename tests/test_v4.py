"""v4: guards / venue failure handling / Arcus rejection pushes / markets (ported from lighter-rh and
arcus-maker-lighter-taker v8-v17)."""
import time
from datetime import datetime
from decimal import Decimal as D
from zoneinfo import ZoneInfo

import pytest

from arb.config import ConfigError, parse_config, us_regular_hours
from arb.engine.executor import ExecResult
from arb.models import Balance, OrderState, OrderStatus
from arb.venues.base import classify_failure
from test_arcus_claims import _venue
from test_engine import make_engine

NY = ZoneInfo("America/New_York")


def ny(*a):
    return datetime(*a, tzinfo=NY).timestamp()


# ------------------------------------------------------------------ config / calendar
def test_guards_config_and_validation():
    base = {"venues": {}, "symbols": {"S": {"venues": {"lighter": "S", "lighter_rh": "S"}}}}
    cfg = parse_config(base)
    assert cfg.guards.slippage_pause_pct == 0.08 and cfg.guards.hedge_latency_max_s == 3.2
    cfg = parse_config({**base, "guards": {"slippage_pause_scope": "symbol", "bell": False, "balance_warn_usdc": 500}})
    assert cfg.guards.slippage_pause_scope == "symbol" and cfg.guards.balance_warn_usdc == 500
    with pytest.raises(ConfigError):
        parse_config({**base, "guards": {"slippage_pause_scope": "nope"}})
    with pytest.raises(ConfigError):
        parse_config({**base, "guards": {"no_such_key": 1}})


def test_us_early_close_and_unknown_year():
    assert us_regular_hours(ny(2026, 11, 27, 12, 0))
    assert not us_regular_hours(ny(2026, 11, 27, 14, 0))  # day after Thanksgiving: 13:00 close
    assert us_regular_hours(ny(2026, 11, 30, 14, 0))
    assert not us_regular_hours(ny(2028, 3, 1, 11, 0))  # calendar not maintained -> off-hours


# ------------------------------------------------------------------ venue failures
def test_classify_failure():
    assert classify_failure("HTTP 429 Too Many Requests") == "rate_limit"
    assert classify_failure("invalid nonce (21104)") == "nonce"
    assert classify_failure("HTTP 503 bad gateway") == "api_error"
    assert classify_failure("Connection reset") == "api_error"
    assert classify_failure("post-only order would cross") is None


def test_lighter_reached_send():
    from arb.venues.lighter import _reached_send

    def send_tx():
        raise RuntimeError("boom")

    def sign():
        raise RuntimeError("sign failed")

    for fn, want in ((send_tx, True), (sign, False)):
        try:
            fn()
        except RuntimeError as exc:
            assert _reached_send(exc) is want


# ------------------------------------------------------------------ Arcus rejection pushes
ZERO_REJ = {"clientId": "123", "status": "REJECTED", "originalSize": "0", "marketId": 0, "side": "BUY",
            "rejectReason": "POST_ONLY_WOULD_MATCH"}


def _order(v, cid="123"):
    st = OrderState(cid, "arcus", "SNDK-USD", False, D("0.1"), D("1338"), True, False, status=OrderStatus.OPEN)
    v._track(st)
    return st


def test_arcus_zero_value_rejection_of_new_order():
    v = _venue()
    st = _order(v)
    v._apply_order_row(dict(ZERO_REJ))
    assert st.status == OrderStatus.REJECTED


def test_arcus_zero_value_rejection_after_cancel_or_real_row_is_not_merged():
    v = _venue()
    st = _order(v)
    v._cancel_sent.add(st.client_id)
    v._apply_order_row(dict(ZERO_REJ))
    assert st.status == OrderStatus.OPEN  # rejected cancel, not a rejected order
    v2 = _venue()
    st2 = _order(v2)
    v2._apply_order_row({"orderId": "o-1", "clientId": "123", "status": "OPEN", "timeInForce": "ALO",
                         "originalSize": "0.1", "remainingSize": "0.1", "marketId": 31})
    v2._apply_order_row(dict(ZERO_REJ))
    assert st2.status == OrderStatus.OPEN and st2.filled == 0


def test_arcus_foreign_order_reusing_client_id_ignored():
    v = _venue()
    st = _order(v)
    v._apply_order_row({"orderId": "o-1", "clientId": "123", "status": "OPEN", "timeInForce": "ALO",
                        "originalSize": "0.1", "remainingSize": "0.1"})
    v._apply_order_row({"orderId": "o-OLD", "clientId": "123", "status": "FILLED", "timeInForce": "ALO",
                        "originalSize": "5", "remainingSize": "0"})
    v._apply_fill_row({"tradeId": "t9", "orderId": "o-OLD", "clientId": "123", "size": "5", "price": "1"})
    assert st.status == OrderStatus.OPEN and st.filled == 0
    st2 = _order(v, "124")
    v._apply_order_row({"orderId": "o-2", "clientId": "124", "status": "FILLED", "marketId": 99,
                        "originalSize": "0.1", "remainingSize": "0"})
    assert st2.status == OrderStatus.OPEN  # other market


# ------------------------------------------------------------------ guards
def test_backoff_escalates_from_venue_failures(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    T = eng.venues["lighter"]
    T.note_failure("orders", "HTTP 429 too many requests")
    eng.guards.tick(time.time())
    until1 = eng.guards.pauses["backoff:lighter"][0] - time.time()
    T.note_failure("orders", "HTTP 429 too many requests")
    eng.guards.tick(time.time())
    until2 = eng.guards.pauses["backoff:lighter"][0] - time.time()
    assert 8 < until1 <= 10 and 18 < until2 <= 20
    assert "限流" in eng.guards.global_reason() and eng.guards.venue_paused("lighter", time.time())
    T.note_failure("x", "post only would cross")  # business rejection: not a failure
    assert len(T.failures) == 2


def test_api_failure_pause_is_opt_in(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    T = eng.venues["lighter"]
    for _ in range(12):
        T.note_failure("orders", "HTTP 502")
    eng.guards.tick(time.time())
    assert not eng.guards.pauses
    eng.guards.g.api_failure_pause = True
    T.note_failure("orders", "HTTP 502")
    for _ in range(10):
        T.note_failure("orders", "timeout")
    eng.guards.tick(time.time())
    assert "api:lighter" in eng.guards.pauses


def test_pause_blocks_new_slices(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    now = time.time()
    eng.compute(c, now)
    assert eng.decide(c, now) is not None
    eng.guards.pause("latency", 60, "test")
    assert eng.guards.global_reason(now).startswith("test")
    eng.guards.pauses["latency"] = (now - 1, "test")
    assert eng.guards.global_reason(now) == "" and not eng.guards.pauses


def test_volatility_blocks_opens(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, risk={"volatility_max_pct": 1.0, "volatility_block_s": 100})
    now = time.time()
    eng.guards.observe(c, now - 10, 100.0)
    eng.guards.observe(c, now, 101.5)
    assert c.vol_amp == pytest.approx(1.5) and c.vol_block_until > now + 90
    assert "波动" in eng.guards.open_block(c, now)
    eng.compute(c, now)
    assert eng.decide(c, now) is None and "blocked" in c.status


def test_daily_open_limit_and_leg_spread(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, risk={"max_daily_opens": 1})
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    eng.apply_result(c, p, ExecResult(maker_filled=p.qty, maker_cost=p.qty * D("101"), taker_filled=p.qty,
                                      taker_cost=p.qty * D("100")))
    assert c.opens_today == 1 and "今日开仓" in eng.guards.open_block(c, now)
    eng2, c2 = make_engine(tmp_path / "b", fake_pair, risk={"max_leg_spread_pct": 0.05})
    assert "点差" in eng2.guards.open_block(c2, now)  # taker book 99.90/100.00 = 0.1%


def test_latency_pause_and_stats(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    eng.apply_result(c, p, ExecResult(maker_filled=p.qty, maker_cost=p.qty * D("101"), taker_filled=p.qty,
                                      taker_cost=p.qty * D("100"), hedge_latencies=[0.2, 4.0]))
    assert "latency" in eng.guards.pauses and "对冲延迟" in eng.guards.global_reason()
    assert eng.guards.latency_stats() == (4.0, 4.0)


def test_slippage_pause_all_and_symbol_scope(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    res = ExecResult(maker_filled=p.qty, maker_cost=p.qty, taker_filled=p.qty, taker_cost=p.qty)
    assert eng.guards.on_result(c, p, res, p.required_dev - 0.02) == pytest.approx(-0.02)
    assert not eng.guards.pauses
    eng.guards.on_result(c, p, res, p.required_dev - 0.1)
    assert "slippage" in eng.guards.pauses
    eng2, c2 = make_engine(tmp_path / "b", fake_pair)
    eng2.guards.g.slippage_pause_scope = "symbol"
    eng2.compute(c2, now)
    p2 = eng2.decide(c2, now)
    eng2.guards.on_result(c2, p2, res, p2.required_dev - 0.1)
    assert not eng2.guards.pauses and c2.paused_until > now + 800 and "滑点" in c2.pause_reason


def test_slippage_burst(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    res = ExecResult(maker_filled=p.qty, maker_cost=p.qty, taker_filled=p.qty, taker_cost=p.qty)
    for i in range(4):
        eng.guards.on_result(c, p, res, p.required_dev - 0.02)
    assert not eng.guards.pauses
    eng.guards.on_result(c, p, res, p.required_dev - 0.02)
    assert "slippage" in eng.guards.pauses and "负滑点" in eng.guards.pauses["slippage"][1]


def test_reduce_only_rejection_blocks_opens(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    eng.guards.on_result(c, p, ExecResult(error="order rejected: market is in reduce-only mode"), None)
    assert "只减仓" in eng.guards.open_block(c, time.time())


def test_equity_gap_and_balance_alerts(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)
    eng.guards.g.balance_warn_usdc = 2000
    now = time.time()
    eng.balances["lighter"] = Balance("lighter", D("1500"), D("1500"), ts=now)
    eng.guards.on_balances(now)
    assert "equity_gap" in eng.guards.alerts and "balance:lighter" in eng.guards.alerts
    eng.balances["lighter"] = Balance("lighter", D("5000"), D("5000"), ts=now)
    eng.guards.on_balances(now)
    assert not eng.guards.alerts


def test_signal_confirmation(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair, signal_confirm_s=2.0)
    now = time.time()
    eng.compute(c, now)
    assert eng.decide(c, now) is None and "signal confirming" in c.status
    eng.compute(c, now + 2.5)
    assert eng.decide(c, now + 2.5) is not None


def test_dashboard_shows_guards(tmp_path, fake_pair):
    from rich.console import Console
    from arb.ui import build_view
    eng, c = make_engine(tmp_path, fake_pair)
    eng.guards.pause("latency", 60, "对冲延迟 4.00s")
    eng.guards.alerts["equity_gap"] = "两所权益差 3000 USDC, 需手动调仓"
    eng.pause_text = eng.guards.global_reason()
    con = Console(record=True, width=200, height=50, force_terminal=True)
    con.print(build_view(eng, 50))
    out = con.export_text()
    assert "暂停新单" in out and "权益差" in out and "套利对持仓明细" in out


# ------------------------------------------------------------------ markets
def test_markets_render():
    from arb.markets import render
    cfg = parse_config({"venues": {}, "symbols": {"SNDK": {"venues": {"lighter": "SNDK", "arcus": "SNDK-USD"}}}})
    listing = {"lighter": {"SNDK": "SNDK", "MU": "MU", "ONLY": "ONLY"}, "arcus": {"SNDK": "SNDK-USD", "MU": "MU-USD"}}
    out = render(cfg, listing, yaml_snippet=True)
    assert "SNDK" in out and "MU" in out and "ONLY" not in out
    assert "MU:\n    venues: {lighter: MU, arcus: MU-USD}" in out
