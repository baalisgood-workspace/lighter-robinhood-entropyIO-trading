import json
from decimal import Decimal as D

from arb.config import parse_config
from arb.engine.claims import Claims
from arb.models import OrderState, OrderStatus
from arb.venues.arcus import ArcusVenue, map_arcus_status, tick_for

TIERS = [{"upToPrice": "5000", "tick": "0.01"}, {"upToPrice": "10000", "tick": "0.02"}, {"tick": "0.5"}]


def test_tick_tiers_and_status():
    assert tick_for(TIERS, D("0.01"), D("1338")) == D("0.01")
    assert tick_for(TIERS, D("0.01"), D("6000")) == D("0.02")
    assert tick_for(TIERS, D("0.01"), D("20000")) == D("0.5")
    assert map_arcus_status({"status": "PARTIALLY_FILLED", "timeInForce": "ALO"}) == OrderStatus.OPEN
    assert map_arcus_status({"status": "PARTIALLY_FILLED", "timeInForce": "IOC"}) == OrderStatus.CANCELED
    assert map_arcus_status({"status": "MARGIN_CANCELED"}) == OrderStatus.CANCELED


def _venue():
    cfg = parse_config({"venues": {}, "symbols": {"S": {"venues": {"lighter": "S", "arcus": "SNDK-USD"}}}}).venues["arcus"]
    v = ArcusVenue(cfg, env={"ARCUS_ACCOUNT_ADDRESS": "0x" + "ab" * 20, "ARCUS_ACCOUNT_INDEX": "0"})
    from arb.models import MarketInfo
    v.markets["SNDK-USD"] = MarketInfo("arcus", "SNDK-USD", D("0.0000001"), D("0.01"), D("5"), D("0.01"),
                                       extra={"market_id": 31, "tick_tiers": TIERS, "status": "ONLINE"})
    return v


def test_ws_book_and_order_flow():
    v = _venue()
    book = {"bids": [["1338.2", "1.5"], ["1338.1", "0"]], "asks": [["1338.21", "0.07"]], "lastSequenceId": 10, "timestamp": 1792382807689284}
    v._on_ws({"type": "subscribed", "channel": "l2Orderbook", "id": "SNDK-USD", "contents": book})
    b = v.book("SNDK-USD")
    assert b.best_bid.price == D("1338.2") and len(b.bids) == 1 and b.is_valid()
    v._on_ws({"type": "channel_data", "channel": "l2Orderbook", "id": "SNDK-USD", "contents": {**book, "lastSequenceId": 9, "bids": [["1", "1"]]}})
    assert v.book("SNDK-USD").best_bid.price == D("1338.2")  # stale sequence ignored

    st = OrderState("123", "arcus", "SNDK-USD", False, D("0.1"), D("1338.21"), True, False, status=OrderStatus.OPEN)
    v._track(st)
    env = {"id": v.address, "accountIndex": 0}
    v._on_ws({"type": "channel_data", "channel": "orders", **env, "contents": {"orders": [
        {"orderId": "o-1", "clientId": "123", "status": "PARTIALLY_FILLED", "timeInForce": "ALO", "originalSize": "0.1", "remainingSize": "0.04"}]}})
    assert st.filled == D("0.06") and st.avg_price == D("1338.21") and st.exchange_id == "o-1"
    v._on_ws({"type": "channel_data", "channel": "userFills", **env, "contents": {"fills": [
        {"tradeId": "t1", "orderId": "o-1", "size": "0.1", "price": "1338.21", "originalSize": "0.1", "remainingSize": "0"}]}})
    v._on_ws({"type": "channel_data", "channel": "orders", **env, "contents": {"orderId": "o-1", "status": "FILLED"}})
    assert st.status == OrderStatus.FILLED and st.filled == D("0.1")
    # someone else's account is ignored
    st2 = OrderState("124", "arcus", "SNDK-USD", True, D("0.1"), D("1338"), True, False, status=OrderStatus.OPEN)
    v._track(st2)
    v._on_ws({"type": "channel_data", "channel": "orders", "id": "0x" + "cd" * 20, "accountIndex": 0,
              "contents": {"orderId": "o-2", "clientId": "124", "status": "FILLED"}})
    assert st2.status == OrderStatus.OPEN


def test_signature_matches_key():
    from nacl.signing import SigningKey
    seed = "11" * 32
    pub = SigningKey(bytes.fromhex(seed)).verify_key.encode().hex()
    v = _venue()
    v.api_key, v._seed = pub, seed
    v._check_credentials()
    sig = v._sign({"b": 1, "a": 2})
    SigningKey(bytes.fromhex(seed)).verify_key.verify(b'{"a":2,"b":1}', bytes.fromhex(sig))


def test_claims_between_instances(tmp_path):
    a = Claims(tmp_path, "lighter+arcus")
    b = Claims(tmp_path, "lighter+entropy")
    assert a.acquire("lighter", "7", "SNDK") == ""
    assert "lighter+arcus" in b.acquire("lighter", "7", "SNDK")  # same account + symbol: blocked
    assert b.acquire("lighter", "8", "SNDK") == ""  # different account: fine
    assert b.acquire("entropy", "0xab", "SNDK") == ""
    a.release_all()
    # stopped instance that still holds a position keeps the symbol
    a.write_accounts({"lighter": "7", "arcus": "0xab:0"})
    (tmp_path / "lighter+arcus" / "positions.json").write_text(json.dumps({"lighter+arcus|SNDK": {"grids": [{"level": 1}]}}))
    assert "open state" in b.acquire("lighter", "7", "SNDK")
    assert b.acquire("lighter", "7", "MU") == ""
