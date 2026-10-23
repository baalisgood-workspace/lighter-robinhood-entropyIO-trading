"""v5.4: fees come from the exchange / account tier (Arcus per-market schedule + tier + discount, Lighter account
tier); configured fees are only the fallback unless fee_source: config."""
import asyncio
from decimal import Decimal as D

import pytest

from arb.config import VenueConfig, parse_config
from arb.engine import signals as sg
from arb.models import MarketInfo
from arb.venues.lighter import LighterVenue

from test_engine import make_engine
from test_v52 import _arcus

TIERS = [{"level": 0, "makerFeePpm": 150, "takerFeePpm": 450}, {"level": 1, "makerFeePpm": 120, "takerFeePpm": 380},
         {"level": 2, "makerFeePpm": 80, "takerFeePpm": 320}, {"level": 5, "makerFeePpm": -20, "takerFeePpm": 200}]
RWA = [{"level": 0, "makerFeePpm": 0, "takerFeePpm": 225}, {"level": 1, "makerFeePpm": 0, "takerFeePpm": 190}]


def _row(name, tiers, custom=False, eff=(150, 450)):
    return {"marketDisplayName": name, "feeTiers": tiers, "hasCustomFeeSchedule": custom,
            "effectiveMakerFeePpm": eff[0], "effectiveTakerFeePpm": eff[1]}


def _arcus_fees(level=None, disc=0):
    v = _arcus()
    v.markets["SKHY-USD"] = MarketInfo("arcus", "SKHY-USD", D("0.01"), D("0.01"), D("5"), D("0.01"), extra={"market_id": 9})
    v._keep_fee_row(_row("ENA-USD", TIERS))
    v._keep_fee_row(_row("SKHY-USD", RWA, custom=True, eff=(0, 225)))

    async def req(method, path, query=None, **kw):
        assert path == "/v1/account/stats" and query["include"] == "feeTier"
        return {"address": v.address, "tradingFeeTier": None if level is None else {"level": level, "feeDiscountPpm": disc}}
    v._request = req
    asyncio.run(v.load_account_fees())
    return v


def test_arcus_crypto_and_stock_fees_with_discount():
    v = _arcus_fees(level=0, disc=50000)  # the user's account: Base, 5 % discount
    assert v.maker_fee_pct("ENA-USD") == pytest.approx(0.01425) and v.taker_fee_pct("ENA-USD") == pytest.approx(0.04275)
    assert v.maker_fee_pct("SKHY-USD") == 0 and v.taker_fee_pct("SKHY-USD") == pytest.approx(0.021375)
    assert v.fee_source("SKHY-USD") == "arcus tier 0 custom" and "discount 5%" in v.fee_summary()
    assert v.cfg.maker_fee_pct == 0.015  # new fallback default (Base tier), never used here


def test_arcus_higher_tier_and_rebate():
    v = _arcus_fees(level=2)
    assert v.maker_fee_pct("ENA-USD") == pytest.approx(0.008) and v.taker_fee_pct("ENA-USD") == pytest.approx(0.032)
    v = _arcus_fees(level=5, disc=50000)
    assert v.maker_fee_pct("ENA-USD") == pytest.approx(-0.002)  # rebate is not "discounted"
    assert v.taker_fee_pct("ENA-USD") == pytest.approx(0.019)


def test_arcus_unknown_tier_uses_base_and_missing_row_uses_config():
    v = _arcus_fees(level=None)
    assert v.maker_fee_pct("ENA-USD") == pytest.approx(0.015) and "assumed Base" in v.fee_summary()
    v = _arcus_fees(level=3)  # level not in the market's table -> the market's effective fee
    assert v.maker_fee_pct("ENA-USD") == pytest.approx(0.015) and v.fee_source("ENA-USD") == "arcus base"
    v.markets["NEW-USD"] = MarketInfo("arcus", "NEW-USD", D("1"), D("1"), D("5"), D("0.01"))
    assert v.maker_fee_pct("NEW-USD") == v.cfg.maker_fee_pct and v.fee_source("NEW-USD") == "config"


def test_fee_source_config_forces_configured_values():
    v = _arcus_fees(level=0)
    v.cfg.fee_source, v.cfg.maker_fee_pct, v.cfg.taker_fee_pct = "config", 0.0, 0.0225
    assert v.maker_fee_pct("ENA-USD") == 0.0 and v.taker_fee_pct("ENA-USD") == 0.0225 and v.fee_source("ENA-USD") == "config"


def test_arcus_refresh_reads_new_schedule():
    v = _arcus_fees(level=0)

    async def req(method, path, query=None, **kw):
        if path == "/v1/markets":
            return {"markets": [_row("ENA-USD", RWA, custom=True, eff=(0, 225))]}
        return {"tradingFeeTier": {"level": 1, "feeDiscountPpm": 0}}
    v._request = req
    asyncio.run(v.refresh_fees())
    assert v.maker_fee_pct("ENA-USD") == 0 and v.taker_fee_pct("ENA-USD") == pytest.approx(0.019)


def _lighter(ticks, tier="premium"):
    v = LighterVenue(VenueConfig(name="lighter_rh", rest_url="https://x", ws_url="wss://x", maker_fee_pct=0.0, taker_fee_pct=0.0),
                     env={"LIGHTER_RH_ACCOUNT_INDEX": "42288", "LIGHTER_RH_API_KEY_INDEX": "4",
                          "LIGHTER_RH_API_KEY_PRIVATE_KEY": "k"})
    v.cfg.env_prefix = "LIGHTER_RH"
    v.account_index, v.api_key_index, v._api_private_key = "42288", "4", "k"
    v.markets["OPENAI"] = MarketInfo("lighter_rh", "OPENAI", D("0.01"), D("0.01"), D("10"), D("0.01"))

    class Signer:
        def create_auth_token_with_expiry(self, api_key_index):
            return "tok", None
    v.signer = Signer()
    seen = {}

    async def get(path, params=None, cost=1.0, headers=None):
        seen.update(path=path, headers=headers, params=params)
        return {"user_tier_name": tier, "current_maker_fee_tick": ticks[0], "current_taker_fee_tick": ticks[1]}
    v._get = get
    return v, seen


def test_lighter_account_tier_fees():
    v, seen = _lighter((120, 350))  # Lighter RH Premium
    asyncio.run(v.load_account_fees())
    assert seen["path"] == "/api/v1/accountLimits" and seen["headers"] == {"authorization": "tok"}
    assert v.maker_fee_pct("OPENAI") == pytest.approx(0.012) and v.taker_fee_pct("OPENAI") == pytest.approx(0.035)
    assert v.fee_source("OPENAI") == "lighter_rh premium"
    v, _ = _lighter((0, 0), tier="standard")
    asyncio.run(v.load_account_fees())
    assert v.maker_fee_pct("OPENAI") == 0 and v.fee_source("OPENAI") == "lighter_rh standard"


def test_lighter_implausible_ticks_keep_configured_fees():
    v, _ = _lighter((120000, 350000))
    with pytest.raises(Exception):
        asyncio.run(v.load_account_fees())
    assert v.maker_fee_pct("OPENAI") == 0.0 and v.fee_source("OPENAI") == "config"


def test_engine_thresholds_include_exchange_fees(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c = make_engine(tmp_path, fake_pair)
    base = eng._fees(c)
    M.markets["X"].extra.update(maker_fee_api=0.01425, taker_fee_api=0.04275, fee_src="arcus tier 0")
    assert eng._fees(c) == pytest.approx(base + 2 * 0.01425)
    assert eng._fees(c) == pytest.approx(sg.roundtrip_fee_pct(0.01425, T.taker_fee_pct("X")))


def test_config_validation():
    sym = {"S": {"venues": {"lighter": "S", "arcus": "ENA-USD"}}}
    with pytest.raises(Exception, match="fee_source"):
        parse_config({"venues": {"arcus": {"fee_source": "exchange"}}, "symbols": sym})
    cfg = parse_config({"venues": {}, "symbols": sym})
    a = cfg.venues["arcus"]
    assert (a.fee_source, a.maker_fee_pct, a.taker_fee_pct, a.fee_refresh_s) == ("auto", 0.015, 0.045, 3600.0)
