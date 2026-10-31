"""v5.7.3: Entropy balance of unified / portfolio margin accounts comes from the spot clearinghouse state."""
import asyncio
from decimal import Decimal as D

import pytest

from arb.config import VenueConfig
from arb.venues.entropy import EntropyVenue


def _venue(mode, dex_state=None, spot=None):
    v = EntropyVenue(VenueConfig(name="entropy", rest_url="https://x", ws_url="wss://x", maker_fee_pct=0.003, taker_fee_pct=0.009,
                                 dex="io"), env={"ENTROPY_ACCOUNT_ADDRESS": "0xabc", "ENTROPY_API_PRIVATE_KEY": "k"})
    seen = []

    async def info(payload, cost=1.0):
        seen.append(payload)
        if isinstance(mode, Exception) and payload["type"] == "userAbstraction":
            raise mode
        return {"userAbstraction": mode, "clearinghouseState": dex_state or {}, "spotClearinghouseState": spot or {}}[payload["type"]]
    v._info = info
    v.seen = seen
    return v


SPOT = {"balances": [{"coin": "PURR", "token": 1, "total": "9", "hold": "0"},
                     {"coin": "USDC", "token": 0, "total": "1000.5", "hold": "200.5"}]}


@pytest.mark.parametrize("mode", ["unifiedAccount", "portfolioMargin"])
def test_unified_balance_from_spot(mode):
    dex = {"marginSummary": {"accountValue": "0"}, "withdrawable": "0",
           "assetPositions": [{"position": {"coin": "io:ANTH", "unrealizedPnl": "-1.5"}}]}
    b = asyncio.run(_venue(mode, dex, SPOT).balance())
    assert b.available == D("800.0") and b.equity == D("999.0")


@pytest.mark.parametrize("mode", ["default", "disabled", "dexAbstraction"])
def test_standard_balance_from_dex(mode):
    v = _venue(mode, {"marginSummary": {"accountValue": "300"}, "withdrawable": "250"}, SPOT)
    b = asyncio.run(v.balance())
    assert (b.equity, b.available) == (D("300"), D("250"))
    assert all(p["type"] != "spotClearinghouseState" for p in v.seen)
    assert all(p["dex"] == "io" for p in v.seen if p["type"] == "clearinghouseState")


def test_standard_empty_dex_warns_once(caplog):
    v = _venue("default", {"marginSummary": {"accountValue": "0"}, "withdrawable": "0"})
    asyncio.run(v.balance())
    asyncio.run(v.balance())
    assert sum("holds 0" in r.getMessage() for r in caplog.records) == 1


def test_unknown_mode_fails_closed():
    with pytest.raises(RuntimeError):
        asyncio.run(_venue(RuntimeError("down")).balance())


def test_unified_without_usdc_is_zero():
    b = asyncio.run(_venue("unifiedAccount", {}, {"balances": []}).balance())
    assert (b.equity, b.available) == (D(0), D(0))
