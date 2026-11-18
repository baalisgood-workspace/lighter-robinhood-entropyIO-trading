"""v5.9.4: Hyperliquid / Entropy per-address action budget - tracked from userRateLimit, new maker orders spaced
when it is low (over the budget one per action_over_gap_s), budget rejections do not trigger the venue backoff."""
import asyncio
import time
from decimal import Decimal

import pytest
from conftest import FakeVenue
from test_ported import plan, run

from arb.config import ConfigError, VenueConfig, load_config
from arb.models import OrderState, OrderStatus
from arb.venues.base import classify_failure
from arb.venues.entropy import EntropyVenue

D = Decimal
REJECT = ("Too many cumulative requests sent (41192 > 41086) for cumulative volume traded $31087.88. "
          "Place taker orders to free up 1 request per USDC traded.")


def _venue(used=1000, cap=11000, **cfg):
    v = EntropyVenue(VenueConfig(name="entropy", rest_url="https://x", ws_url="wss://x", maker_fee_pct=0.003,
                                 taker_fee_pct=0.009, dex="io", **cfg),
                     env={"ENTROPY_ACCOUNT_ADDRESS": "0xabc", "ENTROPY_API_PRIVATE_KEY": "k"})

    async def info(payload, cost=1.0):
        assert payload == {"type": "userRateLimit", "user": "0xabc"}
        return {"cumVlm": "1000.0", "nRequestsUsed": v.used, "nRequestsCap": v.cap, "nRequestsSurplus": 0}
    v.used, v.cap = used, cap
    v._info = info
    return v


def test_budget_ok_low_over():
    v = _venue(used=1000, cap=11000)
    assert v.actions_left() is None and v.action_wait_s() == 0.0  # not read yet: never blocks
    asyncio.run(v.refresh_action_budget())
    assert v.actions_left() == 10000 and v.action_wait_s() == 0.0 and v.action_budget_text() == ""
    v.used = 9500  # 1500 left <= soft reserve 2000
    asyncio.run(v.refresh_action_budget())
    v._note_action(new_order=True)
    assert v.actions_left() == 1499
    assert 9.0 < v.action_wait_s() <= 10.0 and "只剩 1499" in v.action_budget_text()
    assert v.action_wait_s(time.time() + 10.5) == 0.0
    v.used = 11050  # over the budget
    asyncio.run(v.refresh_action_budget())
    assert v.actions_left() == -50 and 10.0 < v.action_wait_s() <= 11.0 and "已用完" in v.action_budget_text()
    v._note_action(new_order=False)  # cancels are counted but never move the spacing
    assert v.actions_left() == -51
    v2 = _venue(used=11050, cap=11000, action_budget=False)
    asyncio.run(v2.refresh_action_budget())
    assert v2.action_wait_s() == 0.0 and v2.action_budget_text() == ""


def test_budget_rejection_takes_exchange_numbers_without_backoff():
    v = _venue()
    assert classify_failure(REJECT) == "rate_limit"  # the old path: global venue backoff
    assert v._note_budget_rejection(REJECT)
    assert v.actions_left() == 41086 - 41192 and v.action_wait_s(time.time()) == 0.0  # nothing sent yet
    assert not v._note_budget_rejection("Order must have minimum value of $10")

    class Ex:  # SDK stand-in: rejects with the budget error
        def order(self, *a, **k):
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"error": REJECT}]}}}
    v.exchange = Ex()
    st = asyncio.run(v.place_post_only("io:OAI", True, D("1"), D("100"), False))
    assert st.status == OrderStatus.REJECTED and len(v.failures) == 0  # no venue backoff
    assert 10.0 < v.action_wait_s() <= 11.0


class BudgetMaker(FakeVenue):
    """Maker venue whose action budget allows a new order only every `gap` seconds."""

    def __init__(self, name, gap):
        super().__init__(name)
        self.gap, self.last = gap, 0.0

    def action_wait_s(self, now=None):
        return max(0.0, self.last + self.gap - (time.time() if now is None else now))

    async def place_post_only(self, symbol, is_buy, qty, price, reduce_only):
        self.last = time.time()
        return await super().place_post_only(symbol, is_buy, qty, price, reduce_only)


def _pair(gap):
    M, T = BudgetMaker("lighter_rh", gap), FakeVenue("lighter")
    M.set_book("X", "100.90", "101.00")
    T.set_book("X", "99.90", "100.00")
    return M, T


def test_requote_that_must_wait_ends_the_slice():
    M, T = _pair(gap=30)
    p = plan(required=0.3, maker_timeout_s=5, requote_min_interval_s=0.05, max_requotes=5)

    async def go():
        ex = __import__("arb.engine.executor", fromlist=["Executor"]).Executor({"lighter_rh": M, "lighter": T})
        task = asyncio.create_task(ex.run(p))
        await asyncio.sleep(0.2)
        M.set_book("X", "100.80", "100.85")  # our ask 101.00 is no longer best -> requote wanted
        return await asyncio.wait_for(task, 3)
    res = asyncio.run(go())
    assert len(M.placed) == 1 and res.note.startswith("action budget")


def test_short_wait_is_slept_through():
    M, T = _pair(gap=0.3)
    M.last = time.time()  # the first order may go out ~0.3 s from now: waited for inside the slice
    res = run(M, T, plan(required=0.3, maker_timeout_s=0.8))
    assert len(M.placed) >= 1 and not res.note.startswith("action budget")


def test_config_validation(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("venues:\n  entropy: {enabled: true, action_over_gap_s: 5}\n  lighter: {enabled: true}\n")
    with pytest.raises(ConfigError):
        load_config(str(p))
    v = load_config("config/lighter_entropy.yaml").venues["entropy"]
    assert (v.action_soft_reserve, v.action_low_gap_s, v.action_over_gap_s) == (2000, 10, 11)
