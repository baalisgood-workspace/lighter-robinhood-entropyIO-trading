"""v5.9.1: a maker order whose state cannot be read because the venue API is down (HTTP 5xx) is waited out
(execution.cancel_outage_wait_s) instead of halting after cancel_confirm_s."""
import time
from decimal import Decimal

from conftest import FakeVenue
from test_ported import plan, run

from arb.models import OrderState, OrderStatus
from arb.util import new_client_index

D = Decimal


class OutageMaker(FakeVenue):
    """Every REST call fails with HTTP 502 until `up_at`; afterwards the order is resolved as `outcome`.
    lost_submit: the first post-only order's send result is lost too (UNKNOWN, as on a 502 answer)."""

    def __init__(self, name, outage_s, outcome="rejected", lost_submit=False, api_error=True):
        super().__init__(name)
        self.cfg.unknown_lookup_s = 0
        self.up_at = time.time() + outage_s
        self.outcome, self.lost_submit, self.api_error = outcome, lost_submit, api_error
        self.cancels = self.lookups = 0
        self.first = None

    def _down(self, where):
        if time.time() >= self.up_at:
            return False
        if self.api_error:
            self.note_failure(where, "VenueError: x /info orderStatus -> HTTP 502: <html>")
        return True

    def _resolve(self, st):
        if self.outcome == "filled":
            st.apply(filled=st.qty, avg_price=st.price, status=OrderStatus.FILLED)
        elif self.outcome == "rejected":
            st.apply(status=OrderStatus.REJECTED, reason="unknown oid")
        else:
            st.apply(status=OrderStatus.CANCELED)
        self._notify(st.client_id)

    async def place_post_only(self, symbol, is_buy, qty, price, reduce_only):
        if self.first is None:
            st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, price, True, reduce_only,
                            status=OrderStatus.UNKNOWN if self.lost_submit else OrderStatus.OPEN)
            self._track(st)
            self.placed.append(st)
            self.first = st
            return st
        return await super().place_post_only(symbol, is_buy, qty, price, reduce_only)

    async def cancel(self, st):
        self.cancels += 1
        if self._down("cancel"):
            raise RuntimeError("x /info orderStatus -> HTTP 502: <html>")
        self._resolve(st)

    async def refresh_order(self, st):
        self.lookups += 1
        if self._down("info orderStatus"):
            raise RuntimeError("x /info orderStatus -> HTTP 502: <html>")
        if st is self.first and not st.status.final and self.lost_submit:
            self._resolve(st)
        return st


def _pair(**kw):
    M, T = OutageMaker("lighter_rh", **kw), FakeVenue("lighter")
    M.set_book("X", "100.90", "101.00")
    T.set_book("X", "99.90", "100.00")
    return M, T


FAST = dict(maker_timeout_s=0.2, order_final_timeout_s=0.2, cancel_confirm_s=0.4)


def test_outage_is_waited_out_instead_of_halting():
    M, T = _pair(outage_s=1.5)  # longer than cancel_confirm_s
    t0 = time.monotonic()
    r = run(M, T, plan(cancel_outage_wait_s=10, **FAST))
    assert not r.halted, r.error
    assert 1.3 < time.monotonic() - t0 < 4 and M.cancels >= 3
    assert M.first.status.final and r.maker_filled == 0 and r.taker_filled == 0
    assert any("API is failing" in e for e in r.events) and any("API is back" in e for e in r.events)


def test_outage_longer_than_the_wait_still_halts():
    M, T = _pair(outage_s=60)
    t0 = time.monotonic()
    r = run(M, T, plan(cancel_outage_wait_s=1.2, **FAST))
    assert r.halted and "not final after cancel" in r.error
    assert 1.1 < time.monotonic() - t0 < 4


def test_outage_wait_can_be_switched_off():
    M, T = _pair(outage_s=60)
    t0 = time.monotonic()
    r = run(M, T, plan(cancel_outage_wait_s=0, **FAST))
    assert r.halted and time.monotonic() - t0 < 1.5  # halts after cancel_confirm_s as before


def test_other_cancel_failures_do_not_extend_the_wait():
    """A cancel that keeps failing while the API itself answers (no HTTP 5xx / timeout) halts after cancel_confirm_s."""
    M, T = _pair(outage_s=60, api_error=False)
    t0 = time.monotonic()
    r = run(M, T, plan(cancel_outage_wait_s=10, **FAST))
    assert r.halted and "not final after cancel" in r.error and time.monotonic() - t0 < 1.5


def test_lost_submit_during_outage_resolved_as_never_placed():
    """11-12 incident: the submit got HTTP 502 and every lookup too; once the API is back the order is unknown
    to the exchange -> rejected, no fill, no halt."""
    M, T = _pair(outage_s=1.5, lost_submit=True)
    r = run(M, T, plan(cancel_outage_wait_s=10, **FAST))
    assert not r.halted, r.error
    assert M.first.status == OrderStatus.REJECTED and r.maker_filled == 0 and len(T.ioc_log) == 0
    assert len(M.placed) == 1 and r.note == "maker order outcome was unknown"  # the slice ends, no new order


def test_lost_submit_that_did_fill_is_hedged_after_the_outage():
    M, T = _pair(outage_s=1.5, lost_submit=True, outcome="filled")
    r = run(M, T, plan(cancel_outage_wait_s=10, **FAST))
    assert not r.halted, r.error
    assert r.maker_filled == D("1") and r.taker_filled == D("1") and r.hedged == D("1")


def test_stop_request_ends_the_outage_wait(fake_pair):
    import asyncio
    from arb.engine.executor import Executor
    M, T = _pair(outage_s=60)
    t0 = time.monotonic()
    stop_at = time.time() + 1.0
    ex = Executor({"lighter_rh": M, "lighter": T}, stop_flag=lambda: time.time() >= stop_at)
    r = asyncio.run(ex.run(plan(maker_timeout_s=5, order_final_timeout_s=0.2, cancel_confirm_s=0.4, cancel_outage_wait_s=30)))
    assert r.halted and time.monotonic() - t0 < 4  # no 30s wait once the engine is stopping


def test_config_rejects_negative_outage_wait():
    import yaml
    from pathlib import Path
    from arb.config import ConfigError, load_config
    raw = yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "config.yaml").read_text())
    raw.setdefault("defaults", {}).setdefault("execution", {})["cancel_outage_wait_s"] = -1
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(raw, f)
    try:
        load_config(f.name)
        assert False, "expected a ConfigError"
    except ConfigError as exc:
        assert "cancel_outage_wait_s" in str(exc)
