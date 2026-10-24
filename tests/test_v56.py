"""v5.6: residual dust without grids no longer locks a direction, unclosable dust grids are cleaned up on both
venues (top up to the minimum, then reduce-only close), and open signals that are not traded are journaled."""
import asyncio
import json
import time
from decimal import Decimal as D

from arb.engine import signals as sg
from arb.models import OrderState, OrderStatus, Position
from arb.util import new_client_index

from conftest import FakeVenue
from test_engine import make_engine


class PosVenue(FakeVenue):
    """FakeVenue with positions that follow its IOC fills (reduce-only never flips the position)."""

    def __init__(self, name):
        super().__init__(name)
        self.pos = {}

    async def place_ioc(self, symbol, is_buy, qty, limit_price, reduce_only):
        cur = self.pos.get(symbol, D(0))
        if reduce_only:
            qty = min(qty, abs(cur)) if (cur > 0) != is_buy else D(0)
        m = self.markets[symbol]
        st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, limit_price, False, reduce_only)
        if qty <= 0 or qty < m.min_qty or (not (reduce_only and self.reduce_only_any_notional) and qty * limit_price < m.min_notional):
            st.apply(status=OrderStatus.REJECTED, reason="below minimum")
        else:
            st.apply(filled=qty, avg_price=limit_price, status=OrderStatus.FILLED)
            self.pos[symbol] = cur + (qty if is_buy else -qty)
        self.ioc_log.append(st)
        return st

    async def positions(self):
        return {s: Position(self.name, s, q) for s, q in self.pos.items() if q}


def _pair():
    M, T = PosVenue("lighter_rh"), PosVenue("lighter")
    M.set_book("X", "99.90", "100.00")
    T.set_book("X", "100.90", "101.00")
    return M, T


def _dust(tmp_path, direction, grid, residual_maker, m_pos, t_pos, **risk):
    M, T = _pair()
    eng, c = make_engine(tmp_path, (M, T), max_grids=1, order_value_usdc=100,
                         risk={"dust_max_usdc": 150, "dust_grace_s": 60, **risk})
    sp = eng.position(c)
    sp.add_open(direction, 1, D(grid), D(grid), 0.5)
    sp.residual_maker = D(residual_maker)
    M.pos["X"], T.pos["X"] = D(m_pos), D(t_pos)
    T.markets["X"].min_notional = D("200")  # the grid (~100 USDC) is below the taker minimum (the PONS case)
    now = time.time()
    eng.compute(c, now)
    eng.decide(c, now)
    asyncio.run(eng.reconcile())
    return eng, c, sp, M, T


def _journal(eng, kind):
    p = eng.journal_path
    return [json.loads(x) for x in p.read_text().splitlines() if json.loads(x)["kind"] == kind] if p.exists() else []


def test_residual_without_grid_does_not_lock_direction(tmp_path, fake_pair):
    M, T = fake_pair
    eng, c = make_engine(tmp_path, fake_pair)  # default books: sell-maker signal
    sp = eng.position(c)
    sp.direction, sp.residual_maker = sg.LONG_MAKER, D("0.004")  # the VVV / ENA case: grids [] + dust residual
    now = time.time()
    eng.compute(c, now)
    p = eng.decide(c, now)
    assert p and p.kind == "open" and p.direction == sg.SELL_MAKER
    sp.add_open(sg.SHORT_MAKER, 1, D("1"), D("1"), 0.5)  # no assertion error: stale direction replaced
    assert sp.direction == sg.SHORT_MAKER


def test_dust_grid_flattened_on_both_venues(tmp_path):
    # long-maker grid 1 hedged on both legs, taker leg below the taker minimum -> not closable
    eng, c, sp, M, T = _dust(tmp_path, sg.LONG_MAKER, "1", "0", "1", "-1")
    assert c.unclosable.startswith("not closable") and eng.reconciled_ok
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log and not T.ioc_log  # grace period not over yet
    c.unclosable_since -= 61
    asyncio.run(eng._fix_residuals())
    assert [(o.is_buy, o.qty, o.reduce_only) for o in M.ioc_log] == [(False, D("1"), True)]
    assert [(o.is_buy, o.reduce_only) for o in T.ioc_log] == [(False, False), (True, True)]  # top-up, then close
    assert T.ioc_log[1].qty == T.ioc_log[0].qty + 1
    assert not M.pos.get("X") and not T.pos.get("X") and sp.is_flat() and sp.direction is None
    j = _journal(eng, "dust_flatten")
    assert j and j[-1]["ok"] and len(j[-1]["orders"]) == 3
    assert asyncio.run(eng.reconcile())


def test_dust_close_hedge_pending_flattened_on_taker_only(tmp_path):
    # short-maker grid whose maker leg is already bought back, taker hedge below the minimum (the ANTHROPIC case)
    eng, c, sp, M, T = _dust(tmp_path, sg.SHORT_MAKER, "1", "1", "0", "1")
    assert "close hedge pending" in c.unclosable
    c.unclosable_since -= 61
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log
    assert [(o.is_buy, o.reduce_only) for o in T.ioc_log] == [(True, False), (False, True)]
    assert not T.pos.get("X") and sp.is_flat()


def test_dust_too_large_is_left_alone(tmp_path):
    eng, c, sp, M, T = _dust(tmp_path, sg.LONG_MAKER, "1", "0", "1", "-1", dust_max_usdc=50)
    c.unclosable_since -= 61
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log and not T.ioc_log and sp.grids and "dust_max_usdc" in c.dust_note


def test_dust_flatten_can_be_disabled(tmp_path):
    eng, c, sp, M, T = _dust(tmp_path, sg.LONG_MAKER, "1", "0", "1", "-1", dust_flatten=False)
    c.unclosable_since -= 61
    asyncio.run(eng._fix_residuals())
    assert not M.ioc_log and not T.ioc_log and sp.grids


def test_skipped_signal_is_journaled_once(tmp_path, fake_pair):
    eng, c = make_engine(tmp_path, fake_pair)  # default books: sell-maker signal
    sp = eng.position(c)
    sp.add_open(sg.LONG_MAKER, 1, D("1"), D("1"), 0.5)  # opposite position held ...
    fake_pair[1].markets["X"].min_notional = D("200")  # ... that cannot be closed (otherwise this is a close signal)
    now = time.time()
    eng.compute(c, now)
    eng.decide(c, now)
    eng.decide(c, now + 1)
    j = _journal(eng, "signal_skip")
    assert len(j) == 1 and j[0]["reason"] == "opposite position held" and j[0]["direction"] == sg.SELL_MAKER
    assert j[0]["dev"] >= j[0]["need"]


def test_close_only_signal_journaled_not_logged(tmp_path, fake_pair):
    M, T = fake_pair
    raw_close_only = {"close_only": True}
    eng, c = make_engine(tmp_path, fake_pair, trading=raw_close_only)
    now = time.time()
    eng.compute(c, now)
    assert eng.decide(c, now) is None
    j = _journal(eng, "signal_skip")
    assert j and j[0]["reason"] == "close_only"
