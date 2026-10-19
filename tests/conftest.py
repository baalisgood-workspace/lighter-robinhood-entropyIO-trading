import asyncio
import sys
import time
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from arb.config import VenueConfig  # noqa: E402
from arb.models import Balance, Level, MarketInfo, OrderBook, OrderState, OrderStatus  # noqa: E402
from arb.util import new_client_index  # noqa: E402
from arb.venues.base import Venue  # noqa: E402

D = Decimal


class FakeVenue(Venue):
    """In-memory venue. Maker orders fill according to `maker_fills` (list of (delay_s, qty));
    IOC orders fill `ioc_ratio` of their qty at the limit price."""

    def __init__(self, name: str, symbols=("X",)):
        super().__init__(VenueConfig(name=name, rest_url="https://x", ws_url="wss://x", maker_fee_pct=0.0, taker_fee_pct=0.0))
        for s in symbols:
            self.markets[s] = MarketInfo(venue=name, symbol=s, size_step=D("0.01"), min_qty=D("0.01"),
                                         min_notional=D("1"), price_tick=D("0.01"))
        self.connected = True
        self.maker_fills: list[tuple[float, Decimal]] = []
        self.ioc_ratio = D("1")
        self.ioc_log: list[OrderState] = []
        self.placed: list[OrderState] = []
        self.canceled: list[OrderState] = []

    def set_book(self, sym, bid, ask, size="100"):
        self.books[sym] = OrderBook(self.name, sym, bids=[Level(D(str(bid)), D(size))], asks=[Level(D(str(ask)), D(size))],
                                    ts_local=time.time() + 3600)

    async def start(self, symbols, trading): ...
    async def stop(self): ...
    def has_credentials(self): return True

    async def place_post_only(self, symbol, is_buy, qty, price, reduce_only):
        st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, price, True, reduce_only, status=OrderStatus.OPEN)
        self._track(st)
        self.placed.append(st)
        fills = list(self.maker_fills)

        async def run():
            total = D(0)
            for delay, q in fills:
                await asyncio.sleep(delay)
                if st.status.final:
                    return
                total = min(st.qty, total + q)
                st.apply(filled=total, avg_price=price, status=OrderStatus.FILLED if total >= st.qty else None)
                self._notify(st.client_id)
        asyncio.get_running_loop().create_task(run())
        return st

    async def place_ioc(self, symbol, is_buy, qty, limit_price, reduce_only):
        st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, limit_price, False, reduce_only)
        f = self.round_qty(symbol, qty * self.ioc_ratio)
        st.apply(filled=f, avg_price=limit_price, status=OrderStatus.FILLED if f >= qty else OrderStatus.CANCELED)
        self.ioc_log.append(st)
        return st

    async def cancel(self, st):
        st.apply(status=OrderStatus.CANCELED)
        self.canceled.append(st)
        self._notify(st.client_id)

    async def refresh_order(self, st): return st
    async def cancel_all(self, symbols): ...
    async def positions(self): return {}
    async def balance(self): return Balance(self.name, D("10000"), D("10000"))


@pytest.fixture
def fake_pair():
    M, T = FakeVenue("lighter_rh"), FakeVenue("lighter")
    M.set_book("X", "100.90", "101.00")
    T.set_book("X", "99.90", "100.00")
    return M, T
