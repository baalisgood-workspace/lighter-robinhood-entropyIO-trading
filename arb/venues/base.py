"""Abstract venue interface used by the engine.

Every adapter keeps an in-memory order book per symbol (pushed by WebSocket),
tracks the orders it submitted and exposes async trading primitives. Symbols are
the venue-native names (e.g. "OPENAI" on Lighter, "io:OAI" on Entropy).
"""
from __future__ import annotations

import asyncio
import os
import time
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

from arb.models import Balance, MarketInfo, OrderBook, OrderState, OrderStatus, Position
from arb.util import floor_to_step, round_to_step


class VenueError(RuntimeError):
    pass


class Venue(ABC):
    name: str

    def __init__(self, cfg, env=None):
        self.cfg = cfg
        self.name = cfg.name
        self.env = env if env is not None else os.environ
        self.books: dict[str, OrderBook] = {}
        self.markets: dict[str, MarketInfo] = {}
        self.funding_hourly: dict[str, float] = {}  # decimal per hour (0.0001 = 0.01%/h)
        self.orders: dict[str, OrderState] = {}  # client_id -> state
        self.market_event = asyncio.Event()  # set on every book update (engine wakes up)
        self._order_events: dict[str, asyncio.Event] = {}
        self.trading = False
        self.connected = False
        self.alive_ts = 0.0  # wall time of the last message on the public market-data stream

    def quote_age(self, symbol: str, now: Optional[float] = None) -> float:
        """Seconds since the book was last confirmed current.

        Exchanges only push a book when it changes; an unchanged book on a connection that is still
        delivering messages (other books, stats, pongs) is current, so the newer of the two counts."""
        b = self.books.get(symbol)
        if b is None or not b.ts_local:
            return float("inf")
        now = time.time() if now is None else now
        return now - max(b.ts_local, self.alive_ts if self.connected else 0.0)

    # ------------------------------------------------------------ lifecycle
    @abstractmethod
    async def start(self, symbols: list[str], trading: bool) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    def has_credentials(self) -> bool: ...

    # ------------------------------------------------------------ market data
    def book(self, symbol: str) -> Optional[OrderBook]:
        return self.books.get(symbol)

    def market(self, symbol: str) -> MarketInfo:
        try:
            return self.markets[symbol]
        except KeyError:
            raise VenueError(f"{self.name}: unknown market {symbol}") from None

    def round_qty(self, symbol: str, qty: Decimal) -> Decimal:
        return floor_to_step(qty, self.market(symbol).size_step)

    def round_price(self, symbol: str, price: Decimal, up: bool) -> Decimal:
        tick = self.market(symbol).price_tick
        return round_to_step(price, tick, up)

    def price_tick_at(self, symbol: str, price: Decimal) -> Decimal:
        return self.market(symbol).price_tick

    def maker_fee_pct(self, symbol: str) -> float:
        return self.cfg.maker_fee_pct if self.cfg.maker_fee_pct is not None else self.market(symbol).maker_fee_pct

    def taker_fee_pct(self, symbol: str) -> float:
        return self.cfg.taker_fee_pct if self.cfg.taker_fee_pct is not None else self.market(symbol).taker_fee_pct

    # ------------------------------------------------------------ orders
    def _track(self, st: OrderState) -> None:
        self.orders[st.client_id] = st
        self._order_events.setdefault(st.client_id, asyncio.Event())

    def _notify(self, client_id: str) -> None:
        ev = self._order_events.get(client_id)
        if ev:
            ev.set()

    async def wait_order(self, st: OrderState, timeout: float) -> bool:
        """Wait for a push update of this order. Returns True if an update arrived."""
        ev = self._order_events.setdefault(st.client_id, asyncio.Event())
        try:
            await asyncio.wait_for(ev.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            ev.clear()

    def forget(self, st: OrderState) -> None:
        if st.status.final:
            self.orders.pop(st.client_id, None)
            self._order_events.pop(st.client_id, None)

    @abstractmethod
    async def place_post_only(self, symbol: str, is_buy: bool, qty: Decimal, price: Decimal, reduce_only: bool) -> OrderState: ...

    @abstractmethod
    async def place_ioc(self, symbol: str, is_buy: bool, qty: Decimal, limit_price: Decimal, reduce_only: bool) -> OrderState:
        """Taker order. Returns after the order reached a final state (or UNKNOWN on timeout)."""

    @abstractmethod
    async def cancel(self, st: OrderState) -> None: ...

    @abstractmethod
    async def refresh_order(self, st: OrderState) -> OrderState: ...

    @abstractmethod
    async def cancel_all(self, symbols: list[str]) -> None: ...

    # ------------------------------------------------------------ account
    @abstractmethod
    async def positions(self) -> dict[str, Position]: ...

    @abstractmethod
    async def balance(self) -> Balance: ...

    async def wait_final(self, st: OrderState, timeout: float, poll: float = 1.0) -> OrderState:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not st.status.final:
            left = deadline - loop.time()
            if left <= 0:
                break
            got = await self.wait_order(st, min(poll, left))
            if not got and not st.status.final:
                try:
                    await self.refresh_order(st)
                except Exception:  # noqa: BLE001 - refresh is best effort
                    pass
        if not st.status.final:
            st.status = OrderStatus.UNKNOWN if st.status != OrderStatus.OPEN else st.status
        return st
