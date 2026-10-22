"""Abstract venue interface used by the engine.

Every adapter keeps an in-memory order book per symbol (pushed by WebSocket),
tracks the orders it submitted and exposes async trading primitives. Symbols are
the venue-native names (e.g. "OPENAI" on Lighter, "io:OAI" on Entropy).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

from arb.models import Balance, MarketInfo, OrderBook, OrderState, OrderStatus, Position
from arb.util import floor_to_step, round_to_step

log = logging.getLogger("arb.venue")


class VenueError(RuntimeError):
    pass


def classify_failure(text: str) -> Optional[str]:
    """rate_limit | nonce | api_error | None (ordinary order rejection: not a venue health problem)."""
    t = (text or "").lower()
    if "429" in t or "too many" in t or "rate limit" in t or "ratelimit" in t or "rate-limit" in t:
        return "rate_limit"
    if "nonce" in t or "21104" in t:
        return "nonce"
    if (any(f"http {c}" in t for c in range(500, 600)) or "timeout" in t or "timed out" in t
            or "transport" in t or "connection" in t or "server disconnected" in t or "cannot connect" in t):
        return "api_error"
    return None


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
        self.alive_ts = 0.0  # wall time of the last frame (incl. ping/pong) on the public market-data stream
        self.generation = 0  # incremented on every market-data (re)connect
        self.book_epoch: dict[str, int] = {}  # per-symbol: bumped whenever a book is invalidated
        self.alarms: list[dict] = []  # market-health alarms drained by the engine (journal)
        self.frames = 0  # received WS frames (dashboard throughput)
        self.alarm_count = 0
        self.failures: deque = deque(maxlen=500)  # (seq, ts, kind, detail) API failures, read by the engine guards
        self.failure_seq = 0
        # v5.2 late fills: finished orders stay reachable for exchange pushes / REST re-reads for a grace
        # period after the executor forgot them. A fill that shows up there was never seen by the executor
        # (typical race: the CANCELED push arrives before the fill push) and is handed to the engine.
        self._retired: dict[str, list] = {}  # client_id -> [state, filled already accounted, expires_at]
        self.late_fills: list[dict] = []  # drained by the engine

    # ------------------------------------------------------------ liveness / health
    def mark_alive(self) -> None:
        self.alive_ts = time.time()
        self.frames += 1

    def on_connected(self) -> None:
        self.generation += 1
        self.alive_ts = time.time()
        self.connected = True
        if self.generation > 1:
            self.alarm("reconnect")

    def alarm(self, kind: str, symbol: str = "", detail: str = "") -> None:
        """kind: connection_timeout | sequence_gap | generation_change | reconnect | degraded"""
        self.alarm_count += 1
        if len(self.alarms) < 1000:
            self.alarms.append({"ts": time.time(), "venue": self.name, "kind": kind, "symbol": symbol, "detail": detail[:200]})

    def note_failure(self, where: str, text: str) -> Optional[str]:
        """Record an API failure if it is a rate limit / nonce / transport problem (not a business rejection)."""
        kind = classify_failure(text)
        if kind is None:
            return None
        self.failure_seq += 1
        self.failures.append((self.failure_seq, time.time(), kind, f"{where}: {text}"[:200]))
        self.alarm(kind, "", f"{where}: {text}")
        return kind

    def reset_books(self, symbols: Optional[list[str]] = None) -> None:
        """Invalidate books (all, or the given symbols) until a fresh snapshot arrives."""
        for s in list(self.books if symbols is None else symbols):
            self.books.pop(s, None)
            self.book_epoch[s] = self.book_epoch.get(s, 0) + 1
        if symbols is None:
            for s in self.markets:
                self.book_epoch[s] = self.book_epoch.get(s, 0) + 1
        self.market_event.set()

    def epoch(self, symbol: str) -> tuple[int, int]:
        return (self.generation, self.book_epoch.get(symbol, 0))

    def quote_age(self, symbol: str, now: Optional[float] = None) -> float:
        """Seconds since the book was last confirmed current.

        Exchanges only push a book when it changes; an unchanged book on a connection that is still
        delivering messages (other books, stats, pongs) is current, so the newer of the two counts."""
        b = self.books.get(symbol)
        if b is None or not b.ts_local:
            return float("inf")
        now = time.time() if now is None else now
        return now - max(b.ts_local, self.alive_ts if self.connected else 0.0)

    @property
    def account_id(self) -> str:
        """Identity of the trading account (used to keep parallel instances off each other's symbols)."""
        return "default"

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
        if client_id in self._retired:
            self._check_late(client_id)

    def _lookup(self, client_id: str) -> Optional[OrderState]:
        """Live order, or a recently finished one still inside the late-fill grace period."""
        st = self.orders.get(client_id)
        if st is not None:
            return st
        r = self._retired.get(client_id)
        return r[0] if r is not None else None

    def _check_late(self, client_id: str) -> None:
        r = self._retired.get(client_id)
        if r is None:
            return
        st, seen, _exp = r
        if st.filled > seen:
            delta = st.filled - seen
            r[1] = st.filled
            px = st.avg_price or st.price
            fill = {"ts": time.time(), "venue": self.name, "symbol": st.symbol, "is_buy": st.is_buy, "qty": delta,
                    "price": px, "client_id": st.client_id, "post_only": st.post_only}
            self.late_fills.append(fill)
            log.warning("%s LATE FILL %s %s %s @ %s (order %s already finished, %s accounted before)", self.name,
                        st.symbol, "buy" if st.is_buy else "sell", delta, px, st.client_id, seen)
            self.alarm("late_fill", st.symbol, f"{st.client_id} {'buy' if st.is_buy else 'sell'} {delta}")

    def drain_late_fills(self) -> list[dict]:
        out, self.late_fills = self.late_fills, []
        return out

    def drop_retired(self, symbol: Optional[str] = None) -> None:
        """Stop watching finished orders (all, or one symbol): after a reconcile adopted the exchange position,
        a later push for them must not be counted a second time."""
        for cid in [c for c, r in self._retired.items() if symbol is None or r[0].symbol == symbol]:
            self._retired.pop(cid, None)

    def _prune_retired(self) -> None:
        now = time.time()
        for cid in [c for c, r in self._retired.items() if r[2] < now]:
            self._retired.pop(cid, None)

    def _verify_retired_later(self, st: OrderState) -> None:
        """Re-read a finished (canceled) maker order from REST after it was forgotten, so a fill the push
        streams delivered late or never is still found. Enabled per venue (verify_cancel_rest)."""
        delays = [d for d in (getattr(self.cfg, "verify_cancel_delays_s", None) or []) if d > 0]
        if not delays:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def run() -> None:
            waited = 0.0
            for d in sorted(delays):
                await asyncio.sleep(max(0.0, d - waited))
                waited = d
                if st.client_id not in self._retired:
                    return
                try:
                    await self.refresh_order(st)
                except Exception as exc:  # noqa: BLE001 - best effort, the reconcile is the backstop
                    log.debug("%s late-fill re-read %s failed: %s", self.name, st.client_id, exc)
                self._check_late(st.client_id)
        loop.create_task(run())

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
            self._prune_retired()
            grace = float(getattr(self.cfg, "late_fill_grace_s", 0) or 0)
            if grace > 0 and st.status != OrderStatus.REJECTED:
                self._retired[st.client_id] = [st, st.filled, time.time() + grace]
                if st.post_only and st.status == OrderStatus.CANCELED and getattr(self.cfg, "verify_cancel_rest", False):
                    self._verify_retired_later(st)

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
