"""Venue-neutral data models."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional

ZERO = Decimal("0")


@dataclass(slots=True)
class Level:
    price: Decimal
    size: Decimal


@dataclass
class OrderBook:
    venue: str
    symbol: str
    bids: list[Level] = field(default_factory=list)  # best first (descending)
    asks: list[Level] = field(default_factory=list)  # best first (ascending)
    ts_exchange: float = 0.0  # seconds, exchange timestamp
    ts_local: float = 0.0  # seconds, local receive time

    @property
    def best_bid(self) -> Optional[Level]:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[Level]:
        return self.asks[0] if self.asks else None

    @property
    def mid(self) -> Optional[Decimal]:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0].price + self.asks[0].price) / 2

    def is_valid(self) -> bool:
        bb, ba = self.best_bid, self.best_ask
        return bool(bb and ba and bb.price > 0 and ba.price > 0 and bb.price < ba.price and bb.size > 0 and ba.size > 0)

    def age(self, now: Optional[float] = None) -> float:
        now = time.time() if now is None else now
        newest = max(self.ts_local, 0.0)
        return now - newest if newest else float("inf")


@dataclass
class MarketInfo:
    venue: str
    symbol: str
    size_step: Decimal
    min_qty: Decimal
    min_notional: Decimal
    price_tick: Optional[Decimal]  # None -> venue-specific rounding (Hyperliquid)
    maker_fee_pct: float = 0.0
    taker_fee_pct: float = 0.0
    extra: dict = field(default_factory=dict)


class OrderStatus(str, Enum):
    PENDING = "pending"  # submitted, not acknowledged yet
    OPEN = "open"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    UNKNOWN = "unknown"

    @property
    def final(self) -> bool:
        return self in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED)


@dataclass
class OrderState:
    client_id: str
    venue: str
    symbol: str
    is_buy: bool
    qty: Decimal
    price: Decimal
    post_only: bool
    reduce_only: bool
    status: OrderStatus = OrderStatus.PENDING
    filled: Decimal = ZERO
    avg_price: Optional[Decimal] = None
    exchange_id: Optional[str] = None
    reason: str = ""
    created: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)

    @property
    def remaining(self) -> Decimal:
        return max(ZERO, self.qty - self.filled)

    def apply(self, *, status: Optional[OrderStatus] = None, filled: Optional[Decimal] = None,
              avg_price: Optional[Decimal] = None, exchange_id: Optional[str] = None, reason: str = "") -> None:
        """Monotonic update: filled quantity never decreases and final states stick."""
        if filled is not None and filled > self.filled:
            self.filled = filled
        if avg_price is not None and avg_price > 0:
            self.avg_price = avg_price
        if exchange_id:
            self.exchange_id = str(exchange_id)
        if status is not None and not self.status.final:
            self.status = status
        if reason:
            self.reason = reason
        self.updated = time.time()


@dataclass
class Position:
    venue: str
    symbol: str
    qty: Decimal  # signed, + long / - short
    entry_price: Optional[Decimal] = None


@dataclass
class Balance:
    venue: str
    equity: Decimal
    available: Decimal
    ts: float = field(default_factory=time.time)
