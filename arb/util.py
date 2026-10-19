"""Small shared helpers: decimal rounding, rate limiting, ids, logging."""
from __future__ import annotations

import asyncio
import itertools
import logging
import logging.handlers
import math
import os
import time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Optional

ZERO = Decimal("0")


def D(x) -> Decimal:
    if isinstance(x, Decimal):
        return x
    if isinstance(x, float):
        if not math.isfinite(x):
            raise ValueError(f"non-finite number: {x}")
        return Decimal(repr(x))
    return Decimal(str(x))


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be positive")
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be positive")
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def round_to_step(value: Decimal, step: Decimal, up: bool) -> Decimal:
    return ceil_to_step(value, step) if up else floor_to_step(value, step)


def pct(a: float, b: float) -> float:
    """(a - b) / b * 100"""
    return (a - b) / b * 100.0


class RateLimiter:
    """Token bucket. `rate_per_minute` tokens refill continuously; burst = capacity."""

    def __init__(self, rate_per_minute: float, capacity: Optional[float] = None):
        self.rate = max(rate_per_minute, 1e-6) / 60.0
        self.capacity = capacity if capacity is not None else max(1.0, min(rate_per_minute / 6.0, 20.0))
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
        self.updated = now

    def available(self) -> float:
        self._refill()
        return self.tokens

    async def acquire(self, cost: float = 1.0) -> None:
        async with self._lock:
            while True:
                self._refill()
                if self.tokens >= cost:
                    self.tokens -= cost
                    return
                await asyncio.sleep((cost - self.tokens) / self.rate)


_counter = itertools.count()


def new_client_index() -> int:
    """Unique positive integer < 2**47 usable as Lighter client_order_index and HL cloid."""
    ms = int(time.time() * 1000) % 10**12
    return ms * 100 + next(_counter) % 100 + 1


def setup_logging(log_dir: str = "logs", level: str = "INFO", console: bool = True) -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for h in list(root.handlers):
        root.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    fh = logging.handlers.TimedRotatingFileHandler(os.path.join(log_dir, "arb.log"), when="midnight", backupCount=14, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)
    if console:
        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, level.upper(), logging.INFO))
        ch.setFormatter(fmt)
        root.addHandler(ch)
    for noisy in ("websockets", "urllib3", "asyncio", "aiohttp"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
