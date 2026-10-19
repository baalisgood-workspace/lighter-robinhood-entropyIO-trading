"""Pure pricing / signal helpers (no I/O), unit-tested.

Directions are expressed from the maker venue's point of view:
    sell_maker: post-only SELL on the maker venue, taker BUY hedge on the taker venue
    buy_maker : post-only BUY  on the maker venue, taker SELL hedge on the taker venue

Edge (percent):
    sell_maker: raw = (maker_px - taker_px) / taker_px * 100 ; dev = raw - basis
    buy_maker : raw = (taker_px - maker_px) / maker_px * 100 ; dev = raw + basis
where basis = EMA((mid_maker - mid_taker) / mid_taker * 100). A positive dev means
the spread is wider than its natural level in our favour.

Position opened by sell_maker is "short_maker" and is closed with buy_maker
(close_dev = -dev(buy_maker)); "long_maker" is closed with sell_maker.
Grid k opens when dev - costs >= T1 + (k-1)*step and closes when
close_dev <= T1*first_close_ratio (k = 1) or <= open level of grid k-1 (k > 1).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Optional

from arb.models import OrderBook

SELL_MAKER = "sell_maker"
BUY_MAKER = "buy_maker"
SHORT_MAKER = "short_maker"
LONG_MAKER = "long_maker"

OPEN_DIRECTION = {SHORT_MAKER: SELL_MAKER, LONG_MAKER: BUY_MAKER}
CLOSE_DIRECTION = {SHORT_MAKER: BUY_MAKER, LONG_MAKER: SELL_MAKER}
POSITION_OF = {SELL_MAKER: SHORT_MAKER, BUY_MAKER: LONG_MAKER}


def maker_is_buy(direction: str) -> bool:
    return direction == BUY_MAKER


def open_level(t1: float, step: float, k: int) -> float:
    return t1 + (k - 1) * step


def close_level(t1: float, step: float, ratio: float, k: int) -> float:
    return t1 * ratio if k <= 1 else open_level(t1, step, k - 1)


def basis_value(mid_maker: Decimal, mid_taker: Decimal) -> float:
    return float((mid_maker - mid_taker) / mid_taker * 100)


def maker_quote(book: OrderBook, is_buy: bool, mode: str, tick: Optional[Decimal]) -> Optional[Decimal]:
    """Passive price: join best bid/ask, or improve by one tick if it stays passive."""
    bb, ba = book.best_bid, book.best_ask
    if not bb or not ba:
        return None
    if mode == "improve" and tick:
        if is_buy and bb.price + tick < ba.price:
            return bb.price + tick
        if not is_buy and ba.price - tick > bb.price:
            return ba.price - tick
    return bb.price if is_buy else ba.price


@dataclass
class Fill:
    vwap: Decimal
    worst: Decimal


def taker_fill(book: OrderBook, is_buy: bool, qty: Decimal, levels: int, band_pct: float) -> Optional[Fill]:
    """VWAP for an immediate `qty` using at most `levels` levels within best +/- band."""
    side = book.asks if is_buy else book.bids
    if not side or qty <= 0:
        return None
    best = side[0].price
    band = Decimal(str(band_pct)) / 100
    limit = best * (1 + band) if is_buy else best * (1 - band)
    need, cost, worst = qty, Decimal(0), best
    for lv in side[:levels]:
        if (is_buy and lv.price > limit) or (not is_buy and lv.price < limit):
            break
        take = min(need, lv.size)
        cost += take * lv.price
        worst = lv.price
        need -= take
        if need <= 0:
            return Fill(vwap=cost / qty, worst=worst)
    return None


def raw_edge(direction: str, maker_px: Decimal, taker_px: Decimal) -> float:
    if direction == SELL_MAKER:
        return float((maker_px - taker_px) / taker_px * 100)
    return float((taker_px - maker_px) / maker_px * 100)


def adjusted_edge(direction: str, raw: float, basis: float) -> float:
    return raw - basis if direction == SELL_MAKER else raw + basis


def roundtrip_fee_pct(maker_fee: float, taker_fee: float, close_taker_taker: bool = False, maker_venue_taker_fee: float = 0.0) -> float:
    close_maker_leg = maker_venue_taker_fee if close_taker_taker else maker_fee
    return maker_fee + taker_fee + close_maker_leg + taker_fee


def adverse_funding_pct(direction: str, rate_maker: Optional[float], rate_taker: Optional[float], hours: float) -> float:
    """Expected funding cost (percent of notional) over `hours`; favourable funding is not credited.
    Rates are decimal per hour; a long pays `rate` when positive."""
    if rate_maker is None or rate_taker is None:
        return 0.0
    if direction == SELL_MAKER:  # short maker, long taker
        hourly = rate_taker - rate_maker
    else:  # long maker, short taker
        hourly = rate_maker - rate_taker
    return max(0.0, hourly) * hours * 100.0


@dataclass
class Quote:
    direction: str
    maker_px: Optional[Decimal] = None
    taker_vwap: Optional[Decimal] = None
    taker_worst: Optional[Decimal] = None
    raw: Optional[float] = None
    dev: Optional[float] = None
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.dev is not None


def build_quote(direction: str, maker_book: OrderBook, taker_book: OrderBook, qty: Decimal, basis: float,
                maker_mode: str, maker_tick: Optional[Decimal], levels: int, band_pct: float,
                maker_as_taker: bool = False) -> Quote:
    """Executable edge for `direction` and `qty`. With maker_as_taker the maker-venue leg is priced
    as an immediate order too (taker_taker close mode)."""
    q = Quote(direction)
    mb = maker_is_buy(direction)
    if maker_as_taker:
        f = taker_fill(maker_book, mb, qty, levels, band_pct)
        if f is None:
            q.reason = "maker-venue depth"
            return q
        q.maker_px = f.vwap
    else:
        q.maker_px = maker_quote(maker_book, mb, maker_mode, maker_tick)
    if q.maker_px is None:
        q.reason = "maker book"
        return q
    f = taker_fill(taker_book, not mb, qty, levels, band_pct)
    if f is None:
        q.reason = "taker depth"
        return q
    q.taker_vwap, q.taker_worst = f.vwap, f.worst
    q.raw = raw_edge(direction, q.maker_px, f.vwap)
    q.dev = adjusted_edge(direction, q.raw, basis)
    return q


def mid_move_pct(history: list[tuple[float, float]]) -> float:
    if not history:
        return 0.0
    vals = [v for _, v in history]
    lo, hi = min(vals), max(vals)
    return (hi - lo) / lo * 100 if lo > 0 else 0.0


TickFn = Callable[[Decimal], Decimal]
