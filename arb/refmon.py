"""v5.9 `arb refmon` / `arb refrank`: read-only reference monitor (no keys, never trades).

Records, every `interval_s`, the best bid/ask of every symbol that is listed on at least one of our small venues
(Lighter, Lighter RH, Entropy, Arcus - their public WebSocket books, same adapters as `monitor`) and on at least
one big reference venue (Binance, OKX, Bybit, Hyperliquid, Aster, Ondo Perps - one public REST "all tickers"
request per venue and interval). One gzip CSV per UTC day under `out_dir`:

    ts, venue, symbol, bid, ask, bid_qty, ask_qty, depth_bid_usd, depth_ask_usd, funding_rate, funding_h, age_s

- symbol: common base name (BTC, NVDA, OPENAI, 1000PEPE ...); venue-native names are mapped by `normalize`.
- bid_qty / ask_qty: base units at the best price (blank when the venue does not report them: Hyperliquid, Ondo).
- depth_*_usd: notional within depth_band_pct of the mid. Small venues: every row. Reference venues (v5.9.3): only
  the symbols in `ref_depth_symbols`, from a separate order-book poll every `ref_depth_interval_s` (latest poll).
- funding_rate: as reported for one funding interval of funding_h hours (blank when unknown).
- age_s: small venues = seconds since the book last changed; reference venues (v5.9.3) = sample time minus the
  venue's own quote timestamp (Binance / Aster / OKX report one; blank for Bybit / Hyperliquid / Ondo).
- Hyperliquid has no bid/ask in its one-request snapshot: the impact prices (`impactPxs`, prices to trade the
  venue's impact notional) are recorded as bid/ask - a conservative executable price.

v5.9.3: next to each day file, `YYYY-MM-DD.meta.json` records per venue and symbol the fees refrank uses (from the
venue's API where it publishes them: Arcus per market, Entropy growth mode, Ondo per contract; otherwise the
config), the category (stock / crypto / other) and the minimum order notional. Rewritten every `meta_refresh_s`.

`arb refrank` reads those files and ranks (small venue, other venue, symbol) combinations: how far the small
venue's price deviates from the other venue, how often the deviation exceeds the round-trip fees, how fast it
comes back and how deep the books are. It is an upper bound: it does not know whether a maker order would have
been filled, nor what an order does to the price after the venue's own latency (Lighter Standard accounts:
about 300 ms on taker orders).
"""
from __future__ import annotations

import asyncio
import csv
import gzip
import json
import logging
import math
import statistics
import time
import types
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

from arb.config import ConfigError, VENUE_NAMES, parse_venues

log = logging.getLogger("arb.refmon")

REF_VENUES = ("binance", "okx", "bybit", "hyperliquid", "aster", "ondo")
COLS = ["ts", "venue", "symbol", "bid", "ask", "bid_qty", "ask_qty", "depth_bid_usd", "depth_ask_usd",
        "funding_rate", "funding_h", "age_s"]

# base-tier fee schedules (percent, checked 2026-11-18), override in refmon.yaml `fees:` with your own tiers.
# Used only where the venue's API does not give a per-market fee (Arcus, Entropy and Ondo do, see MarketInfo).
DEFAULT_FEES = {
    "lighter": (0.0, 0.0), "lighter_rh": (0.0, 0.0), "entropy": (0.03, 0.09), "arcus": (0.015, 0.045),
    "binance": (0.02, 0.05), "okx": (0.02, 0.05), "bybit": (0.02, 0.055), "hyperliquid": (0.015, 0.045),
    "aster": (0.0, 0.04), "ondo": (0.02, 0.05),
}
# stock / ETF / index perps where the venue charges a different schedule (`fees: {venue: {stock: {...}}}` overrides)
DEFAULT_STOCK_FEES = {"arcus": (0.0, 0.0225), "aster": (0.0, 0.0125)}
ENTROPY_GROWTH_FACTOR = 0.1  # Hyperliquid HIP-3 growth mode: 10% of the normal fee
STOCK_TAGS = {"STOCK", "STOCKS", "ETF", "INDEX", "INDICES", "EQUITIES", "COMMODITY", "COMMODITIES"}
URLS = {
    "binance": "https://fapi.binance.com", "aster": "https://fapi.asterdex.com", "okx": "https://www.okx.com",
    "bybit": "https://api.bybit.com", "hyperliquid": "https://api.hyperliquid.xyz", "ondo": "https://api.ondoperps.xyz",
}
ALIASES = {"OAI": "OPENAI", "ANTH": "ANTHROPIC"}


@dataclass
class RefmonConfig:
    small_venues: list = field(default_factory=lambda: list(VENUE_NAMES))
    ref_venues: list = field(default_factory=lambda: list(REF_VENUES))
    interval_s: float = 5.0
    depth_band_pct: float = 0.1
    out_dir: str = "data/refmon"
    flush_s: float = 60.0
    funding_refresh_s: float = 300.0
    min_edge_pct: float = 0.02  # refrank: an episode needs edge >= round-trip fees + this (like the bot's T1)
    include: list = field(default_factory=list)  # only these symbols (empty = every overlapping symbol)
    exclude: list = field(default_factory=list)
    fees: dict = field(default_factory=dict)  # {venue: {maker: pct, taker: pct, stock: {maker, taker}}}
    fee_overrides: dict = field(default_factory=dict)  # {venue: {SYMBOL: {maker: pct, taker: pct}}}, beats the API
    venues: dict = field(default_factory=dict)  # small-venue overrides, as in config.yaml `venues:`
    ref_depth_symbols: list = field(default_factory=list)  # reference-venue order-book depth for these symbols
    ref_depth_interval_s: float = 15.0
    ref_depth_venues: list = field(default_factory=list)  # reference venues polled for depth (empty = all)
    meta_refresh_s: float = 3600.0  # re-read fees / categories / minimum sizes and rewrite the day's meta file

    def fee(self, venue: str, symbol: Optional[str] = None, info: Optional["MarketInfo"] = None) -> tuple[float, float]:
        return self.fee_src(venue, symbol, info)[:2]

    def fee_src(self, venue: str, symbol: Optional[str] = None,
                info: Optional["MarketInfo"] = None) -> tuple[float, float, str]:
        """(maker %, taker %, source). Priority: fee_overrides > the venue's own per-market fee (info) >
        `fees: {venue: {stock: ...}}` for stock-like markets > `fees: {venue: ...}` > built-in base tier."""
        ov = ((self.fee_overrides.get(venue) or {}).get(symbol.upper()) if symbol else None) or None
        if ov:
            m, t = self._venue_fee(venue, False)
            return float(ov.get("maker", m)), float(ov.get("taker", t)), "override"
        if info is not None and info.maker is not None and info.taker is not None:
            return info.maker, info.taker, info.src
        stock = info is not None and info.category == "stock"
        m, t = self._venue_fee(venue, stock)
        factor = info.fee_factor if info is not None else 1.0
        src = ("config" if venue in self.fees else "default") + (" stock" if stock else "")
        if factor != 1.0:
            src += f" x{factor:g} ({info.src})"
        return m * factor, t * factor, src

    def _venue_fee(self, venue: str, stock: bool) -> tuple[float, float]:
        f = self.fees.get(venue) or {}
        m, t = DEFAULT_FEES.get(venue, (0.0, 0.0))
        m, t = float(f.get("maker", m)), float(f.get("taker", t))
        if stock:
            st = f.get("stock")
            if isinstance(st, dict):
                return float(st.get("maker", m)), float(st.get("taker", t))
            if venue in DEFAULT_STOCK_FEES:
                return DEFAULT_STOCK_FEES[venue]
        return m, t


def load_refmon_config(path: Optional[str]) -> RefmonConfig:
    raw = {}
    if path:
        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    names = set(RefmonConfig.__dataclass_fields__)
    unknown = set(raw) - names
    if unknown:
        raise ConfigError(f"refmon config: unknown keys {sorted(unknown)}")
    cfg = RefmonConfig(**raw)
    bad = [v for v in cfg.small_venues if v not in VENUE_NAMES] + [v for v in cfg.ref_venues if v not in REF_VENUES]
    if bad:
        raise ConfigError(f"refmon config: unknown venues {bad}; small {list(VENUE_NAMES)}, reference {list(REF_VENUES)}")
    if cfg.interval_s < 1 or cfg.depth_band_pct <= 0 or cfg.flush_s < cfg.interval_s:
        raise ConfigError("refmon config: interval_s >= 1, depth_band_pct > 0, flush_s >= interval_s")
    if cfg.ref_depth_interval_s < 2 or cfg.meta_refresh_s < 60:
        raise ConfigError("refmon config: ref_depth_interval_s >= 2, meta_refresh_s >= 60")
    if [v for v in cfg.ref_depth_venues if v not in cfg.ref_venues]:
        raise ConfigError("refmon config: ref_depth_venues must be reference venues listed in ref_venues")
    if len(cfg.ref_depth_symbols) > 40:
        raise ConfigError("refmon config: ref_depth_symbols: at most 40 symbols (one order-book request each per venue)")
    for v, f in {**cfg.fees, **cfg.fee_overrides}.items():
        if not isinstance(f, dict):
            raise ConfigError(f"refmon config: fees / fee_overrides.{v} must be a mapping")
    cfg.fee_overrides = {v: {str(k).upper(): x for k, x in (f or {}).items()} for v, f in cfg.fee_overrides.items()}
    return cfg


# ---------------------------------------------------------------- symbol names
def normalize(venue: str, name: str) -> Optional[str]:
    """Venue-native market name -> common base symbol (None = not a USDT/USD linear perp we compare)."""
    n = name.upper()
    if venue in ("binance", "aster", "bybit"):
        if not n.endswith("USDT"):
            return None
        n = n[:-4]
    elif venue == "okx":
        parts = n.split("-")
        if len(parts) != 3 or parts[1] != "USDT" or parts[2] != "SWAP":
            return None
        n = parts[0]
    elif venue == "ondo":
        if not n.endswith("-USD.P"):
            return None
        n = n[:-6]
    elif venue == "hyperliquid":
        if name.startswith("k") and name[1:].isupper():  # kPEPE = 1000 PEPE
            n = "1000" + name[1:]
    elif venue == "arcus":
        n = n[:-4] if n.endswith("-USD") else n
    elif venue == "entropy":
        n = n.split(":", 1)[1] if ":" in n else n
    return ALIASES.get(n, n) if n else None


# ---------------------------------------------------------------- reference venues (public REST)
@dataclass
class Quote:
    bid: float
    ask: float
    bid_qty: Optional[float] = None
    ask_qty: Optional[float] = None
    funding_rate: Optional[float] = None
    funding_h: Optional[float] = None
    ts: Optional[float] = None  # the venue's own quote time (epoch seconds), when it reports one


@dataclass
class MarketInfo:
    """Per (venue, symbol) facts recorded in the day's meta file. maker/taker (percent) only when the venue's API
    publishes the fee of this very market; otherwise the config fee x fee_factor applies (RefmonConfig.fee_src)."""
    maker: Optional[float] = None
    taker: Optional[float] = None
    fee_factor: float = 1.0
    category: Optional[str] = None  # stock | crypto | other
    min_notional: Optional[float] = None
    src: str = "api"


def _cat(base: str, stock_flag: Optional[bool] = None) -> str:
    if stock_flag:
        return "stock"
    from arb.autosym import classify
    c = classify(base)
    return "stock" if c == "us_equity" else c


def info_arcus(rows: list) -> dict[str, MarketInfo]:
    """Arcus /v1/markets: effective per-market fees in ppm (Base tier; your tier may be lower) and categories."""
    out = {}
    for m in rows:
        name = m.get("marketDisplayName")
        base = normalize("arcus", name) if name else None
        if not base:
            continue
        mk, tk = _f(m.get("effectiveMakerFeePpm")), _f(m.get("effectiveTakerFeePpm"))
        cat = str(m.get("category") or "").upper()
        out[base] = MarketInfo(mk / 1e4 if mk is not None else None, tk / 1e4 if tk is not None else None,
                               category="stock" if cat in STOCK_TAGS else _cat(base),
                               min_notional=_f(m.get("minOrderNotional")), src="api base tier")
    return out


def info_entropy(universe: list) -> dict[str, MarketInfo]:
    """Entropy (HIP-3 dex) meta: growth-mode markets pay 10% of the normal fee (checked against real fills)."""
    out = {}
    for u in universe:
        if u.get("isDelisted"):
            continue
        base = normalize("entropy", u.get("name", ""))
        if base:
            growth = u.get("growthMode") == "enabled"
            out[base] = MarketInfo(fee_factor=ENTROPY_GROWTH_FACTOR if growth else 1.0, category=_cat(base),
                                   src="growth mode" if growth else "no growth mode")
    return out


def _f(x) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def parse_binance_like(book: list, listed: dict[str, str], funding: dict[str, tuple]) -> dict[str, Quote]:
    """Binance / Aster `/fapi/v1/ticker/bookTicker` (all symbols). listed: native -> base."""
    out = {}
    for r in book:
        base = listed.get(r.get("symbol"))
        bid, ask = _f(r.get("bidPrice")), _f(r.get("askPrice"))
        if base and bid and ask:
            fr, fh = funding.get(r["symbol"], (None, None))
            t = _f(r.get("time"))
            out[base] = Quote(bid, ask, _f(r.get("bidQty")), _f(r.get("askQty")), fr, fh, t / 1000 if t else None)
    return out


def parse_okx(rows: list, listed: dict[str, str], ct_val: dict[str, float],
              funding: Optional[dict[str, tuple]] = None) -> dict[str, Quote]:
    out = {}
    for r in rows:
        inst = r.get("instId")
        base = listed.get(inst)
        bid, ask = _f(r.get("bidPx")), _f(r.get("askPx"))
        if base and bid and ask:
            cv = ct_val.get(inst, 1.0)
            bq, aq = _f(r.get("bidSz")), _f(r.get("askSz"))
            fr, fh = (funding or {}).get(inst, (None, None))
            t = _f(r.get("ts"))
            out[base] = Quote(bid, ask, bq * cv if bq is not None else None, aq * cv if aq is not None else None,
                              fr, fh, t / 1000 if t else None)
    return out


def parse_okx_funding(rows: list) -> dict[str, tuple]:
    """OKX /api/v5/public/funding-rate?instId=ANY: instId -> (rate, interval hours)."""
    out = {}
    for r in rows:
        ft, nt = _f(r.get("fundingTime")), _f(r.get("nextFundingTime"))
        h = round((nt - ft) / 3.6e6, 2) if ft and nt and nt > ft else None
        out[r.get("instId")] = (_f(r.get("fundingRate")), h)
    return out


def parse_bybit(rows: list, listed: dict[str, str]) -> dict[str, Quote]:
    out = {}
    for r in rows:
        base = listed.get(r.get("symbol"))
        bid, ask = _f(r.get("bid1Price")), _f(r.get("ask1Price"))
        if base and bid and ask:
            out[base] = Quote(bid, ask, _f(r.get("bid1Size")), _f(r.get("ask1Size")), _f(r.get("fundingRate")),
                              _f(r.get("fundingIntervalHour")))
    return out


def parse_hyperliquid(data: list) -> dict[str, Quote]:
    meta, ctxs = data
    out = {}
    for u, c in zip(meta.get("universe", []), ctxs):
        if u.get("isDelisted"):
            continue
        base = normalize("hyperliquid", u["name"])
        imp = c.get("impactPxs") or []
        bid, ask = (_f(imp[0]), _f(imp[1])) if len(imp) == 2 else (_f(c.get("midPx")), _f(c.get("midPx")))
        if base and bid and ask:
            out[base] = Quote(bid, ask, None, None, _f(c.get("funding")), 1.0)
    return out


def parse_ondo(rows: list) -> dict[str, Quote]:
    out = {}
    for r in rows:
        if r.get("disabled"):
            continue
        base = normalize("ondo", r.get("market", ""))
        bid, ask = _f(r.get("bid")), _f(r.get("ask"))
        if base and bid and ask:
            out[base] = Quote(bid, ask, None, None, _f(r.get("fundingRate")), None)
    return out


def info_ondo(rows: list) -> dict[str, MarketInfo]:
    """Ondo contracts carry their own makerFee / takerFee (fractions) and tags (Stock / ETF / Crypto ...)."""
    out = {}
    for r in rows:
        if r.get("disabled"):
            continue
        base = normalize("ondo", r.get("market", ""))
        if not base:
            continue
        mk, tk = _f(r.get("makerFee")), _f(r.get("takerFee"))
        tags = {str(t).upper() for t in r.get("tags") or []}
        out[base] = MarketInfo(mk * 100 if mk is not None else None, tk * 100 if tk is not None else None,
                               category=_cat(base, bool(tags & STOCK_TAGS)), src="api")
    return out


def _min_notional(filters: list) -> Optional[float]:
    for f in filters or []:
        if f.get("filterType") == "MIN_NOTIONAL":
            return _f(f.get("notional") or f.get("minNotional"))
    return None


def book_depth(bids, asks, band_pct: float, mult: float = 1.0) -> Optional[tuple[float, float]]:
    """(bid USD, ask USD) within band_pct of the mid, from [[price, size, ...], ...] levels (best first).
    mult converts the venue's size unit to base units (OKX contracts)."""
    try:
        bb, ba = float(bids[0][0]), float(asks[0][0])
    except (IndexError, TypeError, ValueError, KeyError):
        return None
    mid = (bb + ba) / 2
    lim = mid * band_pct / 100
    side = lambda lv: sum(float(p) * float(s) * mult for p, s, *_ in lv if abs(float(p) - mid) <= lim)  # noqa: E731
    return round(side(bids)), round(side(asks))


class RefSource:
    """One reference venue: market list at start (and every meta refresh), one snapshot request per interval,
    funding every few minutes, and (v5.9.3) an optional order-book poll for a few symbols (depth)."""

    def __init__(self, venue: str, session, funding_refresh_s: float):
        self.venue = venue
        self.url = URLS[venue]
        self.s = session
        self.listed: dict[str, str] = {}  # native -> base
        self.native: dict[str, str] = {}  # base -> native
        self.info: dict[str, MarketInfo] = {}  # base -> fees / category / minimum notional
        self.ct_val: dict[str, float] = {}
        self.funding: dict[str, tuple] = {}
        self.funding_ts = 0.0
        self.funding_refresh_s = funding_refresh_s
        self.depth: dict[str, tuple] = {}  # base -> (bid USD, ask USD, local time)
        self.errors = 0
        self.depth_errors = 0

    async def _get(self, path: str):
        async with self.s.get(self.url + path, allow_redirects=False) as r:
            r.raise_for_status()
            return await r.json(content_type=None)

    async def _post(self, body: dict):
        async with self.s.post(self.url + "/info", json=body, allow_redirects=False) as r:
            r.raise_for_status()
            return await r.json(content_type=None)

    def _add(self, native: str, base: str, info: Optional[MarketInfo] = None) -> None:
        self.listed[native] = base
        self.native[base] = native
        self.info[base] = info or MarketInfo(category=_cat(base), src="config")

    async def load_markets(self) -> set[str]:
        v = self.venue
        self.listed, self.native, self.info = {}, {}, {}
        if v in ("binance", "aster"):
            d = await self._get("/fapi/v1/exchangeInfo")
            for s in d.get("symbols", []):
                ct = s.get("contractType")
                if s.get("status") == "TRADING" and ct in ("PERPETUAL", "TRADIFI_PERPETUAL"):
                    base = normalize(v, s["symbol"])
                    if base:
                        sub = {str(x).upper() for x in s.get("underlyingSubType") or []}
                        stock = ct == "TRADIFI_PERPETUAL" or bool(sub & STOCK_TAGS)
                        self._add(s["symbol"], base, MarketInfo(category=_cat(base, stock),
                                                                min_notional=_min_notional(s.get("filters")), src="config"))
        elif v == "okx":
            d = await self._get("/api/v5/public/instruments?instType=SWAP")
            for s in d.get("data", []):
                if s.get("state") == "live" and s.get("settleCcy") == "USDT" and s.get("ctType") == "linear":
                    base = normalize(v, s["instId"])
                    if base:
                        self._add(s["instId"], base)
                        self.ct_val[s["instId"]] = _f(s.get("ctVal")) or 1.0
        elif v == "bybit":
            d = await self._get("/v5/market/instruments-info?category=linear&limit=1000")
            for s in d.get("result", {}).get("list", []):
                if s.get("status") == "Trading" and s.get("contractType") == "LinearPerpetual" and s.get("quoteCoin") == "USDT":
                    base = normalize(v, s["symbol"])
                    if base:
                        mn = _f((s.get("lotSizeFilter") or {}).get("minNotionalValue"))
                        self._add(s["symbol"], base, MarketInfo(category=_cat(base), min_notional=mn, src="config"))
        elif v == "hyperliquid":
            meta, _ = await self._post({"type": "metaAndAssetCtxs"})
            for u in meta.get("universe", []):
                base = normalize(v, u["name"])
                if base and not u.get("isDelisted"):
                    self._add(u["name"], base)
        elif v == "ondo":
            rows = (await self._get("/v1/perps/contracts?sparkline=false")).get("result", [])
            info = info_ondo(rows)
            for r in rows:
                base = normalize(v, r.get("market", ""))
                if base in info:
                    self._add(r["market"], base, info[base])
        return set(self.native)

    async def _refresh_funding(self) -> None:
        if self.venue not in ("binance", "aster", "okx") or time.time() - self.funding_ts < self.funding_refresh_s:
            return
        self.funding_ts = time.time()
        try:
            if self.venue == "okx":  # one request for every swap (instId=ANY)
                self.funding = parse_okx_funding((await self._get("/api/v5/public/funding-rate?instId=ANY")).get("data", []))
                return
            prem = await self._get("/fapi/v1/premiumIndex")
            hours: dict[str, float] = {}
            try:
                for r in await self._get("/fapi/v1/fundingInfo"):
                    hours[r["symbol"]] = _f(r.get("fundingIntervalHours")) or 8.0
            except Exception as exc:  # noqa: BLE001 - intervals are optional (default 8h)
                log.debug("%s fundingInfo: %s", self.venue, exc)
            self.funding = {r["symbol"]: (_f(r.get("lastFundingRate")), hours.get(r["symbol"], 8.0)) for r in prem}
        except Exception as exc:  # noqa: BLE001
            log.warning("%s funding: %s", self.venue, exc)

    async def snapshot(self) -> dict[str, Quote]:
        v = self.venue
        try:
            if v in ("binance", "aster"):
                await self._refresh_funding()
                return parse_binance_like(await self._get("/fapi/v1/ticker/bookTicker"), self.listed, self.funding)
            if v == "okx":
                await self._refresh_funding()
                return parse_okx((await self._get("/api/v5/market/tickers?instType=SWAP")).get("data", []),
                                 self.listed, self.ct_val, self.funding)
            if v == "bybit":
                return parse_bybit((await self._get("/v5/market/tickers?category=linear")).get("result", {}).get("list", []),
                                   self.listed)
            if v == "hyperliquid":
                return parse_hyperliquid(await self._post({"type": "metaAndAssetCtxs"}))
            if v == "ondo":
                return parse_ondo((await self._get("/v1/perps/contracts?sparkline=false")).get("result", []))
        except Exception as exc:  # noqa: BLE001 - one failed request = one missing sample, never a crash
            self.errors += 1
            if self.errors <= 5 or self.errors % 100 == 0:
                log.warning("%s snapshot failed (%d): %s", v, self.errors, exc)
        return {}

    async def fetch_depth(self, base: str, band_pct: float) -> Optional[tuple[float, float]]:
        """One order-book request for one symbol: (bid USD, ask USD) within band_pct of the mid."""
        v, n = self.venue, self.native.get(base)
        if n is None:
            return None
        if v in ("binance", "aster"):
            d = await self._get(f"/fapi/v1/depth?symbol={n}&limit=50")
            return book_depth(d.get("bids"), d.get("asks"), band_pct)
        if v == "okx":
            d = ((await self._get(f"/api/v5/market/books?instId={n}&sz=50")).get("data") or [{}])[0]
            return book_depth(d.get("bids"), d.get("asks"), band_pct, self.ct_val.get(n, 1.0))
        if v == "bybit":
            d = (await self._get(f"/v5/market/orderbook?category=linear&symbol={n}&limit=50")).get("result") or {}
            return book_depth(d.get("b"), d.get("a"), band_pct)
        if v == "hyperliquid":
            lv = (await self._post({"type": "l2Book", "coin": n})).get("levels") or [[], []]
            conv = lambda side: [[x["px"], x["sz"]] for x in side]  # noqa: E731
            return book_depth(conv(lv[0]), conv(lv[1]), band_pct)
        if v == "ondo":
            d = (await self._get(f"/v1/perps/depth?market={n}")).get("result") or {}
            return book_depth(d.get("bids"), d.get("asks"), band_pct)
        return None

    async def depth_loop(self, symbols: list[str], interval_s: float, band_pct: float) -> None:
        """Poll the order books of `symbols` one after another, spread evenly over interval_s."""
        syms = [b for b in symbols if b in self.native]
        if not syms:
            return
        gap = interval_s / len(syms)
        while True:
            for b in syms:
                t0 = time.time()
                try:
                    r = await self.fetch_depth(b, band_pct)
                    if r is not None:
                        self.depth[b] = (r[0], r[1], time.time())
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - a missed poll leaves the depth blank, never a crash
                    self.depth_errors += 1
                    if self.depth_errors <= 5 or self.depth_errors % 100 == 0:
                        log.warning("%s depth %s failed (%d): %s", self.venue, b, self.depth_errors, exc)
                await asyncio.sleep(max(0.2, gap - (time.time() - t0)))

    def depth_for(self, base: str, now: float, max_age_s: float) -> tuple[Optional[float], Optional[float]]:
        d = self.depth.get(base)
        if d is None or now - d[2] > max_age_s:
            return None, None
        return d[0], d[1]


# ---------------------------------------------------------------- small venues (our WebSocket adapters)
def depth_usd(levels, mid: float, band_pct: float) -> float:
    lim = mid * band_pct / 100
    return sum(float(lv.price) * float(lv.size) for lv in levels if abs(float(lv.price) - mid) <= lim)


def small_rows(ts: float, venue, names: dict[str, str], band_pct: float) -> list[list]:
    """names: base -> venue-native name. One row per fresh, valid book."""
    rows = []
    for base, native in names.items():
        b = venue.book(native)
        if b is None or not b.is_valid():
            continue
        mid = float(b.mid)
        fh = venue.funding_hourly.get(native)
        rows.append([round(ts, 1), venue.name, base, float(b.best_bid.price), float(b.best_ask.price),
                     float(b.best_bid.size), float(b.best_ask.size), round(depth_usd(b.bids, mid, band_pct)),
                     round(depth_usd(b.asks, mid, band_pct)), fh, 1.0 if fh is not None else None,
                     round(max(0.0, venue.quote_age(native, ts)), 1)])
    return rows


# ---------------------------------------------------------------- per-day meta file (v5.9.3)
async def small_info(session, vcfg) -> dict[str, MarketInfo]:
    """Per-market fee facts a small venue publishes: Arcus fees per market, Entropy growth mode. Lighter's fee
    depends on the account tier (Standard 0 / Premium), which a key-less monitor cannot read: config."""
    url = vcfg.rest_url.rstrip("/")
    if vcfg.kind == "arcus":
        async with session.get(url + "/v1/markets", allow_redirects=False) as r:
            data = await r.json(content_type=None)
        rows = data.get("markets", data)
        return info_arcus(list(rows.values()) if isinstance(rows, dict) else rows)
    if vcfg.kind == "hyperliquid":
        async with session.post(url + "/info", json={"type": "meta", "dex": vcfg.dex}, allow_redirects=False) as r:
            return info_entropy((await r.json(content_type=None)).get("universe", []))
    return {}


def meta_path(out_dir, ts: float) -> Path:
    return Path(out_dir) / (datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d") + ".meta.json")


def build_meta(cfg: RefmonConfig, symbols: dict[str, set], infos: dict[str, dict]) -> dict:
    """{venue: {symbol: {maker, taker, fee_src, category, min_notional}}} with the fees refrank will use."""
    out: dict = {}
    for v, bases in symbols.items():
        for b in sorted(bases):
            info = (infos.get(v) or {}).get(b)
            m, t, src = cfg.fee_src(v, b, info)
            out.setdefault(v, {})[b] = dict(maker=round(m, 6), taker=round(t, 6), fee_src=src,
                                            category=info.category if info and info.category else _cat(b),
                                            min_notional=info.min_notional if info else None)
    return out


def write_meta(out_dir, ts: float, venues: dict, depth_symbols: list, band_pct: float) -> Path:
    p = meta_path(out_dir, ts)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(dict(ts=round(ts, 1), depth_band_pct=band_pct, ref_depth_symbols=depth_symbols,
                                   venues=venues), ensure_ascii=False, sort_keys=True))
    tmp.replace(p)
    return p


def load_meta(out_dir, days: Optional[int] = None) -> dict:
    """Newest meta per (venue, symbol) over the analysed day files: {venue: {symbol: {...}}}."""
    files = sorted(Path(out_dir).glob("*.meta.json"))
    if days:
        files = files[-days:]
    out: dict = {}
    for p in files:  # oldest first: newer days overwrite
        try:
            for v, rows in (json.loads(p.read_text()).get("venues") or {}).items():
                out.setdefault(v, {}).update(rows)
        except (OSError, ValueError) as exc:
            log.warning("meta %s unreadable: %s", p, exc)
    return out


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.10g}"
    return str(v)


class Writer:
    def __init__(self, out_dir: Path):
        self.dir = out_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.buf: list[list] = []

    def add(self, rows: list[list]) -> None:
        self.buf.extend(rows)

    def flush(self) -> int:
        if not self.buf:
            return 0
        by_day: dict[str, list] = defaultdict(list)
        for r in self.buf:
            by_day[datetime.fromtimestamp(r[0], timezone.utc).strftime("%Y-%m-%d")].append(r)
        n = len(self.buf)
        for day, rows in by_day.items():
            p = self.dir / f"{day}.csv.gz"
            new = not p.exists()
            with gzip.open(p, "at", newline="") as fh:  # appended gzip members read back as one stream
                w = csv.writer(fh)
                if new:
                    w.writerow(COLS)
                w.writerows([[_fmt(x) for x in r] for r in rows])
        self.buf = []
        return n


async def run_refmon(cfg: RefmonConfig, duration: Optional[float] = None) -> int:
    from arb.markets import _fetch
    from arb.venues import build_venue
    from arb.venues.transport import http_session

    vcfgs = parse_venues({"venues": cfg.venues})
    shim = types.SimpleNamespace(venues=vcfgs)
    include = {s.upper() for s in cfg.include}
    exclude = {s.upper() for s in cfg.exclude}
    async with http_session(15) as s:
        small_listing = {}
        for n in cfg.small_venues:
            try:
                small_listing[n] = {base: native for base, native in (await _fetch(s, shim, n)).items()}
            except Exception as exc:  # noqa: BLE001
                log.warning("%s market list failed: %s", n, exc)
        refs = {n: RefSource(n, s, cfg.funding_refresh_s) for n in cfg.ref_venues}
        ref_bases: dict[str, set] = {}
        for n, src in refs.items():
            try:
                ref_bases[n] = await src.load_markets()
            except Exception as exc:  # noqa: BLE001
                log.warning("%s market list failed: %s", n, exc)
                ref_bases[n] = set()
        all_ref = set().union(*ref_bases.values()) if ref_bases else set()
        wanted: dict[str, dict[str, str]] = {}
        for n, lst in small_listing.items():
            keep = {b: nat for b, nat in lst.items() if b in all_ref and (not include or b in include) and b not in exclude}
            if keep:
                wanted[n] = keep
        total = sorted({b for m in wanted.values() for b in m})
        print(f"refmon: {len(total)} symbols on a small and a reference venue; small venues "
              + ", ".join(f"{n} {len(m)}" for n, m in wanted.items()) + "; reference venues "
              + ", ".join(f"{n} {len(ref_bases.get(n, set()) & set(total))}" for n in refs))
        depth_syms = sorted({str(x).upper() for x in cfg.ref_depth_symbols} & set(total))
        if cfg.ref_depth_symbols:
            print(f"refmon: reference-venue depth every {cfg.ref_depth_interval_s:g}s for {', '.join(depth_syms) or 'none'}"
                  + (f" (not listed: {', '.join(sorted({str(x).upper() for x in cfg.ref_depth_symbols} - set(total)))})"
                     if len(depth_syms) < len(cfg.ref_depth_symbols) else ""))

        async def refresh_meta(reload_refs: bool) -> None:
            infos: dict[str, dict] = {}
            for n in wanted:
                try:
                    infos[n] = await small_info(s, vcfgs[n])
                except Exception as exc:  # noqa: BLE001 - fees fall back to the config
                    log.warning("%s market info failed: %s", n, exc)
            for n, src in refs.items():
                if reload_refs:
                    old = (src.listed, src.native, src.info)
                    try:
                        await src.load_markets()
                    except Exception as exc:  # noqa: BLE001 - keep the previous listing
                        src.listed, src.native, src.info = old
                        log.warning("%s market refresh failed: %s", n, exc)
                infos[n] = src.info
            syms = {n: set(m) for n, m in wanted.items()}
            syms.update({n: set(src.native) & set(total) for n, src in refs.items()})
            p = write_meta(cfg.out_dir, time.time(), build_meta(cfg, syms, infos), depth_syms, cfg.depth_band_pct)
            log.info("refmon meta written: %s", p)

        venues = {}
        depth_tasks: list = []
        try:
            await refresh_meta(False)
            for n, src in refs.items():
                if depth_syms and (not cfg.ref_depth_venues or n in cfg.ref_depth_venues):
                    depth_tasks.append(asyncio.create_task(src.depth_loop(depth_syms, cfg.ref_depth_interval_s,
                                                                          cfg.depth_band_pct)))
            for n, m in wanted.items():
                v = build_venue(vcfgs[n])
                v.book_only = True  # Lighter: order books only (no market_stats), fewer WS messages per IP
                await v.start(list(m.values()), trading=False)
                venues[n] = v
            writer = Writer(Path(cfg.out_dir))
            keep_ref = set(total)
            start = time.time()
            last_flush = start
            last_meta, meta_day = start, meta_path(cfg.out_dir, start).name
            depth_max_age = 2.5 * cfg.ref_depth_interval_s
            next_tick = math.ceil(start / cfg.interval_s) * cfg.interval_s
            while duration is None or time.time() - start < duration:
                await asyncio.sleep(max(0.0, next_tick - time.time()))
                ts = next_tick
                next_tick += cfg.interval_s
                if time.time() > next_tick:  # fell behind (slow requests): skip ticks instead of bursting
                    next_tick = math.ceil(time.time() / cfg.interval_s) * cfg.interval_s
                snaps = await asyncio.gather(*[src.snapshot() for src in refs.values()])
                rows = []
                now = time.time()
                for (n, src), snap in zip(refs.items(), snaps):
                    for base, q in snap.items():
                        if base in keep_ref:
                            db, da = src.depth_for(base, now, depth_max_age)
                            age = round(max(0.0, now - q.ts), 1) if q.ts else None
                            rows.append([round(ts, 1), n, base, q.bid, q.ask, q.bid_qty, q.ask_qty, db, da,
                                         q.funding_rate, q.funding_h, age])
                for n, v in venues.items():
                    rows.extend(small_rows(ts, v, wanted[n], cfg.depth_band_pct))
                writer.add(rows)
                if time.time() - last_flush >= cfg.flush_s:
                    await asyncio.to_thread(writer.flush)
                    last_flush = time.time()
                day = meta_path(cfg.out_dir, time.time()).name
                if time.time() - last_meta >= cfg.meta_refresh_s or day != meta_day:
                    last_meta, meta_day = time.time(), day
                    try:
                        await refresh_meta(True)
                    except Exception as exc:  # noqa: BLE001 - never stop recording for the meta file
                        log.warning("refmon meta refresh failed: %s", exc)
            await asyncio.to_thread(writer.flush)
        finally:
            for t in depth_tasks:
                t.cancel()
            for v in venues.values():
                try:
                    await v.stop()
                except Exception as exc:  # noqa: BLE001
                    log.warning("stop %s: %s", v.name, exc)
    return 0


# ---------------------------------------------------------------- analysis
def load_rows(out_dir: Path, days: Optional[int] = None, symbols: Optional[set] = None):
    """{(venue, symbol): [(ts, bid, ask, depth_bid, depth_ask, age)]} from the newest `days` files."""
    files = sorted(Path(out_dir).glob("*.csv.gz"))
    if days:
        files = files[-days:]
    series: dict[tuple, list] = defaultdict(list)
    for p in files:
        with gzip.open(p, "rt", newline="") as fh:
            for r in csv.DictReader(fh):
                if r.get("ts") == "ts":
                    continue
                if symbols and r["symbol"] not in symbols:
                    continue
                bid, ask = _f(r["bid"]), _f(r["ask"])
                if not bid or not ask or ask < bid:
                    continue
                series[(r["venue"], r["symbol"])].append((float(r["ts"]), bid, ask, _f(r["depth_bid_usd"]),
                                                          _f(r["depth_ask_usd"]), _f(r["age_s"]) or 0.0))
    return series, len(files)


def _depth(db: Optional[float], da: Optional[float]) -> Optional[float]:
    """Thinner side of the book (USD within the band), None when the row has no depth."""
    if db is None and da is None:
        return None
    return min(db or 0.0, da or 0.0)


def analyze_pair(small: list, ref: list, fee_small: tuple, fee_ref: tuple, interval_s: float = 5.0,
                 min_edge: float = 0.02, max_age_s: float = 10.0, ema_s: float = 3600.0,
                 min_depth: float = 0.0, both_take: bool = False) -> Optional[dict]:
    """One (small venue, other venue, symbol): basis statistics and opportunities.

    basis = small mid / other mid - 1 (percent). The bot trades the deviation from a slow EMA of the basis, so an
    opportunity is the executable edge beyond that EMA: sell on the small venue at its ask (maker) and buy the
    other venue at its ask (taker), or the opposite. Round-trip cost = 2 x (small maker + other taker).
    both_take (v5.9.3): both legs cross the spread instead (small sells at its bid / buys at its ask, small taker
    fee) - no maker-fill assumption, the honest lower figure. An episode opens when the edge reaches cost +
    min_edge (the bot's trigger) and closes when the deviation is back to 0 (where the bot closes); it earns the
    entry edge minus the cost. min_depth (v5.9.3): both books need at least this many USD within the band on
    their thinner side where depth is recorded (reference venues: only `ref_depth_symbols`)."""
    ref_by_ts = {round(t / interval_s): (b, a, _depth(db, da), age) for t, b, a, db, da, age in ref}
    pts = []
    for t, b, a, db, da, age in small:
        r = ref_by_ts.get(round(t / interval_s))
        if r is None or age > max_age_s or r[3] > max_age_s:
            continue
        pts.append((t, b, a, r[0], r[1], _depth(db, da), r[2]))
    if len(pts) < 100:
        return None
    pts.sort()
    basis = [((b + a) / 2 / ((rb + ra) / 2) - 1) * 100 for _, b, a, rb, ra, *_ in pts]
    med = statistics.median(basis)
    if abs(med) > 3.0:  # different contract units (1000x, rebased pre-IPO) or a different asset
        return dict(n=len(pts), unit_mismatch=True, basis_med=med)
    cost = 2 * ((fee_small[1] if both_take else fee_small[0]) + fee_ref[1])
    ema = basis[0]
    alpha_base = interval_s / ema_s
    episodes, cur = [], None
    shallow = 0
    for (t, b, a, rb, ra, ds, dr), bs in zip(pts, basis):
        ema += alpha_base * (bs - ema)
        sp, bp = (b, a) if both_take else (a, b)  # price we sell / buy at on the small venue
        sell = ((sp - ra) / ra * 100) - ema  # sell small, buy the other venue at its ask
        buy = ema - ((bp - rb) / rb * 100)  # buy small, sell the other venue at its bid
        edge = max(sell, buy)
        if cur is None:
            if edge >= cost + min_edge:
                if min_depth and any(d is not None and d < min_depth for d in (ds, dr)):
                    shallow += 1
                    continue
                cur = dict(start=t, entry=edge, side=1 if sell >= buy else -1)
        elif (bs - ema) * cur["side"] <= 0:
            episodes.append((cur["entry"] - cost, t - cur["start"]))
            cur = None
    span_days = max((pts[-1][0] - pts[0][0]) / 86400, 1e-9)
    s = sorted(basis)
    q = lambda p: s[int(p * (len(s) - 1))]  # noqa: E731
    hourly: dict[int, list] = defaultdict(list)
    for (t, *_), bs in zip(pts, basis):
        hourly[int(t // 3600)].append(bs)
    hm = [statistics.median(v) for _, v in sorted(hourly.items()) if len(v) >= 10]
    nets = sorted(e[0] for e in episodes)
    secs = sorted(e[1] for e in episodes)
    dsm = [p[5] for p in pts if p[5] is not None]
    drm = [p[6] for p in pts if p[6] is not None]
    return dict(n=len(pts), unit_mismatch=False, days=span_days, basis_med=med, basis_p5=q(0.05), basis_p95=q(0.95),
                drift=(max(hm) - min(hm)) if hm else 0.0, cost=cost, episodes_day=len(episodes) / span_days,
                net_med=nets[len(nets) // 2] if nets else 0.0, revert_med_s=secs[len(secs) // 2] if secs else None,
                net_sum_day=sum(nets) / span_days, depth_med=statistics.median(dsm) if dsm else 0.0,
                ref_depth_med=statistics.median(drm) if drm else None, shallow_day=shallow / span_days)


def make_fee_lookup(cfg: RefmonConfig, meta: Optional[dict] = None):
    """(venue, symbol) -> (maker %, taker %): the recorded meta file first, then the config."""
    meta = meta or {}

    def fee(venue: str, symbol: str) -> tuple[float, float]:
        ov = (cfg.fee_overrides.get(venue) or {}).get(symbol)
        row = (meta.get(venue) or {}).get(symbol)
        if row and not ov and row.get("maker") is not None and row.get("taker") is not None:
            return float(row["maker"]), float(row["taker"])
        return cfg.fee(venue, symbol)
    return fee


def rank(series: dict, cfg: RefmonConfig, small_only: Optional[str] = None, ref_only: Optional[str] = None,
         meta: Optional[dict] = None, min_depth: float = 0.0, both_take: bool = False) -> list[dict]:
    """Every small venue against every reference venue and (v5.9.3) every other small venue (each pair once)."""
    out = []
    fee = make_fee_lookup(cfg, meta)
    symbols = {s for _, s in series}
    others = list(cfg.ref_venues) + list(cfg.small_venues)
    for sym in symbols:
        for i, sv in enumerate(cfg.small_venues):
            if small_only and sv != small_only or (sv, sym) not in series:
                continue
            for rv in others:
                if rv == sv or (both_take and not small_only and rv in cfg.small_venues
                                and cfg.small_venues.index(rv) < i):
                    continue  # both_take is symmetric: small-vs-small pairs once; maker mode: each side quotes once
                if ref_only and rv != ref_only or (rv, sym) not in series:
                    continue
                res = analyze_pair(series[(sv, sym)], series[(rv, sym)], fee(sv, sym), fee(rv, sym), cfg.interval_s,
                                   cfg.min_edge_pct, min_depth=min_depth, both_take=both_take)
                if res:
                    out.append(dict(symbol=sym, small=sv, ref=rv, **res))
    return out


def print_rank(cfg: RefmonConfig, days: Optional[int], top: int, small_only: Optional[str], ref_only: Optional[str],
               symbols: Optional[list], min_depth: float = 0.0, both_take: bool = False) -> int:
    series, nfiles = load_rows(Path(cfg.out_dir), days, {s.upper() for s in symbols} if symbols else None)
    if not series:
        print(f"no refmon data in {cfg.out_dir} (run `arb refmon` first)")
        return 1
    meta = load_meta(cfg.out_dir)  # newest known fees for every symbol, also for days recorded before v5.9.3
    rows = rank(series, cfg, small_only, ref_only, meta, min_depth, both_take)
    bad = [r for r in rows if r["unit_mismatch"]]
    good = sorted((r for r in rows if not r["unit_mismatch"]), key=lambda r: r["net_sum_day"], reverse=True)
    print(f"{nfiles} day file(s), {len(good)} combinations (+{len(bad)} skipped: median basis > 3%, units differ); "
          f"fees: {'meta file + config' if meta else 'config only (no meta file yet)'}")
    if both_take:
        print("edge = both legs cross the spread (no maker-fill assumption) beyond the 1h EMA basis; "
              "cost = 2 x (small taker + other taker) fees;")
    else:
        print("edge = small venue maker + other venue taker beyond the 1h EMA basis; cost = 2 x (small maker + other "
              "taker) fees;")
    print(f"an episode = edge >= cost + {cfg.min_edge_pct:g}% until the deviation is back to 0, earning entry edge - cost."
          + (f"\nmin depth {min_depth:g} USD within {cfg.depth_band_pct:g}% on both books where recorded "
             "(refDep '-' = reference depth not recorded for this symbol)." if min_depth else "")
          + ("\nUpper bound: every maker quote is assumed filled." if not both_take else
             "\nStill an upper bound: 5s samples, no order latency (Lighter Standard taker ~300 ms), no price impact.")
          + "\n")
    hdr = (f"{'symbol':<11}{'small':<11}{'other':<12}{'basis%':>8}{'p5..p95':>15}{'drift':>7}{'cost':>7}"
           f"{'ep/day':>8}{'net bp':>8}{'revert':>8}{'%/day':>8}{'depthU':>9}{'refDep':>9}")
    print(hdr)
    for r in good[:top]:
        rv = f"{r['revert_med_s']:.0f}s" if r["revert_med_s"] is not None else "-"
        rd = f"{r['ref_depth_med']:.0f}" if r.get("ref_depth_med") is not None else "-"
        print(f"{r['symbol']:<11}{r['small']:<11}{r['ref']:<12}{r['basis_med']:>+8.3f}"
              f"{r['basis_p5']:>+7.3f}..{r['basis_p95']:<+6.3f}{r['drift']:>7.3f}{r['cost']:>7.3f}"
              f"{r['episodes_day']:>8.1f}{r['net_med'] * 100:>8.1f}{rv:>8}{r['net_sum_day']:>8.2f}{r['depth_med']:>9.0f}"
              f"{rd:>9}")
    print("\n%/day = sum of net edge per day in percent of one trade's notional (x notional/100 = USDC/day, upper bound)")
    return 0
