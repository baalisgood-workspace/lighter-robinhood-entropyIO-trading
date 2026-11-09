"""v5.9 `arb refmon` / `arb refrank`: read-only reference monitor (no keys, never trades).

Records, every `interval_s`, the best bid/ask of every symbol that is listed on at least one of our small venues
(Lighter, Lighter RH, Entropy, Arcus - their public WebSocket books, same adapters as `monitor`) and on at least
one big reference venue (Binance, OKX, Bybit, Hyperliquid, Aster, Ondo Perps - one public REST "all tickers"
request per venue and interval). One gzip CSV per UTC day under `out_dir`:

    ts, venue, symbol, bid, ask, bid_qty, ask_qty, depth_bid_usd, depth_ask_usd, funding_rate, funding_h, age_s

- symbol: common base name (BTC, NVDA, OPENAI, 1000PEPE ...); venue-native names are mapped by `normalize`.
- bid_qty / ask_qty: base units at the best price (blank when the venue does not report them: Hyperliquid, Ondo).
- depth_*_usd: small venues only, notional within depth_band_pct of the mid.
- funding_rate: as reported for one funding interval of funding_h hours (blank when unknown).
- Hyperliquid has no bid/ask in its one-request snapshot: the impact prices (`impactPxs`, prices to trade the
  venue's impact notional) are recorded as bid/ask - a conservative executable price.

`arb refrank` reads those files and ranks (small venue, reference venue, symbol) combinations: how far the small
venue's price deviates from the reference, how often the deviation exceeds the round-trip fees, how fast it comes
back and how deep the small venue's book is. It is an upper bound: it does not know whether a maker order would
have been filled.
"""
from __future__ import annotations

import asyncio
import csv
import gzip
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

# retail fee schedules (percent), override in refmon.yaml `fees:` with your own tiers
DEFAULT_FEES = {
    "lighter": (0.0, 0.0), "lighter_rh": (0.0, 0.0), "entropy": (0.003, 0.009), "arcus": (0.0, 0.0225),
    "binance": (0.02, 0.05), "okx": (0.02, 0.05), "bybit": (0.02, 0.055), "hyperliquid": (0.015, 0.045),
    "aster": (0.01, 0.035), "ondo": (0.01, 0.025),
}
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
    fees: dict = field(default_factory=dict)  # {venue: {maker: pct, taker: pct}}
    venues: dict = field(default_factory=dict)  # small-venue overrides, as in config.yaml `venues:`

    def fee(self, venue: str) -> tuple[float, float]:
        f = self.fees.get(venue) or {}
        m, t = DEFAULT_FEES.get(venue, (0.0, 0.0))
        return float(f.get("maker", m)), float(f.get("taker", t))


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
            out[base] = Quote(bid, ask, _f(r.get("bidQty")), _f(r.get("askQty")), fr, fh)
    return out


def parse_okx(rows: list, listed: dict[str, str], ct_val: dict[str, float]) -> dict[str, Quote]:
    out = {}
    for r in rows:
        inst = r.get("instId")
        base = listed.get(inst)
        bid, ask = _f(r.get("bidPx")), _f(r.get("askPx"))
        if base and bid and ask:
            cv = ct_val.get(inst, 1.0)
            bq, aq = _f(r.get("bidSz")), _f(r.get("askSz"))
            out[base] = Quote(bid, ask, bq * cv if bq is not None else None, aq * cv if aq is not None else None)
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


class RefSource:
    """One reference venue: market list at start, one snapshot request per interval, funding every few minutes."""

    def __init__(self, venue: str, session, funding_refresh_s: float):
        self.venue = venue
        self.url = URLS[venue]
        self.s = session
        self.listed: dict[str, str] = {}  # native -> base
        self.ct_val: dict[str, float] = {}
        self.funding: dict[str, tuple] = {}
        self.funding_ts = 0.0
        self.funding_refresh_s = funding_refresh_s
        self.errors = 0

    async def _get(self, path: str):
        async with self.s.get(self.url + path, allow_redirects=False) as r:
            r.raise_for_status()
            return await r.json(content_type=None)

    async def _post(self, body: dict):
        async with self.s.post(self.url + "/info", json=body, allow_redirects=False) as r:
            r.raise_for_status()
            return await r.json(content_type=None)

    async def load_markets(self) -> set[str]:
        v = self.venue
        if v in ("binance", "aster"):
            d = await self._get("/fapi/v1/exchangeInfo")
            for s in d.get("symbols", []):
                if s.get("status") == "TRADING" and s.get("contractType") in ("PERPETUAL", "TRADIFI_PERPETUAL"):
                    base = normalize(v, s["symbol"])
                    if base:
                        self.listed[s["symbol"]] = base
        elif v == "okx":
            d = await self._get("/api/v5/public/instruments?instType=SWAP")
            for s in d.get("data", []):
                if s.get("state") == "live" and s.get("settleCcy") == "USDT" and s.get("ctType") == "linear":
                    base = normalize(v, s["instId"])
                    if base:
                        self.listed[s["instId"]] = base
                        self.ct_val[s["instId"]] = _f(s.get("ctVal")) or 1.0
        elif v == "bybit":
            d = await self._get("/v5/market/instruments-info?category=linear&limit=1000")
            for s in d.get("result", {}).get("list", []):
                if s.get("status") == "Trading" and s.get("contractType") == "LinearPerpetual" and s.get("quoteCoin") == "USDT":
                    base = normalize(v, s["symbol"])
                    if base:
                        self.listed[s["symbol"]] = base
        elif v == "hyperliquid":
            return set(parse_hyperliquid(await self._post({"type": "metaAndAssetCtxs"})))
        elif v == "ondo":
            return set(parse_ondo((await self._get("/v1/perps/contracts?sparkline=false")).get("result", [])))
        return set(self.listed.values())

    async def _refresh_funding(self) -> None:
        if self.venue not in ("binance", "aster") or time.time() - self.funding_ts < self.funding_refresh_s:
            return
        self.funding_ts = time.time()
        try:
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
                return parse_okx((await self._get("/api/v5/market/tickers?instType=SWAP")).get("data", []),
                                 self.listed, self.ct_val)
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
        venues = {}
        try:
            for n, m in wanted.items():
                v = build_venue(vcfgs[n])
                v.book_only = True  # Lighter: order books only (no market_stats), fewer WS messages per IP
                await v.start(list(m.values()), trading=False)
                venues[n] = v
            writer = Writer(Path(cfg.out_dir))
            keep_ref = set(total)
            start = time.time()
            last_flush = start
            next_tick = math.ceil(start / cfg.interval_s) * cfg.interval_s
            while duration is None or time.time() - start < duration:
                await asyncio.sleep(max(0.0, next_tick - time.time()))
                ts = next_tick
                next_tick += cfg.interval_s
                if time.time() > next_tick:  # fell behind (slow requests): skip ticks instead of bursting
                    next_tick = math.ceil(time.time() / cfg.interval_s) * cfg.interval_s
                snaps = await asyncio.gather(*[src.snapshot() for src in refs.values()])
                rows = []
                for n, snap in zip(refs, snaps):
                    for base, q in snap.items():
                        if base in keep_ref:
                            rows.append([round(ts, 1), n, base, q.bid, q.ask, q.bid_qty, q.ask_qty, None, None,
                                         q.funding_rate, q.funding_h, 0.0])
                for n, v in venues.items():
                    rows.extend(small_rows(ts, v, wanted[n], cfg.depth_band_pct))
                writer.add(rows)
                if time.time() - last_flush >= cfg.flush_s:
                    await asyncio.to_thread(writer.flush)
                    last_flush = time.time()
            await asyncio.to_thread(writer.flush)
        finally:
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


def analyze_pair(small: list, ref: list, fee_small: tuple, fee_ref: tuple, interval_s: float = 5.0,
                 min_edge: float = 0.02, max_age_s: float = 10.0, ema_s: float = 3600.0) -> Optional[dict]:
    """One (small venue, reference venue, symbol): basis statistics and maker(small)/taker(ref) opportunities.

    basis = small mid / ref mid - 1 (percent). The bot trades the deviation from a slow EMA of the basis, so an
    opportunity is the executable edge beyond that EMA: sell on the small venue at its ask (maker) and buy the
    reference at its ask (taker), or the opposite. Round-trip cost = 2 x (small maker + ref taker). An episode
    opens when the edge reaches cost + min_edge (the bot's trigger) and closes when the deviation is back to 0
    (where the bot closes); it earns the entry edge minus the cost."""
    ref_by_ts = {round(t / interval_s): (b, a) for t, b, a, *_ in ref}
    pts = []
    for t, b, a, db, da, age in small:
        r = ref_by_ts.get(round(t / interval_s))
        if r is None or age > max_age_s:
            continue
        pts.append((t, b, a, r[0], r[1], min(db or 0.0, da or 0.0)))
    if len(pts) < 100:
        return None
    pts.sort()
    basis = [((b + a) / 2 / ((rb + ra) / 2) - 1) * 100 for _, b, a, rb, ra, _ in pts]
    med = statistics.median(basis)
    if abs(med) > 3.0:  # different contract units (1000x, rebased pre-IPO) or a different asset
        return dict(n=len(pts), unit_mismatch=True, basis_med=med)
    cost = 2 * (fee_small[0] + fee_ref[1])
    ema = basis[0]
    alpha_base = interval_s / ema_s
    episodes, cur = [], None
    for (t, b, a, rb, ra, _), bs in zip(pts, basis):
        ema += alpha_base * (bs - ema)
        sell = ((a - ra) / ra * 100) - ema  # sell small at its ask, buy ref at its ask
        buy = ema - ((b - rb) / rb * 100)  # buy small at its bid, sell ref at its bid
        edge = max(sell, buy)
        if cur is None:
            if edge >= cost + min_edge:
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
    return dict(n=len(pts), unit_mismatch=False, days=span_days, basis_med=med, basis_p5=q(0.05), basis_p95=q(0.95),
                drift=(max(hm) - min(hm)) if hm else 0.0, cost=cost, episodes_day=len(episodes) / span_days,
                net_med=nets[len(nets) // 2] if nets else 0.0, revert_med_s=secs[len(secs) // 2] if secs else None,
                net_sum_day=sum(nets) / span_days, depth_med=statistics.median(p[5] for p in pts))


def rank(series: dict, cfg: RefmonConfig, small_only: Optional[str] = None, ref_only: Optional[str] = None) -> list[dict]:
    out = []
    symbols = {s for _, s in series}
    for sym in symbols:
        for sv in cfg.small_venues:
            if small_only and sv != small_only or (sv, sym) not in series:
                continue
            for rv in cfg.ref_venues:
                if ref_only and rv != ref_only or (rv, sym) not in series:
                    continue
                res = analyze_pair(series[(sv, sym)], series[(rv, sym)], cfg.fee(sv), cfg.fee(rv), cfg.interval_s,
                                   cfg.min_edge_pct)
                if res:
                    out.append(dict(symbol=sym, small=sv, ref=rv, **res))
    return out


def print_rank(cfg: RefmonConfig, days: Optional[int], top: int, small_only: Optional[str], ref_only: Optional[str],
               symbols: Optional[list]) -> int:
    series, nfiles = load_rows(Path(cfg.out_dir), days, {s.upper() for s in symbols} if symbols else None)
    if not series:
        print(f"no refmon data in {cfg.out_dir} (run `arb refmon` first)")
        return 1
    rows = rank(series, cfg, small_only, ref_only)
    bad = [r for r in rows if r["unit_mismatch"]]
    good = sorted((r for r in rows if not r["unit_mismatch"]), key=lambda r: r["net_sum_day"], reverse=True)
    print(f"{nfiles} day file(s), {len(good)} combinations (+{len(bad)} skipped: median basis > 3%, units differ)")
    print("edge = small venue maker + reference taker beyond the 1h EMA basis; cost = 2 x (small maker + ref taker) fees;\n"
          f"an episode = edge >= cost + {cfg.min_edge_pct:g}% until the deviation is back to 0, earning entry edge - cost.\n"
          "Upper bound: every maker quote is assumed filled.\n")
    hdr = (f"{'symbol':<11}{'small':<11}{'ref':<12}{'basis%':>8}{'p5..p95':>15}{'drift':>7}{'cost':>7}"
           f"{'ep/day':>8}{'net bp':>8}{'revert':>8}{'%/day':>8}{'depthU':>9}")
    print(hdr)
    for r in good[:top]:
        rv = f"{r['revert_med_s']:.0f}s" if r["revert_med_s"] is not None else "-"
        print(f"{r['symbol']:<11}{r['small']:<11}{r['ref']:<12}{r['basis_med']:>+8.3f}"
              f"{r['basis_p5']:>+7.3f}..{r['basis_p95']:<+6.3f}{r['drift']:>7.3f}{r['cost']:>7.3f}"
              f"{r['episodes_day']:>8.1f}{r['net_med'] * 100:>8.1f}{rv:>8}{r['net_sum_day']:>8.2f}{r['depth_med']:>9.0f}")
    print("\n%/day = sum of net edge per day in percent of one trade's notional (x notional/100 = USDC/day, upper bound)")
    return 0
