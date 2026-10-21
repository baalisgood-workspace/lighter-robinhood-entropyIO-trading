"""v5 auto symbols: add every symbol that >= min_venues venues list, with a per-category parameter template.

Classification (first match wins): auto_symbols.categories (yours) > built-in lists below > Arcus market
category (EQUITIES / INDICES / COMMODITIES = US-listed stock or ETF -> us_equity, CRYPTO -> crypto) > other.
`other` (pre-IPO, non-US stocks, FX, metals, energy, rates, unknown) gets the most conservative template.

Explicitly configured symbols are never changed. Auto symbols only open positions as allowed by
auto_symbols.trade (default none = monitor only); see engine Ctx.policy.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import time
import types
from pathlib import Path
from typing import Optional

from arb.config import VENUE_NAMES, _deep_merge, parse_venues

log = logging.getLogger("arb.autosym")

US_EQUITY = set("""
AAOI AAPL AMC AMD AMZN ARM ASML ASTS AVGO AXTI BABA BAC BB BE BIO BMNR BOT CCL CLSK COIN CPER CRCL CRWV DELL DRAM
EWY F GEV GLD GME GOOGL HOOD IBM INTC IREN IWM KORU LITE LUNR META MRNA MRVL MSFT MSTR MU NBIS NOK NOW NVDA ORCL
PLTR QBTS QCOM QNT QQQ RGTI RKLB RVI SGOV SKHY SLV SMCI SNDK SOFI SOXL SOXS SPCX SPY STRC TSLA TSM TTWO USAR USO
VT WDC WULF
""".split())

# pre-IPO / non-US stocks / FX / metals / energy / rates / index CFDs: no single US session, thin or synthetic prices
OTHER = set("""
OPENAI ANTHROPIC SHEIN ZHIPU MINIMAX UNITREE CXMT KIOXIA VARIATIONAL POPMART TENCENT XIAOMI BYD SMIC SAMSUNGUSD
SKHYNIXUSD HYUNDAIUSD XAU XAG XCU XPD XPT WTI BRENTOIL NATGAS EURUSD GBPUSD AUDUSD NZDUSD USDCAD USDCHF USDHKD
USDJPY USDKRW US10Y US100 US500 TUR30 H100
""".split())

CRYPTO = set("""
0G 1000BONK 1000FLOKI 1000PEPE 1000SHIB 2Z AAVE ADA AERO AI APEX APT ARB ARC ASTER AVAX AVNT AXS AZTEC BCH BERA BNB
BTC CASHCAT CRO CRV DASH DATA DOGE DOT DYDX EDGE EIGEN ENA ETH ETHFI FARTCOIN FF FIL GMX GRAM GRASS HBAR HYPE ICP JTO
JUP KAITO LDO LINEA LINK LIT LTC MET MNT MON MORPHO NEAR NMR ONDO OP OURA PAXG PENDLE PENGU POL PONS POPCAT PROVE
PUMP PYTH RAY RIVER ROBO S SEI SKR SKY SOL SPX STABLE STBL STRK SUI SYRUP TAO TIA TRUMP TRX UNI USELESS VIRTUAL VVV
WEN WIF WLD WLFI XLM XMR XPL XRP ZEC ZK ZORA ZRO
""".split())

DEFAULT_TEMPLATES = {
    # US stocks/ETFs: normal threshold in NY regular hours, doubled outside (nights, weekends, holidays)
    "us_equity": {"strategy": {"session": "us_equity", "initial_threshold_pct": 0.30, "grid_step_pct": 0.15,
                               "offhours_threshold_pct": 0.60}},
    "crypto": {"strategy": {"initial_threshold_pct": 0.20, "grid_step_pct": 0.10}},
    "other": {"strategy": {"initial_threshold_pct": 0.40, "grid_step_pct": 0.20}},
}


def classify(base: str, arcus_category: Optional[str] = None, extra: Optional[dict] = None) -> str:
    for cat in ("us_equity", "crypto", "other"):
        if base in {str(x).upper() for x in (extra or {}).get(cat, []) or []}:
            return cat
    if base in OTHER:
        return "other"
    if base in US_EQUITY:
        return "us_equity"
    if base in CRYPTO:
        return "crypto"
    if arcus_category in ("EQUITIES", "INDICES", "COMMODITIES"):
        return "us_equity"
    if arcus_category == "CRYPTO":
        return "crypto"
    return "other"


def expand(raw: dict, listing: dict[str, dict[str, str]], meta: Optional[dict] = None,
           only_venues: Optional[list[str]] = None) -> tuple[dict, list[str]]:
    """Return (raw config with auto symbols added, added keys). `listing` = {venue: {base: market name}}."""
    raw = copy.deepcopy(raw)
    a = raw.get("auto_symbols") or {}
    if raw.get("symbols") == "auto":
        raw["symbols"] = {}
    symbols = raw.setdefault("symbols", {}) or {}
    raw["symbols"] = symbols
    min_venues = int(a.get("min_venues", 2))
    include = [str(x).upper() for x in a.get("include") or []]
    exclude = {str(x).upper() for x in a.get("exclude") or []}
    templates = _deep_merge(DEFAULT_TEMPLATES, a.get("templates") or {})
    names = [n for n in VENUE_NAMES if n in listing and (only_venues is None or n in only_venues)]
    configured_markets = {(v, m) for s in symbols.values() if isinstance(s, dict) for v, m in (s.get("venues") or {}).items()}
    bases = sorted({b for n in names for b in listing[n]})
    bases = [b for b in include if b in bases] + [b for b in bases if b not in include]
    added: list[str] = []
    cap = a.get("max_symbols")
    for b in bases:
        if b in symbols or b.upper() in exclude:
            continue
        vmap = {n: listing[n][b] for n in names if b in listing[n] and (n, listing[n][b]) not in configured_markets}
        if len(vmap) < min_venues:
            continue
        if cap is not None and len(added) >= int(cap):
            break
        cat = classify(b, (meta or {}).get(b), a.get("categories"))
        entry = copy.deepcopy(templates.get(cat) or {})
        entry.update({"venues": vmap, "auto": True, "category": cat})
        symbols[b] = entry
        added.append(b)
    return raw, added


def _cache_path(raw: dict) -> Path:
    return Path((raw.get("paths") or {}).get("data_dir", "data")) / "markets_cache.json"


def fetch_listing(raw: dict, timeout_s: float = 20.0) -> tuple[dict, dict, str]:
    """(listing, arcus categories, source) - live REST, falling back to data/markets_cache.json."""
    from arb.markets import fetch_all
    cfg = types.SimpleNamespace(venues=parse_venues(raw))
    meta: dict = {}
    listing: dict = {}
    try:
        listing = asyncio.run(asyncio.wait_for(fetch_all(cfg, None, meta, quiet=True), timeout_s))
    except Exception as exc:  # noqa: BLE001
        log.warning("auto_symbols: market list failed: %s", exc)
    path = _cache_path(raw)
    cached: dict = {}
    if path.exists():
        try:
            cached = json.loads(path.read_text())
        except ValueError:
            cached = {}
    missing = [n for n in VENUE_NAMES if not listing.get(n)]
    src = "live"
    for n in missing:  # a venue that failed now: use its last good list
        if (cached.get("listing") or {}).get(n):
            listing[n] = cached["listing"][n]
            src = f"live + cache({','.join(missing)})"
    meta = meta or dict(cached.get("meta") or {})
    good = {n: m for n, m in listing.items() if m}
    if good and not missing:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"ts": time.time(), "listing": good, "meta": meta}))
        except OSError:
            pass
    return listing, meta, src
