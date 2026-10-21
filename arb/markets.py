"""`arb markets`: which symbols are listed on which venue (public REST, read-only).

Ported idea from lighter-rh scripts/manage_symbols.py: before adding a symbol, check that every venue
of the pair lists an active perp market for it, and print a ready-to-paste config snippet.
"""
from __future__ import annotations

import asyncio
from typing import Optional

from arb.config import VENUE_NAMES, AppConfig

# Entropy / Hyperliquid HIP-3 names -> symbol keys used in config.yaml
ALIASES = {"OAI": "OPENAI", "ANTH": "ANTHROPIC"}


def base_of(venue: str, name: str) -> str:
    n = name
    if venue == "arcus" and n.endswith("-USD"):
        n = n[:-4]
    if venue == "entropy" and ":" in n:
        n = n.split(":", 1)[1]
    return ALIASES.get(n, n)


async def _fetch(session, cfg: AppConfig, venue: str) -> dict[str, str]:
    """{base symbol: venue market name} of active perps."""
    v = cfg.venues[venue]
    url = v.rest_url.rstrip("/")
    out: dict[str, str] = {}
    if v.kind == "lighter":
        async with session.get(url + "/api/v1/orderBooks", allow_redirects=False) as r:
            data = await r.json(content_type=None)
        for m in data.get("order_books", []):
            if m.get("market_type", "perp") == "perp" and m.get("status") == "active":
                out[base_of(venue, m["symbol"])] = m["symbol"]
    elif v.kind == "arcus":
        async with session.get(url + "/v1/markets", allow_redirects=False) as r:
            data = await r.json(content_type=None)
        rows = data.get("markets", data)
        rows = list(rows.values()) if isinstance(rows, dict) else rows
        for m in rows:
            name = m.get("marketDisplayName")
            if name and m.get("status") == "ONLINE":
                out[base_of(venue, name)] = name
    else:
        async with session.post(url + "/info", json={"type": "meta", "dex": v.dex}, allow_redirects=False) as r:
            data = await r.json(content_type=None)
        for u in data.get("universe", []):
            if not u.get("isDelisted"):
                out[base_of(venue, u["name"])] = u["name"]
    return out


async def fetch_all(cfg: AppConfig, venues: Optional[list[str]] = None) -> dict[str, dict[str, str]]:
    from arb.venues.transport import http_session
    names = venues or list(VENUE_NAMES)
    res: dict[str, dict[str, str]] = {}
    async with http_session() as s:
        got = await asyncio.gather(*[_fetch(s, cfg, n) for n in names], return_exceptions=True)
    for n, g in zip(names, got):
        if isinstance(g, Exception):
            print(f"{n}: market list failed: {g}")
            res[n] = {}
        else:
            res[n] = g
    return res


def render(cfg: AppConfig, listing: dict[str, dict[str, str]], only: Optional[list[str]] = None,
           min_venues: int = 2, yaml_snippet: bool = False) -> str:
    names = list(listing)
    bases = sorted({b for m in listing.values() for b in m})
    if only:
        want = {x.upper() for x in only}
        bases = [b for b in bases if b.upper() in want]
    lines = [f"{'symbol':<12}" + "".join(f"{n:<14}" for n in names) + "config"]
    snippet = []
    for b in bases:
        have = {n: listing[n].get(b) for n in names}
        cnt = sum(1 for x in have.values() if x)
        if cnt < min_venues:
            continue
        sc = cfg.symbols.get(b)
        state = "-"
        if sc is not None:
            missing = [n for n, x in have.items() if x and n not in sc.venues]
            state = "yes" + (f" (also listed on {','.join(missing)})" if missing else "")
        lines.append(f"{b:<12}" + "".join(f"{(have[n] or '-'):<14}" for n in names) + state)
        if sc is None:
            vm = ", ".join(f"{n}: {have[n]!r}" if ":" in str(have[n]) else f"{n}: {have[n]}" for n in names if have[n])
            snippet.append(f"  {b}:\n    venues: {{{vm}}}")
    out = "\n".join(lines)
    if yaml_snippet:
        out += ("\n\n# paste under `symbols:` in config/config.yaml (check fees, thresholds and session first;"
                " US stocks: add `strategy: *us`)\n" + ("\n".join(snippet) if snippet else "# nothing new"))
    return out
