"""v5.7.1 `arb points` (read-only): which part of the own Lighter / Lighter RH volume can earn points.

Lighter on Robinhood Chain rewards no "standard-to-standard" activity: a fill earns points only when at least one
side pays a fee (premium accounts pay fees, standard accounts do not). Every trade record carries maker_fee and
taker_fee (fee ticks, 1e-6 of notional; missing = 0), so a fill of ours counts when our fee or the
counterparty's fee is not 0 (a negative fee = maker rebate, also a fee-paying tier). Each run also appends the
live points total to data/<pair>/points_history.jsonl; the next run prints points per 10k USDC of own volume since that snapshot.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

HISTORY_FILE = "points_history.jsonl"


def classify(t: dict, account: int, names: Optional[dict] = None) -> Optional[dict]:
    """Own role, notional and fees of one trade record (None if the account is not a side of it)."""
    ask, bid = int(t.get("ask_account_id", -1)), int(t.get("bid_account_id", -1))
    if account not in (ask, bid) or t.get("type", "trade") != "trade":
        return None
    maker_is_ask = bool(t.get("is_maker_ask"))
    if ask == bid:
        role = "self"  # both sides ours: never rewarded (self-trade)
    else:
        role = "maker" if (account == ask) == maker_is_ask else "taker"
    mf, tf = int(t.get("maker_fee") or 0), int(t.get("taker_fee") or 0)
    mine, theirs = (mf, tf) if role == "maker" else (tf, mf)
    try:
        usd = float(t.get("usd_amount") or 0)
    except (TypeError, ValueError):
        usd = 0.0
    sym = t.get("market_symbol") or (names or {}).get(int(t.get("market_id", -1))) or f"market {t.get('market_id')}"
    return dict(symbol=sym, role=role, usd=usd, my_fee=mine,
                their_fee=theirs, rewarded=role != "self" and (mine != 0 or theirs != 0))


def summarize(trades: list[dict], account: int, names: Optional[dict] = None) -> dict:
    rows: dict = defaultdict(lambda: dict(n=0, usd=0.0, rew_usd=0.0, maker_usd=0.0, maker_rew_usd=0.0))
    for t in trades:
        c = classify(t, account, names)
        if c is None:
            continue
        for k in (c["symbol"], "TOTAL"):
            r = rows[k]
            r["n"] += 1
            r["usd"] += c["usd"]
            r["rew_usd"] += c["usd"] if c["rewarded"] else 0.0
            if c["role"] == "maker":
                r["maker_usd"] += c["usd"]
                r["maker_rew_usd"] += c["usd"] if c["rewarded"] else 0.0
    return dict(rows)


def last_snapshot(path: Path, venue: str) -> Optional[dict]:
    p = Path(path)
    if not p.exists():
        return None
    last = None
    for line in p.read_text().splitlines():
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if r.get("venue") == venue and r.get("points") is not None:
            last = r
    return last


def append_snapshot(path: Path, venue: str, account: int, points: Optional[float], now: float) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a") as f:
        f.write(json.dumps(dict(ts=round(now, 1), venue=venue, account=account, points=points)) + "\n")


def render(venue: str, account: int, days: float, rows: dict, points: Optional[float], prev: Optional[dict],
           vol_since_prev: Optional[float], now: Optional[float] = None) -> str:
    now = time.time() if now is None else now
    out = [f"== {venue} account {account}: own fills of the last {days:g} day(s) "
           f"(rewarded = our fee or the counterparty's fee != 0; standard-to-standard earns nothing)"]
    if not rows:
        out.append("   no fills in the window")
    else:
        out.append(f"   {'symbol':<12}{'fills':>7}{'volume':>12}{'rewarded':>12}{'share':>7}{'maker vol':>12}{'maker share':>12}")
        keys = sorted((k for k in rows if k != "TOTAL"), key=lambda k: -rows[k]["usd"]) + ["TOTAL"]
        for k in keys:
            r = rows[k]
            sh = r["rew_usd"] / r["usd"] * 100 if r["usd"] else 0.0
            msh = f"{r['maker_rew_usd'] / r['maker_usd'] * 100:.0f}%" if r["maker_usd"] else "-"
            out.append(f"   {k:<12}{r['n']:>7}{r['usd']:>12.0f}{r['rew_usd']:>12.0f}{sh:>6.0f}%{r['maker_usd']:>12.0f}{msh:>12}")
    if points is None:
        out.append("   live points: not available on this venue")
    else:
        line = f"   live points total: {points:,.2f}"
        if prev is not None and vol_since_prev is not None:
            hours = (now - float(prev["ts"])) / 3600
            dp = points - float(prev["points"])
            per = f"{dp / vol_since_prev * 1e4:,.3f} points per 10k USDC own volume" if vol_since_prev > 0 else "no own volume"
            line += f"  (+{dp:,.2f} since the snapshot {hours:.1f}h ago; {vol_since_prev:,.0f} USDC own volume -> {per})"
        out.append(line)
    return "\n".join(out)
