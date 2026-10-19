"""Spread statistics from data/*/spread_samples/*.csv and trade stats from data/*/events.jsonl."""
from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Optional


def _q(xs: list[float], q: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    i = min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))
    return xs[i]


def load(data_root: Path, days: Optional[list[str]] = None) -> list[dict]:
    """All samples under data/<instance>/spread_samples/*.csv (monitor and every live pair)."""
    rows = []
    for f in sorted(data_root.glob("*/spread_samples/*.csv")):
        if days and f.stem not in days:
            continue
        with f.open(newline="") as fh:
            rows.extend(csv.DictReader(fh))
    return rows


def summarize(rows: list[dict], threshold: float) -> list[dict]:
    groups: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: {"sell": [], "buy": [], "best": [], "basis": []})
    for r in rows:
        if r.get("basis_ready") not in ("1", 1):
            continue
        g = groups[(r["pair"], r["maker"], r["taker"], r["symbol"])]
        vals = []
        for side, col in (("sell", "dev_sell_maker"), ("buy", "dev_buy_maker")):
            v = r.get(col)
            if v not in (None, "", "-"):
                g[side].append(float(v))
                vals.append(float(v))
        if vals:
            g["best"].append(max(vals))
        try:
            g["basis"].append(float(r["basis_ema"]))
        except (KeyError, ValueError):
            pass
    out = []
    for (pair, maker, taker, sym), g in sorted(groups.items()):
        best = g["best"]
        if not best:
            continue
        out.append(dict(pair=pair, maker=maker, taker=taker, symbol=sym, n=len(best),
                        basis_mean=statistics.fmean(g["basis"]) if g["basis"] else float("nan"),
                        best_mean=statistics.fmean(best), p50=_q(best, 0.5), p90=_q(best, 0.9), p99=_q(best, 0.99),
                        max=max(best), pct_above=100.0 * sum(1 for x in best if x >= threshold) / len(best)))
    return out


def print_report(data_root: Path, threshold: float, days: Optional[list[str]] = None) -> int:
    rows = load(data_root, days)
    if not rows:
        print(f"no samples under {data_root}/*/spread_samples (run `python -m arb monitor` first)")
        return 1
    stats = summarize(rows, threshold)
    if not stats:
        print("samples found, but none with a warmed-up basis yet (basis_ready=1)")
        return 1
    hdr = f"{'maker->taker':<26}{'symbol':<11}{'n':>7}{'basis':>8}{'mean':>8}{'p50':>8}{'p90':>8}{'p99':>8}{'max':>8}{'%>=T':>8}"
    print(f"best-side dev (% vs basis EMA, before fees); T = {threshold:.2f}%")
    print(hdr)
    print("-" * len(hdr))
    for s in stats:
        print(f"{s['maker'] + '->' + s['taker']:<26}{s['symbol']:<11}{s['n']:>7}{s['basis_mean']:>8.3f}{s['best_mean']:>8.3f}"
              f"{s['p50']:>8.3f}{s['p90']:>8.3f}{s['p99']:>8.3f}{s['max']:>8.3f}{s['pct_above']:>7.2f}%")
    return 0


def load_events(data_root: Path) -> list[dict]:
    out = []
    for f in sorted(data_root.glob("*/events.jsonl")):
        with f.open() as fh:
            for line in fh:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
    return out


def trade_stats(events: list[dict]) -> dict:
    """Per pair/symbol: opens, closes, gross two-leg PnL of closes, hedge retries, health alarms, halts."""
    st: dict = defaultdict(lambda: dict(opens=0, closes=0, gross_pnl=0.0, wins=0, zero_fill=0, requotes=0, alarms=0, halts=0))
    for e in events:
        k = (e.get("pair") or "-", e.get("symbol") or e.get("venue") or "-")
        kind = e.get("kind")
        if kind in ("open", "close") and float(e.get("hedged") or 0) > 0:
            s = st[k]
            s["opens" if kind == "open" else "closes"] += 1
            s["zero_fill"] += int(e.get("zero_fill_hedges") or 0)
            s["requotes"] += int(e.get("requotes") or 0)
            if kind == "close" and e.get("gross_pnl_usdc") is not None:
                s["gross_pnl"] += float(e["gross_pnl_usdc"])
                s["wins"] += float(e["gross_pnl_usdc"]) > 0
        elif kind == "market_health":
            st[(e.get("pair") or "-", f"{e.get('venue')}:{e.get('alarm') or 'health'}")]["alarms"] += 1
        elif kind == "halt":
            st[k]["halts"] += 1
    return st


def print_trades(data_root: Path) -> None:
    ev = load_events(data_root)
    if not ev:
        return
    stats = trade_stats(ev)
    if not stats:
        return
    hdr = f"{'pair':<22}{'symbol':<16}{'opens':>7}{'closes':>8}{'win%':>7}{'grossPnL$':>11}{'0-fill':>8}{'requote':>9}{'alarms':>8}{'halts':>7}"
    print("\ntrades (events.jsonl; gross two-leg PnL of closes, before fees/funding)")
    print(hdr)
    print("-" * len(hdr))
    for (pair, sym), s in sorted(stats.items()):
        win = 100.0 * s["wins"] / s["closes"] if s["closes"] else 0.0
        print(f"{pair:<22}{sym:<16}{s['opens']:>7}{s['closes']:>8}{win:>6.0f}%{s['gross_pnl']:>11.2f}{s['zero_fill']:>8}"
              f"{s['requotes']:>9}{s['alarms']:>8}{s['halts']:>7}")
