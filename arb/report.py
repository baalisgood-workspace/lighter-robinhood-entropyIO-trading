"""Spread statistics from data/spread_samples/*.csv (written by monitor/live)."""
from __future__ import annotations

import csv
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


def load(sample_dir: Path, days: Optional[list[str]] = None) -> list[dict]:
    rows = []
    for f in sorted(sample_dir.glob("*.csv")):
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


def print_report(sample_dir: Path, threshold: float, days: Optional[list[str]] = None) -> int:
    rows = load(sample_dir, days)
    if not rows:
        print(f"no samples in {sample_dir} (run `python -m arb monitor` first)")
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
