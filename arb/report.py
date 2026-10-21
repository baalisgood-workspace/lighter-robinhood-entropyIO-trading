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
        if days and f.stem[:10] not in days:  # YYYY-MM-DD.csv or YYYY-MM-DD_v5.csv
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


def write_charts(data_root: Path, out_dir: Path, threshold: float, days: Optional[list[str]] = None,
                 symbols: Optional[list[str]] = None) -> list[Path]:
    """One PNG per (pair, symbol): basis-adjusted dev of both directions + basis EMA over time
    (ported idea from lighter-rh history/chart_generator). Needs matplotlib (optional dependency)."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from datetime import datetime
    except ImportError:
        raise RuntimeError("matplotlib is not installed: pip install matplotlib") from None
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in load(data_root, days):
        if symbols and r["symbol"] not in symbols:
            continue
        groups[(r["pair"], r["symbol"])].append(r)
    out_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for (pair, sym), rows in sorted(groups.items()):
        rows.sort(key=lambda r: float(r["ts"]))
        ts = [datetime.fromtimestamp(float(r["ts"])) for r in rows]

        def col(name):
            return [float(r[name]) if r.get(name) not in (None, "", "-") else float("nan") for r in rows]
        fig, (a1, a2) = plt.subplots(2, 1, figsize=(12, 6), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
        a1.plot(ts, col("dev_sell_maker"), lw=0.7, label="dev sell maker")
        a1.plot(ts, col("dev_buy_maker"), lw=0.7, label="dev buy maker")
        a1.axhline(threshold, color="red", ls="--", lw=0.8, label=f"T1 {threshold:g}%")
        a1.set_ylabel("%")
        a1.legend(loc="upper left", fontsize=8)
        a1.set_title(f"{pair} {sym}  ({len(rows)} samples)")
        a2.plot(ts, col("basis_now"), lw=0.5, alpha=0.6, label="basis now")
        a2.plot(ts, col("basis_ema"), lw=1.0, label="basis EMA")
        a2.set_ylabel("%")
        a2.legend(loc="upper left", fontsize=8)
        fig.autofmt_xdate()
        fig.tight_layout()
        path = out_dir / f"{pair.replace('+', '_')}_{sym}.png"
        fig.savefig(path, dpi=110)
        plt.close(fig)
        files.append(path)
    return files


# ---------------------------------------------------------------- v5: arb rank
EPISODE_HYSTERESIS_PCT = 0.03  # an opportunity ends when the excess falls this far below zero (no flicker double count)
MIN_COVERAGE_H = 1.0


def _fnum(r: dict, k: str) -> Optional[float]:
    v = r.get(k)
    if v in (None, "", "-"):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def rank(rows: list[dict], mode: str = "maker_taker", cfg=None, pair: Optional[str] = None) -> list[dict]:
    """Score every (pair, maker, taker, symbol) from samples.

    excess = best-direction dev - (T1 + round-trip fees + adverse funding) of the chosen open mode.
    An opportunity (episode) starts when excess >= 0 and ends when it drops below -0.03% or data stops.
    score = opportunities/day x (mean T1 + mean peak excess) x liquidity factor, i.e. roughly the
    edge (% of one slice notional) that could be captured per day. A ranking aid, not a profit forecast.
    """
    a = getattr(cfg, "auto_symbols", None)
    max_spread = getattr(a, "max_book_spread_pct", None)
    min_depth = getattr(a, "min_depth_usdc", None)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        if r.get("basis_ready") not in ("1", 1) or (pair and r.get("pair") != pair):
            continue
        groups[(r["pair"], r["maker"], r["taker"], r["symbol"])].append(r)
    out = []
    for (pk, maker, taker, sym), rs in groups.items():
        rs.sort(key=lambda r: float(r["ts"]))
        sc = cfg.symbols.get(sym) if cfg is not None else None
        t1_cfg = sc.strategy.initial_threshold_pct if sc else 0.30
        fee_cfg = 0.0
        if cfg is not None:
            vm, vt = cfg.venues.get(maker), cfg.venues.get(taker)
            mf = (vm.maker_fee_pct or 0.0) if vm else 0.0
            mtf = (vm.taker_fee_pct or 0.0) if vm else 0.0
            tf = (vt.taker_fee_pct or 0.0) if vt else 0.0
            fee_cfg = (mtf if mode == "taker_taker" else mf) + tf + mf + tf
        dts = [float(b["ts"]) - float(a_["ts"]) for a_, b in zip(rs, rs[1:])]
        step = statistics.median(dts) if dts else 5.0
        max_gap = max(3 * step, 15.0)
        covered = sum(min(d, max_gap) for d in dts)
        eps: list[dict] = []
        cur = None
        last_ts = None
        opp_rows = rth_opp = 0
        t1s, spreads, depths = [], [], []
        for r in rs:
            ts = float(r["ts"])
            if mode == "taker_taker":
                ds, db = _fnum(r, "tt_dev_sell"), _fnum(r, "tt_dev_buy")
            else:
                ds, db = _fnum(r, "dev_sell_maker"), _fnum(r, "dev_buy_maker")
            t1 = _fnum(r, "t1")
            t1 = t1_cfg if t1 is None else t1
            fee = _fnum(r, "fee_tt" if mode == "taker_taker" else "fee_mt")
            fee = fee_cfg if fee is None else fee
            sp, dp = _fnum(r, "book_spread_pct"), _fnum(r, "depth_usdc")
            if sp is not None:
                spreads.append(sp)
            if dp is not None:
                depths.append(dp)
            exs = []
            for d, fund in ((ds, _fnum(r, "fund_sell")), (db, _fnum(r, "fund_buy"))):
                if d is not None:
                    exs.append(d - (t1 + fee + (fund or 0.0)))
            if not exs:
                continue
            ex = max(exs)
            if cur is not None and last_ts is not None and ts - last_ts > max_gap:
                eps.append(cur)
                cur = None
            last_ts = ts
            if ex >= 0:
                opp_rows += 1
                rth_opp += r.get("rth") in ("1", 1)
                if cur is None:
                    cur = {"start": ts, "end": ts, "peak": ex, "t1": t1}
                cur["end"], cur["peak"] = ts, max(cur["peak"], ex)
            elif cur is not None and ex < -EPISODE_HYSTERESIS_PCT:
                eps.append(cur)
                cur = None
            t1s.append(t1)
        if cur is not None:
            eps.append(cur)
        hours = covered / 3600
        days = max(hours / 24, 1e-9)
        n = len(rs)
        med_sp = statistics.median(spreads) if spreads else None
        med_dp = statistics.median(depths) if depths else None
        liq = 1.0
        note = ""
        if med_sp is not None and max_spread is not None and med_sp > max_spread:
            liq, note = 0.0, f"spread {med_sp:.3f}%>{max_spread:g}%"
        elif med_dp is not None and min_depth:
            liq = min(1.0, med_dp / min_depth)
            if liq < 1:
                note = f"depth {med_dp:.0f}<{min_depth:.0f}"
        peak = statistics.fmean([e["peak"] for e in eps]) if eps else 0.0
        t1m = statistics.fmean([e["t1"] for e in eps]) if eps else (statistics.fmean(t1s) if t1s else t1_cfg)
        per_day = len(eps) / days
        score = per_day * (t1m + peak) * liq
        if hours < MIN_COVERAGE_H:
            score, note = 0.0, f"only {hours:.1f}h data"
        out.append(dict(pair=pk, maker=maker, taker=taker, symbol=sym, mode=mode, hours=round(hours, 2), samples=n,
                        episodes=len(eps), per_day=round(per_day, 2), avg_peak_excess=round(peak, 4),
                        avg_duration_s=round(statistics.fmean([e["end"] - e["start"] + step for e in eps]), 1) if eps else 0.0,
                        pct_time=round(100.0 * opp_rows / n, 2) if n else 0.0,
                        rth_share=round(100.0 * rth_opp / opp_rows, 1) if opp_rows else None,
                        book_spread_pct=None if med_sp is None else round(med_sp, 4),
                        depth_usdc=None if med_dp is None else round(med_dp), score=round(score, 4), note=note,
                        category=(rs[-1].get("category") or "")))
    out.sort(key=lambda x: -x["score"])
    return out


def print_rank(data_root: Path, mode: str, days: Optional[list[str]], pair: Optional[str], top: int, save: bool,
               cfg=None) -> int:
    rows = load(data_root, days)
    if not rows:
        print(f"no samples under {data_root}/*/spread_samples (run `python -m arb monitor --all-symbols` first)")
        return 1
    res = rank(rows, mode, cfg, pair)
    if not res:
        print("samples found, but none with a warmed-up basis yet (basis_ready=1)")
        return 1
    print(f"rank ({mode} open): excess = dev - (T1 + fees + funding); score ~ opportunities/day x (T1 + peak excess) x liquidity")
    hdr = (f"{'#':>3} {'maker->taker':<24}{'symbol':<11}{'cat':<10}{'hours':>6}{'opp/day':>8}{'peak+%':>8}{'dur s':>7}"
           f"{'time%':>7}{'RTH%':>6}{'spread%':>8}{'depthU':>8}{'score':>8}  note")
    print(hdr)
    print("-" * len(hdr))
    for i, r in enumerate(res[:top], 1):
        rth = "-" if r["rth_share"] is None else f"{r['rth_share']:.0f}"
        sp = "-" if r["book_spread_pct"] is None else f"{r['book_spread_pct']:.3f}"
        dp = "-" if r["depth_usdc"] is None else f"{r['depth_usdc']:.0f}"
        print(f"{i:>3} {r['maker'] + '->' + r['taker']:<24}{r['symbol']:<11}{(r['category'] or '-')[:9]:<10}{r['hours']:>6.1f}"
              f"{r['per_day']:>8.1f}{r['avg_peak_excess']:>8.3f}{r['avg_duration_s']:>7.0f}{r['pct_time']:>7.2f}{rth:>6}"
              f"{sp:>8}{dp:>8}{r['score']:>8.3f}  {r['note']}")
    if len(res) > top:
        print(f"... {len(res) - top} more (--top N)")
    if save:
        path = data_root / "rank.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        import time as _t
        path.write_text(json.dumps({"ts": _t.time(), "mode": mode, "rows": res}, indent=1))
        print(f"\nsaved {path} (used by auto_symbols.trade: ranked, top {getattr(getattr(cfg, 'auto_symbols', None), 'ranked_top_n', 15)})")
    return 0
