"""v5.6.1 threshold auto-tune advisor (advise only: it never changes a parameter). v5.7: autotune.mode apply
lets the running bot apply the advice (Engine.tune_once); applied values live in data/<pair>/tune_applied.json.

Every close in the journal (events.jsonl, kind "close") carries the gross two-leg PnL of the closed quantity.
Each close is attributed to the T1 (initial threshold, %) and session bucket in effect when its grid was opened
(v5.6.1 journal fields open_t1 / open_bucket; older closes: inferred from the close trigger and the current
grid_step_pct / first_close_ratio). Per symbol and bucket (all | rth | off) only the closes at the CURRENT T1
count. Net bps per close = gross bps - round-trip fee bps; notional-weighted mean with a one-sided confidence
band (z * standard error, effective sample size of the weights):

    fewer than min_closes          -> hold (insufficient data); with history at a lower T1 whose lower bound
                                      > lower_bps: lower one step
    lower bound > lower_bps        -> lower T1 one step (not below the min bound)
    upper bound < raise_bps        -> raise T1 one step; already at the max bound -> close_only
    otherwise                      -> hold

Bounds: autotune.bounds (symbol, then category) over config.DEFAULT_TUNE_BOUNDS; missing: T1/2 .. 2*T1.
"""
from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Optional

from arb.config import AppConfig, SymbolConfig, us_regular_hours

EPS = 1e-6


def category_of(sc: SymbolConfig) -> str:
    if sc.category:
        return sc.category
    if sc.strategy.session == "us_equity":
        return "us_equity"
    from arb.autosym import classify
    return classify(sc.key.upper())


def buckets(sc: SymbolConfig) -> list[str]:
    st = sc.strategy
    return ["rth", "off"] if st.session == "us_equity" and st.offhours_threshold_pct is not None else ["all"]


def bucket_t1(sc: SymbolConfig, bucket: str) -> float:
    st = sc.strategy
    return float(st.offhours_threshold_pct) if bucket == "off" and st.offhours_threshold_pct is not None else float(st.initial_threshold_pct)


def bounds(cfg: AppConfig, sc: SymbolConfig, bucket: str) -> tuple[float, float]:
    b = cfg.autotune.bounds_for(sc.key, category_of(sc))
    cur = bucket_t1(sc, bucket)
    lo_k, hi_k = ("offhours_min", "offhours_max") if bucket == "off" else ("min", "max")
    lo, hi = b.get(lo_k), b.get(hi_k)
    lo = float(lo) if lo is not None else round(cur / 2, 4)
    hi = float(hi) if hi is not None else round(cur * 2, 4)
    return lo, max(lo, hi)


def fallback_fee_pct(cfg: AppConfig, sc: SymbolConfig) -> float:
    """Round-trip fee % from the configured venue fees (journal records before v5.6.1 carry no fee)."""
    from arb.engine.signals import roundtrip_fee_pct
    try:
        pair = cfg.active_pair()
    except Exception:  # noqa: BLE001
        return 0.0
    M, T = cfg.venues[pair.maker], cfg.venues[pair.taker]
    ex = sc.execution
    return roundtrip_fee_pct(M.maker_fee_pct or 0.0, T.taker_fee_pct or 0.0, ex.close_mode == "taker_taker",
                             M.taker_fee_pct or 0.0, open_taker_taker=ex.open_mode == "taker_taker")


def _infer_t1(sc: SymbolConfig, rec: dict) -> Optional[float]:
    req, lvl = rec.get("required"), rec.get("level")
    if req is None:
        return None
    try:
        req, lvl = float(req), int(lvl or 1)
    except (TypeError, ValueError):
        return None
    st = sc.strategy
    if lvl <= 1:
        return round(-req / st.first_close_ratio, 4) if st.first_close_ratio > 0 else None
    return round(-req - (lvl - 2) * st.grid_step_pct, 4)


def load_closes(cfg: AppConfig, events_path: Path, since_ts: float, fees: Optional[dict] = None) -> list[dict]:
    """Closes with a PnL since since_ts: symbol, ts, notional, net_bps, t1, bucket, inferred."""
    out: list[dict] = []
    p = Path(events_path)
    if not p.exists():
        return out
    with p.open() as f:
        for line in f:
            if '"close"' not in line or "gross_pnl_usdc" not in line:
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("kind") != "close" or float(r.get("ts", 0)) < since_ts or r.get("gross_pnl_usdc") is None:
                continue
            sc = cfg.symbols.get(r.get("symbol"))
            if sc is None:
                continue
            try:
                notional = float(r.get("hedged") or 0) * float(r.get("taker_avg") or 0)
                gross = float(r["gross_pnl_usdc"])
            except (TypeError, ValueError):
                continue
            if notional <= 0:
                continue
            fee = r.get("fee_pct")
            if fee is None:
                fee = (fees or {}).get(sc.key)
            if fee is None:
                fee = fallback_fee_pct(cfg, sc)
            t1, bucket, inferred = r.get("open_t1"), r.get("open_bucket"), False
            if t1 is None:
                t1, inferred = _infer_t1(sc, r), True
            if t1 is None:
                continue
            if bucket is None:
                bucket = buckets(sc)[0] if len(buckets(sc)) == 1 else ("rth" if us_regular_hours(float(r["ts"])) else "off")
            gross_bps = gross / notional * 1e4
            out.append(dict(symbol=sc.key, ts=float(r["ts"]), notional=notional, gross_usdc=gross,
                            net_bps=gross_bps - float(fee) * 100, net_usdc=gross - notional * float(fee) / 100,
                            t1=round(float(t1), 4), bucket=bucket, inferred=inferred))
    return out


def stats(rows: list[dict], z: float) -> dict:
    n = len(rows)
    w = sum(r["notional"] for r in rows)
    if n == 0 or w <= 0:
        return dict(n=n, notional=0.0, net_usdc=0.0, mean_bps=None, lb=None, ub=None)
    mean = sum(r["notional"] * r["net_bps"] for r in rows) / w
    var = sum(r["notional"] * (r["net_bps"] - mean) ** 2 for r in rows) / w
    n_eff = w * w / sum(r["notional"] ** 2 for r in rows)
    se = math.sqrt(var / n_eff) if n > 1 else float("inf")
    return dict(n=n, notional=round(w, 2), net_usdc=round(sum(r["net_usdc"] for r in rows), 4), mean_bps=round(mean, 2),
                lb=round(mean - z * se, 2) if se != float("inf") else None,
                ub=round(mean + z * se, 2) if se != float("inf") else None)


def advise(cfg: AppConfig, events_path: Path, now: Optional[float] = None, keys: Optional[list] = None,
           fees: Optional[dict] = None, hours: Optional[float] = None) -> list[dict]:
    at = cfg.autotune
    now = time.time() if now is None else now
    lookback = at.lookback_h if hours is None else hours
    closes = load_closes(cfg, events_path, now - lookback * 3600, fees)
    rows = []
    for sc in cfg.symbols.values():
        if keys is not None and sc.key not in keys:
            continue
        if keys is None and (not sc.enabled or (sc.auto and not any(r["symbol"] == sc.key for r in closes))):
            continue  # offline (`arb tune`): auto symbols only when they traded in the window
        for bucket in buckets(sc):
            cur = bucket_t1(sc, bucket)
            lo, hi = bounds(cfg, sc, bucket)
            mine = [r for r in closes if r["symbol"] == sc.key and r["bucket"] == bucket]
            s = stats([r for r in mine if abs(r["t1"] - cur) < EPS], at.z)
            hist = []
            for t in sorted({r["t1"] for r in mine}):
                hs = stats([r for r in mine if abs(r["t1"] - t) < EPS], at.z)
                hist.append(dict(t1=t, n=hs["n"], notional=hs["notional"], mean_bps=hs["mean_bps"], lb=hs["lb"], ub=hs["ub"]))
            step = at.step_pct
            action, target, why = "hold", None, ""
            if sc.close_only or cfg.close_only:
                why = "close_only is set"
            elif cur < lo - EPS:
                action, target, why = "raise", lo, f"T1 below the min bound {lo:g}"
            elif cur > hi + EPS:
                action, target, why = "lower", hi, f"T1 above the max bound {hi:g}"
            elif s["n"] < at.min_closes:
                good = [h for h in hist if h["t1"] < cur - EPS and h["n"] >= at.min_closes and h["lb"] is not None
                        and h["lb"] > at.lower_bps]
                if good and cur - step >= lo - EPS:
                    h = good[-1]
                    action, target = "lower", round(cur - step, 4)
                    why = (f"only {s['n']}/{at.min_closes} closes at T1 {cur:g}; at T1 {h['t1']:g}: {h['n']} closes, "
                           f"{h['mean_bps']:+.1f} bps (lower bound {h['lb']:+.1f})")
                else:
                    why = f"insufficient data: {s['n']}/{at.min_closes} closes at T1 {cur:g}"
            elif s["lb"] is not None and s["lb"] > at.lower_bps:
                if cur - step >= lo - EPS:
                    action, target = "lower", round(cur - step, 4)
                    why = f"profitable: lower bound {s['lb']:+.1f} bps > {at.lower_bps:g}"
                else:
                    why = f"profitable, already at the min bound {lo:g}"
            elif s["ub"] is not None and s["ub"] < at.raise_bps:
                if cur + step <= hi + EPS:
                    action, target = "raise", round(cur + step, 4)
                    why = f"losing: upper bound {s['ub']:+.1f} bps < {at.raise_bps:g}"
                else:
                    action, why = "close_only", f"losing at the max bound {hi:g} (upper bound {s['ub']:+.1f} bps)"
            else:
                why = f"no significant edge ({s['mean_bps']:+.1f} bps, band {s['lb']:+.1f} .. {s['ub']:+.1f})"
            rows.append(dict(symbol=sc.key, bucket=bucket, t1=cur, min=lo, max=hi, n=s["n"], notional=s["notional"],
                             net_usdc=s["net_usdc"], mean_bps=s["mean_bps"], lb=s["lb"], ub=s["ub"], action=action,
                             target=target, why=why, n_all=len(mine), inferred=sum(1 for r in mine if r["inferred"]),
                             history=hist))
    return rows


APPLIED_FILE = "tune_applied.json"


def set_bucket_t1(sc: SymbolConfig, bucket: str, t1: float) -> None:
    if bucket == "off":
        sc.strategy.offhours_threshold_pct = t1
    else:
        sc.strategy.initial_threshold_pct = t1


def read_applied(path: Path) -> dict:
    try:
        data = json.loads(Path(path).read_text())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def write_applied(path: Path, data: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=1, sort_keys=True))
    tmp.replace(p)


def load_applied(cfg: AppConfig, path: Path) -> list[str]:
    """v5.7 (autotune.mode apply): put the thresholds the bot moved earlier back into cfg. An entry only counts
    while the config file still has the value it replaced (config_t1); after a manual edit the file wins.
    Returns one line per value used."""
    out: list[str] = []
    for key, ent in read_applied(path).items():
        sc = cfg.symbols.get(key)
        if sc is None or not isinstance(ent, dict):
            continue
        for bucket in buckets(sc):
            e = ent.get(bucket)
            if isinstance(e, dict) and e.get("t1") is not None and abs(bucket_t1(sc, bucket) - float(e.get("config_t1", -1))) < EPS:
                set_bucket_t1(sc, bucket, float(e["t1"]))
                sess = "" if bucket == "all" else f" [{bucket}]"
                out.append(f"{key}{sess}: T1 {float(e['config_t1']):g}% -> {float(e['t1']):g}% (autotune)")
        co = ent.get("close_only")
        if isinstance(co, dict) and co.get("config") is False and not sc.close_only:
            sc.close_only = True
            out.append(f"{key}: close_only (autotune)")
    return out


def record_applied(path: Path, sc: SymbolConfig, bucket: str, old_t1: float, new_t1: float, now: float) -> None:
    data = read_applied(path)
    ent = data.setdefault(sc.key, {})
    prev = ent.get(bucket) if isinstance(ent.get(bucket), dict) else None
    # config_t1 = the value in the config file, kept from the first change on
    config_t1 = float(prev["config_t1"]) if prev and abs(float(prev.get("t1", -1)) - old_t1) < EPS else old_t1
    ent[bucket] = dict(t1=new_t1, config_t1=config_t1, ts=round(now, 1))
    write_applied(path, data)


def record_close_only(path: Path, sc: SymbolConfig, now: float) -> None:
    data = read_applied(path)
    data.setdefault(sc.key, {})["close_only"] = dict(config=False, ts=round(now, 1))
    write_applied(path, data)


def advice_text(r: dict) -> str:
    sess = "" if r["bucket"] == "all" else f" [{'US hours' if r['bucket'] == 'rth' else 'off-hours'}]"
    if r["action"] in ("lower", "raise"):
        return f"{r['symbol']}{sess}: {r['action']} T1 {r['t1']:g}% -> {r['target']:g}% ({r['why']})"
    if r["action"] == "close_only":
        return f"{r['symbol']}{sess}: set close_only ({r['why']})"
    return f"{r['symbol']}{sess}: keep T1 {r['t1']:g}% ({r['why']})"


def _b(x) -> str:
    return "-" if x is None else f"{x:+.1f}"


def print_tune(cfg: AppConfig, events_path: Path, hours: Optional[float] = None, symbols: Optional[list] = None,
               show_all: bool = False, history: bool = True) -> int:
    rows = advise(cfg, events_path, hours=hours, keys=symbols)
    at = cfg.autotune
    lb = at.lookback_h if hours is None else hours
    mode = "advise only - nothing is changed" if at.mode == "advise" else "autotune.mode apply: the running bot applies it"
    print(f"threshold advice ({mode}), last {lb:g}h, min {at.min_closes} closes, "
          f"step {at.step_pct:g}%, lower if lower bound > {at.lower_bps:g} bps, raise if upper bound < {at.raise_bps:g} bps")
    print(f"{'symbol':<12}{'sess':<5}{'T1%':>6}{'min-max':>12}{'closes':>7}{'notional':>10}{'net U':>8}{'bps':>7}"
          f"{'low':>7}{'high':>7}  advice")
    shown = 0
    for r in rows:
        if not show_all and not r["n_all"] and r["action"] == "hold":
            continue
        shown += 1
        adv = r["action"] if r["action"] in ("hold", "close_only") else f"{r['action']} -> {r['target']:g}"
        print(f"{r['symbol']:<12}{r['bucket']:<5}{r['t1']:>6.3g}{(str(r['min']) + '-' + str(r['max'])):>12}{r['n']:>7}"
              f"{r['notional']:>10.0f}{r['net_usdc']:>+8.2f}{_b(r['mean_bps']):>7}{_b(r['lb']):>7}{_b(r['ub']):>7}  {adv}: {r['why']}")
        if history and len(r["history"]) > 1 or (history and r["history"] and abs(r["history"][0]["t1"] - r["t1"]) > EPS):
            for h in r["history"]:
                mark = "*" if abs(h["t1"] - r["t1"]) < EPS else " "
                print(f"{'':<12}{'':<5}{mark}{h['t1']:>5.3g}{'':>12}{h['n']:>7}{h['notional']:>10.0f}{'':>8}"
                      f"{_b(h['mean_bps']):>7}{_b(h['lb']):>7}{_b(h['ub']):>7}  (history at this T1)")
    hidden = len(rows) - shown
    if hidden:
        print(f"({hidden} symbol/session(s) without closes in the window hidden; --all shows them)")
    inferred = sum(r["inferred"] for r in rows)
    if inferred:
        print(f"note: {inferred} close(s) from before v5.6.1: their T1 is inferred from the close trigger and the current "
              f"grid_step_pct / first_close_ratio")
    return 0


def print_tune_log(events_path: Path, hours: float = 24.0, symbols: Optional[list] = None) -> int:
    """The advice the running bot journaled (kind tune_advice), newest last; only rows whose advice changed."""
    p = Path(events_path)
    if not p.exists():
        print(f"no journal at {p}")
        return 0
    since = time.time() - hours * 3600
    last: dict = {}
    runs = 0
    with p.open() as f:
        for line in f:
            if '"tune_advice"' not in line:
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("kind") != "tune_advice" or float(r.get("ts", 0)) < since:
                continue
            runs += 1
            ts = time.strftime("%m-%d %H:%M", time.localtime(float(r["ts"])))
            for row in r.get("rows") or []:
                if symbols and row.get("symbol") not in symbols:
                    continue
                k = (row.get("symbol"), row.get("bucket"))
                text = advice_text(row)
                if last.get(k) != text:
                    print(f"{ts}  {text}")
                    last[k] = text
    print(f"{runs} evaluation(s) in the last {hours:g}h" + ("" if runs else " (bot not running v5.6.1 yet, or autotune.enabled: false)"))
    return 0
