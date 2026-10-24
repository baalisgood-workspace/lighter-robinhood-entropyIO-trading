#!/usr/bin/env python3
"""只读诊断：最近 N 小时为什么没有开仓/成交。在仓库根目录运行（机器人运行中也可以）：
   python3 why_idle.py [--hours 8] [--since "10-23 21:30" --until "10-24 04:00"] [--data data]
看三件事：1) 有没有人工干预锁 / 停机记录  2) 最近的开平仓记录  3) 每个标的价差离开仓门槛还差多少"""
import csv, glob, json, os, sys, time
from collections import Counter, defaultdict
a = sys.argv[1:]
H = float(a[a.index("--hours") + 1]) if "--hours" in a else 8.0
D = a[a.index("--data") + 1] if "--data" in a else "data"
now = time.time(); t0 = now - H * 3600; t_end = now
def _pt(v):  # "10-23 21:30" (本机时区，当年)
    return time.mktime(time.strptime(f"{time.localtime().tm_year}-{v}", "%Y-%m-%d %H:%M"))
if "--since" in a:
    t0 = _pt(a[a.index("--since") + 1])
if "--until" in a:
    t_end = _pt(a[a.index("--until") + 1])
ts = lambda t: time.strftime("%m-%d %H:%M:%S", time.localtime(t))
def f(x):
    try:
        return None if x in (None, "", "-") else float(x)
    except ValueError:
        return None
dirs = sorted({os.path.dirname(p) for p in glob.glob(f"{D}/*/events.jsonl") + glob.glob(f"{D}/*/spread_samples")})
if not dirs:
    sys.exit(f"{D}/ 下没找到实例目录（请在仓库根目录运行，或用 --data 指定）")
print(f"现在 {ts(now)}（本机时区），统计 {ts(t0)} ~ {ts(t_end)}\n")
for d in dirs:
    print("=" * 100, f"\n实例目录 {d}")
    lock = os.path.join(d, "MANUAL_INTERVENTION.json")
    if os.path.exists(lock):
        print(f"!!! 有人工干预锁 {lock}（这会阻止交易）：\n   ", open(lock).read()[:800])
    ev = os.path.join(d, "events.jsonl")
    if os.path.exists(ev):
        last, kinds, n_in = {}, Counter(), Counter()
        skips, dusts, tunes = Counter(), [], None
        with open(ev) as fh:
            for line in fh:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                k, t = e.get("kind", "?"), float(e.get("ts") or e.get("t") or 0)
                last[k] = (t, e)
                if t0 <= t <= t_end:
                    n_in[k] += 1
                    if k == "signal_skip":  # v5.6
                        skips[(e.get("symbol"), e.get("reason"), (e.get("detail") or "")[:60])] += 1
                    elif k == "dust_flatten":
                        dusts.append((t, e))
                    elif k == "tune_advice":  # v5.6.1
                        tunes = (t, e)
        print(f"events.jsonl 窗口内各类记录数:", dict(n_in) or "无")
        if skips:
            print("  v5.6 没成交的开仓信号（每个标的每种原因最多 5 分钟记一次）：")
            for (sym, why, det), n in skips.most_common(25):
                print(f"    {sym:<10} {why:<24} {n:>4} 次  {det}")
        for t, e in dusts:
            kind = "残仓(无网格)" if e.get("mode") == "residual" else "残仓"
            src = {"exchange": "交易所已确认", "computed": "按成交计算，交易所数据未跟上"}.get(e.get("source"), "")
            print(f"  {kind}清理 {ts(t)} {e.get('symbol')}: {'成功' if e.get('ok') else '未完成'}  前 {e.get('before')} 后 {e.get('after')}  {src}  {e.get('error') or ''}")
        if tunes:  # v5.6.1: latest threshold advice (advise only)
            t, e = tunes
            sug = [r for r in e.get("rows") or [] if r.get("action") != "hold"]
            print(f"  门槛建议（只建议不修改，{ts(t)}）：" + ("无调整建议" if not sug else ""))
            for r in sug:
                tgt = f" -> {r.get('target')}" if r.get("target") is not None else ""
                print(f"    {r.get('symbol'):<10} {r.get('bucket'):<4} T1 {r.get('t1')}{tgt}  {r.get('action')}: {r.get('why')}")
        for k in ("open", "close"):
            if k in last:
                t, e = last[k]
                print(f"  最后一次 {k}: {ts(t)}  {e.get('symbol')} {e.get('direction', '')} ({(now - t) / 3600:.1f} 小时前)")
        for k, (t, e) in sorted(last.items(), key=lambda x: -x[1][0]):
            if k not in ("open", "close", "markout"):
                print(f"  最近的 {k}: {ts(t)}  {json.dumps(e, ensure_ascii=False)[:300]}")
                break
    files = sorted(glob.glob(os.path.join(d, "spread_samples", "*.csv")))
    rows = defaultdict(list); last_ts = 0
    for p in files[-3:]:
        with open(p, newline="") as fh:
            for r in csv.DictReader(fh):
                t = f(r.get("ts"))
                if t and t0 <= t <= t_end and "t1" in r:
                    rows[r["symbol"]].append(r); last_ts = max(last_ts, t)
    if not rows:
        print("最近没有价差采样（机器人可能没在运行，或没连上行情）\n"); continue
    print(f"价差采样：最后一条 {ts(last_ts)}（{(now - last_ts) / 60:.1f} 分钟前；超过 1 分钟说明程序可能已停或卡住）")
    print("  门槛 = T1 + 往返手续费 + 资金费（第 1 格）。'最佳差距' = 窗口内价差最接近门槛时还差多少（负数 = 曾超过门槛）")
    print(f"  {'标的':<10}{'类别':<11}{'采样':>6}{'就绪%':>7}{'盘中%':>7}{'T1':>7}{'手续费':>8}{'最佳差距':>9}{'超门槛次数':>10}  最后一次超门槛")
    out = []
    for s, rs in rows.items():
        best, hits, lh = None, 0, None
        for r in rs:
            if f(r.get("t1")) is None:
                continue
            need0 = f(r["t1"]) + (f(r["fee_mt"]) or 0)
            hit = False
            for dv, fu in (("dev_sell_maker", "fund_sell"), ("dev_buy_maker", "fund_buy")):
                v = f(r.get(dv))
                if v is None:
                    continue
                g = need0 + (f(r.get(fu)) or 0) - v
                best = g if best is None else min(best, g)
                hit = hit or (g <= 0 and r.get("basis_ready") == "1")
            if hit:
                hits += 1; lh = f(r["ts"])
        rd = sum(r.get("basis_ready") == "1" for r in rs) / len(rs) * 100
        rth = sum(r.get("rth") == "1" for r in rs) / len(rs) * 100
        last = rs[-1]
        out.append((best if best is not None else 9, s, last.get("category", ""), len(rs), rd, rth, f(last.get("t1")) or 0, f(last.get("fee_mt")) or 0, best, hits, lh))
    for best_, s, cat, n, rd, rth, t1, fee, best, hits, lh in sorted(out)[:40]:
        b = "   -" if best is None else f"{best:+.3f}%"
        print(f"  {s:<10}{cat[:10]:<11}{n:>6}{rd:>6.0f}%{rth:>6.0f}%{t1:>6.3f}%{fee:>7.4f}%{b:>9}{hits:>10}  {ts(lh) if lh else '-'}")
    if len(out) > 40:
        print(f"  ……其余 {len(out) - 40} 个标的离门槛更远")
    print()
print("说明：'超门槛次数' > 0 但没有开仓 → 被别的条件挡住（close_only / 余额 / 仓位上限 / 确认次数 / 行情不新鲜），看日志里的 status；")
print("      全部为正 → 这段时间价差本来就没到门槛，属于正常空闲（盘外 / 周末 / 门槛偏高）。")
