"""中文终端监控面板（rich），非 TTY（systemd / nohup）时退回为每 30 秒一行的日志。

布局：
  ┌ 系统状态 ┬ 性能 ┬ 全局风险控制 ┐
  ├ 实时订单簿价格 + 资金费率 ───────┤
  ├ 套利对持仓明细 + 持仓校验 ┬ 交易所总持仓 & 总价值 ┤
  └ 执行记录（最近 N 条）┬ 事件日志 ┘
"""
from __future__ import annotations

import asyncio
import logging
import sys
import time
from datetime import datetime
from typing import Optional

from arb.config import effective_t1
from arb.engine import signals as sg
from arb.engine.engine import Ctx, Engine

log = logging.getLogger("arb.ui")

HOURS_PER_YEAR = 24 * 365


def _p(x, nd=3) -> str:
    return "-" if x is None else f"{x:+.{nd}f}"


def _px(lv) -> str:
    return "-" if lv is None else f"{float(lv.price):.6g}"


def _lv(lv) -> str:
    return "-" if lv is None else f"{float(lv.price):.6g}×{float(lv.size):.4g}"


def _num(x, nd=4) -> str:
    if x is None:
        return "-"
    try:
        return f"{float(x):.{nd}f}"
    except (TypeError, ValueError):
        return str(x)


def _dur(s: float) -> str:
    s = int(s)
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"


def _fund(rate: Optional[float]) -> str:
    """Hourly decimal rate -> '%/h / 年化%'."""
    if rate is None:
        return "-"
    return f"{rate * 100:+.4f}%/{rate * HOURS_PER_YEAR * 100:+.1f}%"


_SIDE = {sg.SHORT_MAKER: ("空", "多"), sg.LONG_MAKER: ("多", "空")}


class _Rates:
    """Frames/s per venue between two renders."""

    def __init__(self) -> None:
        self.prev: dict[str, tuple[float, int]] = {}
        self.rate: dict[str, float] = {}

    def update(self, eng: Engine) -> None:
        now = time.time()
        for n, v in eng.venues.items():
            t0, f0 = self.prev.get(n, (now, v.frames))
            if now - t0 >= 1.0:
                self.rate[n] = (v.frames - f0) / (now - t0)
                self.prev[n] = (now, v.frames)
            elif n not in self.prev:
                self.prev[n] = (now, v.frames)


_rates = _Rates()


# ---------------------------------------------------------------- panels
def _system_panel(eng: Engine):
    from rich.text import Text
    now = time.time()
    t = Text()
    t.append("套利执行终端 v3\n", style="bold")
    if eng.live:
        t.append("运行模式：", style="cyan")
        t.append("实盘模式（执行真实订单）\n", style="bold yellow")
    else:
        t.append("运行模式：", style="cyan")
        t.append("监控模式（不下单）\n", style="green")
    t.append("运行时间：", style="cyan")
    t.append(f"{_dur(now - eng.started)}\n")
    if eng.active:
        t.append("交易对：", style="cyan")
        t.append(f"{eng.active.maker}（maker）→ {eng.active.taker}（taker）\n")
    else:
        t.append("交易对：", style="cyan")
        t.append("无（仅监控）\n")
    n_act = len(eng.active_ctxs()) if eng.active else 0
    t.append("监控标的：", style="cyan")
    t.append(f"{len(eng.ctxs)} 个组合" + (f"，交易 {n_act} 个" if eng.active else "") + "\n")
    t.append("就绪：", style="cyan")
    t.append(f"{sum(1 for c in eng.ctxs if c.ready)}/{len(eng.ctxs)}")
    return t


def _perf_panel(eng: Engine):
    from rich.text import Text
    _rates.update(eng)
    t = Text()
    t.append("行情帧速率：", style="cyan")
    t.append("  ".join(f"{n}={_rates.rate.get(n, 0):.0f}/s" for n in eng.venues) + "\n")
    t.append("行情延迟：", style="cyan")
    now = time.time()
    ages = []
    for n, v in eng.venues.items():
        ages.append(f"{n}={now - v.alive_ts:.1f}s" if v.alive_ts else f"{n}=--")
    t.append("  ".join(ages) + "\n")
    t.append("计算耗时：", style="cyan")
    t.append(f"均值 {eng.tick_ms:.1f}ms  峰值 {eng.tick_max_ms:.1f}ms  循环 {eng.ticks}\n")
    running = len([x for x in eng.exec_tasks.values() if not x.done()])
    t.append("执行切片：", style="cyan")
    t.append(f"{running}/{eng.cfg.max_parallel_slices}" + ("（对账中）" if eng.reconcile_due else "") + "\n")
    t.append("健康告警：", style="cyan")
    t.append("  ".join(f"{n}={v.alarm_count}" for n, v in eng.venues.items()) + "\n")
    t.append("UI 刷新：", style="cyan")
    t.append(f"{max(0.25, eng.cfg.ui_refresh_s):.1f} 秒/次")
    return t


def _risk_panel(eng: Engine):
    from rich.text import Text
    t = Text()
    t.append("状态：", style="cyan")
    if eng.halted:
        t.append("已停止（需人工介入）\n", style="bold red")
    elif eng.cfg.close_only:
        t.append("仅平仓\n", style="yellow")
    else:
        t.append("正常运行\n", style="green")
    names = [eng.active.maker, eng.active.taker] if eng.active else list(eng.venues)
    bad = [n for n in names if not eng.venues[n].connected]
    t.append("网络：", style="cyan")
    t.append("正常\n" if not bad else f"断开 {','.join(bad)}\n", style="green" if not bad else "bold red")
    t.append("余额：", style="cyan")
    if not eng.balances:
        t.append("未知（监控模式或未配置凭证）\n", style="dim")
    else:
        parts, low = [], False
        for n in names:
            b = eng.balances.get(n)
            if b is None:
                parts.append(f"{n}: -")
                continue
            eq = float(b.equity)
            d = eq - eng.equity0.get(n, eq)
            parts.append(f"{n} 权益 {eq:.2f}（{d:+.2f}）可用 {float(b.available):.2f}")
            low = low or any(float(b.available) < c.sym.risk.min_available_usdc for c in eng.active_ctxs())
        t.append(("不足 " if low else "充足 ") + " | ".join(parts) + "\n", style="red" if low else None)
    realized = sum(sp.realized_edge_usdc for sp in eng.positions.items.values())
    gross = sum(float(x.get("gross_pnl_usdc") or 0) for x in eng.trades if x.get("kind") == "close")
    t.append("策略毛利：", style="cyan")
    t.append(f"累计 {realized:+.2f} U（本次记录 {gross:+.2f} U，未扣手续费/资金费）\n")
    t.append("未对冲名义：", style="cyan")
    un = eng._unhedged_notional() if eng.active else 0.0
    t.append(f"{un:.2f} U\n", style="red" if un > 0 else None)
    if eng.active:
        lev = []
        for n in names:
            b = eng.balances.get(n)
            gross_n = eng._venue_notional(n)
            if b is not None and float(b.equity) > 0:
                lev.append(f"{n} {gross_n:.0f}U / {gross_n / float(b.equity):.2f}x")
            else:
                lev.append(f"{n} {gross_n:.0f}U")
        t.append("总名义/杠杆：", style="cyan")
        t.append(" | ".join(lev) + "\n")
    blocked = [c.sym.key for c in eng.active_ctxs() if c.status.startswith("blocked")]
    t.append("开仓限制：", style="cyan")
    if eng.halted:
        t.append(f"全部禁止：{eng.halted}\n", style="red")
        t.append(f"锁文件：{eng.lock_path}", style="red")
    elif blocked:
        t.append(f"{len(blocked)} 个标的受限：{', '.join(blocked[:6])}", style="yellow")
    else:
        t.append("未触发", style="green")
    return t


def _ordered_ctxs(eng: Engine) -> list[Ctx]:
    act = [c for c in eng.ctxs if eng.active and c.pair == eng.active]
    rest = [c for c in eng.ctxs if c not in act]
    return act + rest


def _book_table(eng: Engine, max_rows: int):
    from rich.table import Table
    now = time.time()
    t = Table(title="实时订单簿价格 + 资金费率", expand=True, header_style="bold cyan", title_style="bold")
    for col, kw in (("交易对", {}), ("代币", {}), ("maker 买1/卖1", {}), ("maker 费率(时/年化)", {}),
                    ("taker 买1/卖1", {}), ("taker 费率(时/年化)", {}), ("费率差(年化)", {"justify": "right"}),
                    ("基差", {"justify": "right"}), ("偏离 卖/买 %", {"justify": "right"}), ("T1", {"justify": "right"}),
                    ("下次开/平", {"justify": "right"}), ("状态", {})):
        t.add_column(col, overflow="fold", **kw)
    rows = _ordered_ctxs(eng)
    for c in rows[:max_rows]:
        M, T = eng.venues[c.pair.maker], eng.venues[c.pair.taker]
        mb, tb = M.book(c.maker_sym), T.book(c.taker_sym)
        fm, ft = M.funding_hourly.get(c.maker_sym), T.funding_hourly.get(c.taker_sym)
        diff = "-" if fm is None or ft is None else f"{(ft - fm) * HOURS_PER_YEAR * 100:+.1f}%"
        active = eng.active is not None and c.pair == eng.active
        status = _zh_status(c.disabled or c.status)
        st_style = "green" if c.ready else ("red" if c.disabled or "error" in status else "yellow")
        t.add_row(f"{c.pair.maker}→{c.pair.taker}", c.sym.key,
                  f"{_lv(mb.best_bid if mb else None)} / {_lv(mb.best_ask if mb else None)}", _fund(fm),
                  f"{_lv(tb.best_bid if tb else None)} / {_lv(tb.best_ask if tb else None)}", _fund(ft), diff,
                  f"{c.basis_value:+.3f}{'' if c.ready else '*'}",
                  f"{_p(c.q_sell.dev if c.q_sell else None)} / {_p(c.q_buy.dev if c.q_buy else None)}",
                  f"{effective_t1(c.sym.strategy, now):.2f}",
                  f"{_p(c.next_open, 2) if c.next_open is not None else '-'} / {_p(c.next_close, 2) if c.next_close is not None else '-'}",
                  f"[{st_style}]{status}[/]", style=None if active else "dim")
    if len(rows) > max_rows:
        t.caption = f"另有 {len(rows) - max_rows} 行未显示（终端高度不够）；基差 * = EMA 预热中"
    else:
        t.caption = "基差 * = EMA 预热中 / 未就绪；偏离 = 扣除基差 EMA 后的可成交价差"
    return t


_ZH = (("ready", "就绪"), ("no fresh quotes", "行情不新鲜"), ("warmup", "预热"), ("blocked:", "受限:"),
       ("skipped:", "跳过:"), ("waiting", "等待"), ("close held by profit floor", "平仓被利润地板挡住"),
       ("signal, prices unstable", "信号出现但价格不稳"), ("fresh data", "新鲜行情"), ("price divergence", "价格偏离过大"),
       ("basis warmup", "基差预热"), ("error:", "错误:"))


def _zh_status(s: str) -> str:
    for a, b in _ZH:
        s = s.replace(a, b)
    return s


def _positions_table(eng: Engine):
    from rich.table import Table
    t = Table(title="套利对持仓明细", expand=True, header_style="bold cyan", title_style="bold")
    for col in ("交易对", "代币", "maker 方向 数量 @均价", "taker 方向 数量 @均价", "价差 开/平 %", "层级", "次数 开/平",
                "残差 M/T", "状态"):
        t.add_column(col, overflow="fold")
    n = 0
    for c in _ordered_ctxs(eng):
        sp = eng.position(c)
        if sp.is_flat() and not sp.n_open:
            continue
        n += 1
        q = sp.total_qty
        ms, ts_ = _SIDE.get(sp.direction, ("-", "-"))
        mq = sum((g.maker_cost for g in sp.grids), 0.0)
        tq = sum((g.taker_cost for g in sp.grids), 0.0)
        m_avg = mq / float(q) if q > 0 and mq else None
        t_avg = tq / float(q) if q > 0 and tq else None
        open_dev = (sum(g.open_dev_sum for g in sp.grids) / float(q)) if q > 0 else None
        close_dev = None
        if sp.direction:
            cq = c.q_buy if sg.CLOSE_DIRECTION[sp.direction] == sg.BUY_MAKER else c.q_sell
            close_dev = -cq.dev if cq and cq.ok else None
        levels = ",".join(f"L{g.level}" for g in sp.grids) or "-"
        state = "持仓" if q > 0 else ("残差" if not sp.is_flat() else "已平")
        st_style = "green" if q > 0 else ("red" if not sp.is_flat() else "dim")
        t.add_row(f"{c.pair.maker}→{c.pair.taker}", c.sym.key, f"{c.pair.maker} {ms} {_num(q, 4)} @{_num(m_avg, 4)}",
                  f"{c.pair.taker} {ts_} {_num(q, 4)} @{_num(t_avg, 4)}",
                  f"{_p(open_dev)} / {_p(close_dev)}", levels, f"{sp.n_open}/{sp.n_close}",
                  f"{sp.residual_maker}/{sp.residual_taker}", f"[{st_style}]{state}[/]")
    if not n:
        t.caption = "暂无持仓"
    else:
        t.caption = "价差(开/平)：开仓实现的调整后价差 / 当前平仓方向的可成交价差（越低越接近平仓线）"
    return t


def _check_panel(eng: Engine):
    from rich.text import Text
    t = Text()
    ck = eng.last_check
    if not eng.live:
        t.append("监控模式不做持仓校验", style="dim")
        return t
    if not ck:
        t.append("等待第一次对账 ...", style="dim")
        return t
    t.append("状态：", style="cyan")
    t.append("一致\n" if ck["ok"] else f"不一致（第 {eng.mismatch_strikes} 次）\n", style="green" if ck["ok"] else "bold red")
    t.append("时间：", style="cyan")
    t.append(datetime.fromtimestamp(ck["ts"]).strftime("%m-%d %H:%M:%S") + "\n")
    for r in ck["rows"][:12]:
        mark = "✓" if r["ok"] else "✗"
        t.append(f"{mark} {r['symbol']}: maker {r['maker']}（本地 {r['maker_exp']}）  taker {r['taker']}（本地 {r['taker_exp']}）\n",
                 style="green" if r["ok"] else "red")
    if not ck["rows"]:
        t.append("两边均无持仓", style="dim")
    return t


def _venue_positions(eng: Engine):
    from rich.console import Group
    from rich.table import Table
    pos = Table(title="交易所总持仓", expand=True, header_style="bold cyan", title_style="bold")
    for col in ("交易所", "代币", "方向", "总持仓", "入场价", "现价", "价值(U)", "校验"):
        pos.add_column(col, overflow="fold")
    ok_by = {}
    for r in eng.last_check.get("rows", []):
        ok_by[r["symbol"]] = r["ok"]
    sym_key = {}
    for c in eng.active_ctxs():
        sym_key[(c.pair.maker, c.maker_sym)] = c.sym.key
        sym_key[(c.pair.taker, c.taker_sym)] = c.sym.key
    totals: dict[str, float] = {}
    for vname, ps in eng.exch_positions.items():
        v = eng.venues.get(vname)
        for sym, p in sorted(ps.items()):
            if not p.qty:
                continue
            b = v.book(sym) if v else None
            mid = b.mid if b and b.mid else p.entry_price
            val = abs(float(p.qty * mid)) if mid else 0.0
            totals[vname] = totals.get(vname, 0.0) + val
            k = sym_key.get((vname, sym))
            chk = "-" if k is None else ("✓ 一致" if ok_by.get(k, True) else "✗ 不一致")
            pos.add_row(vname, sym, "多" if p.qty > 0 else "空", f"{abs(p.qty)}", _num(p.entry_price, 4),
                        _num(mid, 4), f"{val:.2f}", chk, style=None if k else "dim")
    if not eng.exch_positions:
        pos.caption = "实盘对账后显示（监控模式无数据）"
    tot = Table(title="交易所总价值（估算）", expand=True, header_style="bold cyan", title_style="bold")
    for col in ("交易所", "持仓价值(U)", "权益(U)", "可用(U)"):
        tot.add_column(col, justify="right")
    names = sorted(set(totals) | set(eng.balances))
    for n in names:
        b = eng.balances.get(n)
        tot.add_row(n, f"{totals.get(n, 0.0):.2f}", "-" if b is None else f"{float(b.equity):.2f}",
                    "-" if b is None else f"{float(b.available):.2f}")
    return Group(pos, tot)


def _trades_table(eng: Engine, max_rows: int):
    from rich.table import Table
    t = Table(title=f"执行记录（最近 {max_rows} 条）", expand=True, header_style="bold cyan", title_style="bold")
    for col in ("时间", "交易对", "代币", "类型", "方向", "数量", "maker 均价", "taker 均价", "原始/调整价差%",
                "毛利U", "改价", "状态"):
        t.add_column(col, overflow="fold")
    rows = [x for x in eng.trades if float(x.get("hedged") or 0) > 0 or x.get("error")][-max_rows:]
    for x in reversed(rows):
        kind = "开仓" if x.get("kind") == "open" else "平仓"
        ts = datetime.fromtimestamp(float(x.get("ts", 0))).strftime("%m-%d %H:%M")
        d = x.get("direction")
        dtxt = "M卖/T买" if d == sg.SELL_MAKER else "M买/T卖"
        err = x.get("error")
        st = f"[red]✗ {err}[/]" if err else f"[green]✓[/] {x.get('note') or ''}"
        pnl = x.get("gross_pnl_usdc")
        t.add_row(ts, str(x.get("pair") or "-"), str(x.get("symbol")), f"[{'magenta' if kind == '开仓' else 'cyan'}]{kind}[/]",
                  dtxt, str(x.get("hedged")), _num(x.get("maker_avg"), 4), _num(x.get("taker_avg"), 4),
                  f"{_p(_f(x.get('raw_edge')))} / {_p(_f(x.get('adj_edge')))}", "-" if pnl is None else f"{float(pnl):+.3f}",
                  str(x.get("requotes") or 0), st)
    if not rows:
        t.caption = "暂无成交"
    return t


def _f(x) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def _events_panel(eng: Engine):
    from rich.text import Text
    return Text("\n".join(eng.events) or "（暂无事件）")


def build_view(eng: Engine, height: Optional[int] = None):
    """Full-screen layout. `height` = terminal rows (used to size the tables)."""
    from rich.layout import Layout
    from rich.panel import Panel

    h = height or 50
    n_books = len(_ordered_ctxs(eng))
    top_h = 10
    bottom_h = max(10, min(16, h // 4))
    mid_h = max(10, min(18, h // 3))
    book_h = max(8, h - top_h - mid_h - bottom_h)
    book_rows = max(3, min(n_books, book_h - 6))

    root = Layout()
    root.split_column(Layout(name="top", size=top_h), Layout(name="book", size=book_h),
                      Layout(name="mid", size=mid_h), Layout(name="bottom"))
    root["top"].split_row(
        Layout(Panel(_system_panel(eng), title="系统状态", border_style="white")),
        Layout(Panel(_perf_panel(eng), title="性能", border_style="white")),
        Layout(Panel(_risk_panel(eng), title="全局风险控制", border_style="red" if eng.halted else "white"), ratio=2))
    root["book"].update(Panel(_book_table(eng, book_rows), border_style="white"))
    left = Layout()
    left.split_column(Layout(_positions_table(eng), ratio=3), Layout(Panel(_check_panel(eng), title="持仓校验"), ratio=2))
    root["mid"].split_row(Layout(left, ratio=3), Layout(Panel(_venue_positions(eng), title="交易所总持仓 & 总价值",
                                                              border_style="green"), ratio=2))
    root["bottom"].split_row(Layout(Panel(_trades_table(eng, max(3, bottom_h - 5)), border_style="white"), ratio=3),
                             Layout(Panel(_events_panel(eng), title="事件日志"), ratio=2))
    return root


async def run_ui(eng: Engine, rich_ui: bool = True) -> None:
    refresh = max(0.25, eng.cfg.ui_refresh_s)
    if rich_ui and sys.stdout.isatty():
        try:
            from rich.console import Console
            from rich.live import Live
        except ImportError:
            Live = None  # type: ignore
        if Live is not None:
            console = Console()
            with Live(build_view(eng, console.size.height), console=console, refresh_per_second=max(1, int(1 / refresh)),
                      screen=True) as live:
                while not eng.stopping:
                    await asyncio.sleep(refresh)
                    try:
                        live.update(build_view(eng, console.size.height))
                    except Exception:  # noqa: BLE001
                        log.exception("ui render")
            return
    # plain mode: one summary line per context every 30 s
    while not eng.stopping:
        await asyncio.sleep(30)
        for c in eng.ctxs:
            log.info("%s %s basis=%+.3f%s dev_sell=%s dev_buy=%s %s", c.pair.key, c.sym.key, c.basis_value,
                     "" if c.ready else "*", _p(c.q_sell.dev if c.q_sell else None), _p(c.q_buy.dev if c.q_buy else None),
                     c.disabled or c.status)

