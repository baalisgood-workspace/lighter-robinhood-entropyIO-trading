"""Terminal dashboard (rich) with a plain-log fallback for non-TTY runs (systemd, nohup)."""
from __future__ import annotations

import asyncio
import logging
import sys
import time

from arb.config import effective_t1
from arb.engine.engine import Engine

log = logging.getLogger("arb.ui")


def _p(x, nd=3) -> str:
    return "-" if x is None else f"{x:+.{nd}f}"


def _px(lv) -> str:
    return "-" if lv is None else f"{float(lv.price):.6g}"


def build_view(eng: Engine):
    from rich.console import Group
    from rich.table import Table
    from rich.text import Text

    now = time.time()
    mode = "LIVE" if eng.live else "MONITOR"
    act = f"{eng.active.maker} maker -> {eng.active.taker} taker" if eng.active else "no active pair (monitor only)"
    head = Text(f"[{mode}] active: {act}   close_only={eng.cfg.close_only}   up {int(now - eng.started)}s", style="bold")
    if eng.halted:
        head.append(f"\nHALTED: {eng.halted}", style="bold red")

    vt = Table(title="venues", expand=False)
    for col in ("venue", "enabled", "conn", "trading", "books", "equity", "available"):
        vt.add_column(col)
    for name, v in eng.venues.items():
        b = eng.balances.get(name)
        vt.add_row(name, "yes" if eng.cfg.venues[name].enabled else "no", "ok" if v.connected else "--",
                   "yes" if v.trading else "no", str(sum(1 for bk in v.books.values() if bk.is_valid())),
                   "-" if b is None else f"{float(b.equity):.2f}", "-" if b is None else f"{float(b.available):.2f}")

    t = Table(title="spreads (dev = edge vs basis EMA, %)", expand=True)
    for col in ("pair (maker->taker)", "sym", "maker bid/ask", "taker bid/ask", "basis", "dev sell", "dev buy",
                "T1", "next open", "next close", "grids", "status"):
        t.add_column(col, overflow="fold")
    for c in eng.ctxs:
        M, T = eng.venues[c.pair.maker], eng.venues[c.pair.taker]
        mb, tb = M.book(c.maker_sym), T.book(c.taker_sym)
        sp = eng.positions.get(c.pair.key, c.sym.key)
        grids = "-"
        if sp.grids:
            grids = f"{sp.direction} " + ",".join(f"L{g.level}:{g.qty}" for g in sp.grids)
        if sp.residual_maker or sp.residual_taker:
            grids += f" res {sp.residual_maker}/{sp.residual_taker}"
        active = eng.active is not None and c.pair == eng.active
        style = "bold" if active else "dim"
        basis = f"{c.basis_value:+.3f}{'' if c.ready else '*'}"
        t.add_row(f"{c.pair.maker}->{c.pair.taker}", c.sym.key,
                  f"{_px(mb.best_bid if mb else None)}/{_px(mb.best_ask if mb else None)}",
                  f"{_px(tb.best_bid if tb else None)}/{_px(tb.best_ask if tb else None)}",
                  basis, _p(c.q_sell.dev if c.q_sell else None), _p(c.q_buy.dev if c.q_buy else None),
                  f"{effective_t1(c.sym.strategy, now):.2f}",
                  _p(c.next_open, 2) if c.next_open is not None else "-",
                  _p(c.next_close, 2) if c.next_close is not None else "-",
                  grids, c.disabled or c.status, style=style)
    ev = Text("\n".join(eng.events) or "(no events)")
    return Group(head, vt, t, Text("events:", style="bold"), ev, Text("basis*: EMA warming up / not ready", style="dim"))


async def run_ui(eng: Engine, rich_ui: bool = True) -> None:
    refresh = max(0.25, eng.cfg.ui_refresh_s)
    if rich_ui and sys.stdout.isatty():
        try:
            from rich.live import Live
        except ImportError:
            Live = None  # type: ignore
        if Live is not None:
            with Live(build_view(eng), refresh_per_second=max(1, int(1 / refresh)), screen=False) as live:
                while not eng.stopping:
                    await asyncio.sleep(refresh)
                    try:
                        live.update(build_view(eng))
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
