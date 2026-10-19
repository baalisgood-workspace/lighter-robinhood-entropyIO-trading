"""Command line entry point: check | monitor | live | positions | report."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys
from pathlib import Path

from arb.config import VENUE_NAMES, AppConfig, ConfigError, effective_t1, load_config

log = logging.getLogger("arb.cli")


def _overrides(args) -> dict:
    ov: dict = {}
    if getattr(args, "venues", None):
        chosen = [v.strip() for v in args.venues.split(",") if v.strip()]
        bad = [v for v in chosen if v not in VENUE_NAMES]
        if bad:
            raise ConfigError(f"--venues: unknown {bad}; allowed {list(VENUE_NAMES)}")
        ov["venues"] = {n: {"enabled": n in chosen} for n in VENUE_NAMES}
    if getattr(args, "close_only", False):
        ov.setdefault("trading", {})["close_only"] = True
    if getattr(args, "no_watch_all", False):
        ov.setdefault("market_data", {})["watch_all_venues"] = False
    elif getattr(args, "watch_all", False) or getattr(args, "cmd", "") == "monitor":
        ov.setdefault("market_data", {})["watch_all_venues"] = True
    return ov


def _load(args) -> AppConfig:
    try:
        from dotenv import load_dotenv
        load_dotenv(args.env_file, override=False)
    except ImportError:
        pass
    return load_config(args.config, _overrides(args))


def describe(cfg: AppConfig) -> str:
    lines = [f"config: {cfg.source}"]
    en = cfg.enabled_venues()
    lines.append(f"enabled venues: {en or 'none'}")
    if len(en) == 2:
        p = cfg.active_pair()
        lines.append(f"active pair: {p.key}  maker={p.maker}  taker={p.taker}")
        syms = cfg.symbols_for_pair(p)
        lines.append(f"tradable symbols ({len(syms)}):")
        for s in syms:
            st = s.strategy
            lines.append(f"  {s.key:<10} {p.maker}:{s.venues[p.maker]:<10} {p.taker}:{s.venues[p.taker]:<10} "
                         f"T1={effective_t1(st):.2f}% step={st.grid_step_pct:.2f}% grids={st.max_grids} "
                         f"grid=${st.grid_value_usdc:.0f} order=${st.order_value_usdc:.0f}"
                         f"{' close_only' if s.close_only or cfg.close_only else ''}")
    else:
        lines.append("active pair: none (enable exactly two venues to trade; monitor still works)")
    lines.append(f"watched venues (market data): {cfg.watched_venues()}")
    for p in cfg.watched_pairs():
        lines.append(f"  pair {p.key}: maker={p.maker} -> taker={p.taker}, {len(cfg.symbols_for_pair(p))} symbols")
    for n in VENUE_NAMES:
        v = cfg.venues[n]
        fee = "exchange-reported" if v.maker_fee_pct is None else f"maker {v.maker_fee_pct}% / taker {v.taker_fee_pct}%"
        creds = _creds_present(cfg, n)
        lines.append(f"venue {n:<11} enabled={v.enabled!s:<5} fees: {fee}; credentials in env: {creds}")
    return "\n".join(lines)


def _creds_present(cfg: AppConfig, name: str) -> bool:
    v = cfg.venues[name]
    if v.kind == "hyperliquid":
        keys = [f"{v.env_prefix}_ACCOUNT_ADDRESS", f"{v.env_prefix}_API_PRIVATE_KEY"]
    elif v.kind == "arcus":
        keys = [f"{v.env_prefix}_ACCOUNT_ADDRESS", f"{v.env_prefix}_API_KEY", f"{v.env_prefix}_API_SIGNING_KEY"]
    else:
        keys = [f"{v.env_prefix}_ACCOUNT_INDEX", f"{v.env_prefix}_API_KEY_INDEX", f"{v.env_prefix}_API_KEY_PRIVATE_KEY"]
    return all(os.environ.get(k) for k in keys)


def instance_dir(cfg: AppConfig, live: bool) -> Path:
    """Each live pair has its own state directory, so several pairs can run in parallel."""
    return Path(cfg.data_dir) / (cfg.active_pair().key if live else "monitor")


@contextlib.contextmanager
def _lock(d: Path):
    import fcntl
    d.mkdir(parents=True, exist_ok=True)
    path = d / "arb.lock"
    fh = path.open("w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise ConfigError(f"another instance is already running with this pair/mode (lock {path})")
    fh.write(str(os.getpid()))
    fh.flush()
    try:
        yield
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()


async def _start_venues(cfg: AppConfig, live: bool) -> dict:
    from arb.venues import build_venue
    active = cfg.active_pair() if live else None
    names = cfg.watched_venues()
    if active:
        names = list(dict.fromkeys([*names, active.maker, active.taker]))
    venues = {}
    try:
        for n in names:
            v = build_venue(cfg.venues[n])
            trading = bool(active and n in (active.maker, active.taker))
            await v.start(cfg.symbols_for_venue(n), trading=trading)
            venues[n] = v
            log.info("venue %s started (trading=%s, %d markets)", n, trading, len(v.markets))
    except Exception:
        await _stop_venues(venues)
        raise
    return venues


async def _stop_venues(venues: dict) -> None:
    for v in venues.values():
        try:
            await v.stop()
        except Exception as exc:  # noqa: BLE001
            log.warning("stop %s: %s", v.name, exc)


async def _run(cfg: AppConfig, live: bool, duration: float | None, ui: bool) -> int:
    from arb.engine.engine import Engine
    from arb.ui import run_ui

    venues = await _start_venues(cfg, live)
    eng = Engine(cfg, venues, live=live, data_dir=instance_dir(cfg, live))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    rc = 0
    try:
        eng.build_contexts()
        await asyncio.sleep(2.0)  # let books arrive
        if live:
            await eng.preflight()
        tasks = [asyncio.create_task(eng.run(), name="engine")]
        tasks.append(asyncio.create_task(run_ui(eng, rich_ui=ui), name="ui"))
        waiter = asyncio.create_task(stop.wait())
        timer = asyncio.create_task(asyncio.sleep(duration)) if duration else None
        done, _ = await asyncio.wait([*tasks, waiter] + ([timer] if timer else []), return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            if t in tasks and t.exception():
                log.error("task %s failed: %r", t.get_name(), t.exception())
                rc = 1
        waiter.cancel()
        if timer:
            timer.cancel()
    except ConfigError as exc:
        print(f"refusing to start: {exc}", file=sys.stderr)
        rc = 2
    finally:
        log.info("shutting down ...")
        await eng.shutdown()
        await asyncio.sleep(0.3)
        for t in asyncio.all_tasks():
            if t is not asyncio.current_task() and t.get_name() in ("engine", "ui"):
                t.cancel()
        await _stop_venues(venues)
    if eng.halted:
        print(f"HALTED: {eng.halted}", file=sys.stderr)
        rc = rc or 3
    return rc


async def _positions(cfg: AppConfig) -> int:
    from arb.engine.state import PositionBook
    from arb.venues import build_venue
    for n in VENUE_NAMES:
        if not _creds_present(cfg, n):
            print(f"{n}: no credentials in env, skipped")
            continue
        v = build_venue(cfg.venues[n])
        try:
            await v.start(cfg.symbols_for_venue(n), trading=False)
            bal = await v.balance()
            pos = await v.positions()
            print(f"{n}: equity={float(bal.equity):.2f} available={float(bal.available):.2f}")
            for sym, p in sorted(pos.items()):
                if p.qty != 0:
                    print(f"    {sym:<12} {p.qty:+}  entry={p.entry_price}")
        except Exception as exc:  # noqa: BLE001
            print(f"{n}: error {exc}")
        finally:
            await v.stop()
    print("local state (data/<pair>/positions.json):")
    any_open = False
    sps = [sp for f in sorted(Path(cfg.data_dir).glob("*/positions.json")) for sp in PositionBook(f).items.values()]
    for sp in sps:
        if not sp.is_flat():
            any_open = True
            print(f"  {sp.pair_key} {sp.symbol}: {sp.direction} grids="
                  f"{[(g.level, str(g.qty)) for g in sp.grids]} residual maker={sp.residual_maker} taker={sp.residual_taker}")
    if not any_open:
        print("  flat")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="arb", description="Lighter / Lighter-RH / Entropy (io) / Arcus maker->taker spread arbitrage (one pair per process)")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--env-file", default=".env")
    ap.add_argument("--log-level", default="INFO")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--venues", help="comma list overriding venues.*.enabled, e.g. lighter,entropy")
        p.add_argument("--no-watch-all", action="store_true", help="only stream data for the enabled venues")
        p.add_argument("--watch-all", action="store_true", help="also stream/display the other venues (default for monitor)")
        return p

    common(sub.add_parser("check", help="validate config and show pair/symbol mapping (offline)"))
    m = common(sub.add_parser("monitor", help="stream public data, compute spreads, never trade"))
    m.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    m.add_argument("--no-ui", action="store_true")
    lv = common(sub.add_parser("live", help="trade the two enabled venues"))
    lv.add_argument("--close-only", action="store_true", help="never open, only close existing grids")
    lv.add_argument("--yes", action="store_true", help="skip interactive confirmation")
    lv.add_argument("--no-ui", action="store_true")
    common(sub.add_parser("positions", help="show exchange balances/positions and local state"))
    r = sub.add_parser("report", help="spread statistics from recorded samples")
    r.add_argument("--threshold", type=float, default=0.30)
    r.add_argument("--days", help="comma list of YYYY-MM-DD")
    args = ap.parse_args(argv)

    try:
        cfg = _load(args)
    except (ConfigError, FileNotFoundError) as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    if args.cmd == "check":
        print(describe(cfg))
        return 0
    if args.cmd == "report":
        from arb.report import print_report
        days = args.days.split(",") if args.days else None
        return print_report(Path(cfg.data_dir), args.threshold, days)

    from arb.util import setup_logging
    use_ui = args.cmd in ("monitor", "live") and not args.no_ui and sys.stdout.isatty()
    setup_logging(cfg.log_dir, args.log_level, console=not use_ui)

    if args.cmd == "positions":
        return asyncio.run(_positions(cfg))

    live = args.cmd == "live"
    if live:
        try:
            pair = cfg.active_pair()
        except ConfigError as exc:
            print(f"config error: {exc}", file=sys.stderr)
            return 2
        missing = [n for n in (pair.maker, pair.taker) if not _creds_present(cfg, n)]
        if missing:
            print(f"missing credentials for {missing} (see .env.example)", file=sys.stderr)
            return 2
        print(describe(cfg))
        if not args.yes:
            ans = input(f"\nStart LIVE trading {pair.maker} (maker) -> {pair.taker} (taker)? type 'yes': ").strip()
            if ans != "yes":
                print("aborted")
                return 1
    try:
        with _lock(instance_dir(cfg, live)):
            return asyncio.run(_run(cfg, live, getattr(args, "duration", None), use_ui))
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
