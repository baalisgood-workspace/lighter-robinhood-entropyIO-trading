"""Command line entry point: check | monitor | live | positions | fees | report."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys
import time
from pathlib import Path

from arb.config import VENUE_NAMES, AppConfig, ConfigError, auto_requested, effective_t1, parse_config, pair_key, read_raw, validate

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
    raw = read_raw(args.config, _overrides(args))
    if getattr(args, "all_symbols", False):
        raw.setdefault("auto_symbols", {})["enabled"] = True
    added: list[str] = []
    if auto_requested(raw) and args.cmd in ("check", "monitor", "live", "positions", "accept-flat"):
        from arb.autosym import expand, fetch_listing
        listing, meta, src = fetch_listing(raw)
        only = None
        if args.cmd != "monitor" and not (raw.get("market_data") or {}).get("watch_all_venues", False):
            en = [n for n in VENUE_NAMES if ((raw.get("venues") or {}).get(n) or {}).get("enabled")]
            only = en if len(en) == 2 else None
        raw, added = expand(raw, listing, meta, only)
        print(f"auto_symbols: +{len(added)} symbol(s) from {src} market lists", file=sys.stderr)
    cfg = parse_config(raw)
    cfg.source = str(args.config)
    _apply_cli_modes(cfg, args)
    return cfg


def _apply_cli_modes(cfg: AppConfig, args) -> None:
    """--maker / --open-mode / --close-mode win over config.yaml (all symbols of this run)."""
    maker = getattr(args, "maker", None)
    if maker:
        en = cfg.enabled_venues()
        if len(en) != 2 or maker not in en:
            raise ConfigError(f"--maker {maker}: must be one of the two enabled venues {en} (use --venues a,b)")
        cfg.roles[pair_key(*en)] = maker
    for attr in ("open_mode", "close_mode"):
        val = getattr(args, attr, None)
        if val:
            for s in cfg.symbols.values():
                setattr(s.execution, attr, val)
    errs = validate(cfg)
    if errs:
        raise ConfigError("invalid configuration:\n  - " + "\n  - ".join(errs))


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
                         f"{' close_only' if s.close_only or cfg.close_only else ''}"
                         f"{(' auto:' + s.category) if s.auto else ''}")
        ex = syms[0].execution if syms else None
        if ex:
            lines.append(f"execution: open {ex.open_mode}, close {ex.close_mode}; auto symbols trade: {cfg.auto_symbols.trade}")
    else:
        lines.append("active pair: none (enable exactly two venues to trade; monitor still works)")
    lines.append(f"watched venues (market data): {cfg.watched_venues()}")
    for p in cfg.watched_pairs():
        lines.append(f"  pair {p.key}: maker={p.maker} -> taker={p.taker}, {len(cfg.symbols_for_pair(p))} symbols")
    for n in VENUE_NAMES:
        v = cfg.venues[n]
        fee = "exchange-reported" if v.maker_fee_pct is None else f"maker {v.maker_fee_pct}% / taker {v.taker_fee_pct}%"
        if v.maker_fee_pct is not None and v.fee_source == "auto" and v.kind in ("lighter", "arcus"):
            fee = f"exchange/account-reported (fallback {fee}; see: arb fees)"
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


async def _fees(cfg: AppConfig) -> int:
    """v5.4: fees the bot will use per symbol (exchange/account-reported, read-only; safe while the bot runs)."""
    from arb.engine.signals import roundtrip_fee_pct
    from arb.venues import build_venue
    try:
        pair = cfg.active_pair()
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    vs = {}
    try:
        for n in (pair.maker, pair.taker):
            v = build_venue(cfg.venues[n])
            await v.start(cfg.symbols_for_venue(n), trading=False)
            vs[n] = v
            if cfg.venues[n].kind == "lighter":
                try:
                    await v.load_account_fees()
                except Exception as exc:  # noqa: BLE001
                    print(f"{n}: account fee tier not readable: {exc}")
            c = cfg.venues[n]
            print(f"{n}: fee_source={c.fee_source}  {v.fee_summary() or 'no account tier'}  "
                  f"(configured fallback maker {c.maker_fee_pct}% / taker {c.taker_fee_pct}%)")
        M, T = vs[pair.maker], vs[pair.taker]
        print(f"\n{'symbol':<10} {pair.maker + ' maker%':>14} {pair.maker + ' taker%':>14} {pair.taker + ' taker%':>14} "
              f"{'roundtrip%':>10}  source")
        for s in sorted(cfg.symbols_for_pair(pair), key=lambda s: s.key):
            ms, ts = s.venues[pair.maker], s.venues[pair.taker]
            if ms not in M.markets or ts not in T.markets:
                print(f"{s.key:<10} (not listed)")
                continue
            mk, mt, tt = M.maker_fee_pct(ms), M.taker_fee_pct(ms), T.taker_fee_pct(ts)
            rt = roundtrip_fee_pct(mk, tt, s.execution.close_mode == "taker_taker", mt,
                                   open_taker_taker=s.execution.open_mode == "taker_taker")
            print(f"{s.key:<10} {mk:>14.5f} {mt:>14.5f} {tt:>14.5f} {rt:>10.4f}  {M.fee_source(ms)} / {T.fee_source(ts)}")
        print("\nroundtrip% is added to the open threshold (open = T1 + grid step + roundtrip).")
        return 0
    finally:
        await _stop_venues(vs)


async def _accept_flat(cfg: AppConfig, confirm: bool) -> int:
    """Operator tool (ported from arcus-maker-lighter-taker operator_resolve_hedge --accept-flat): after the
    position was closed by hand, reset the local grids of a symbol to flat - only when BOTH venues show
    no position and no resting order for it. Dry run unless --confirm."""
    import json
    from arb.engine.state import PositionBook, SymbolPosition
    from arb.venues import build_venue
    pair = cfg.active_pair()
    d = instance_dir(cfg, True)
    book = PositionBook(d / "positions.json")
    open_sps = [sp for sp in book.items.values() if sp.pair_key == pair.key and not sp.is_flat()]
    if not open_sps:
        print(f"{d / 'positions.json'}: no open grids for {pair.key}")
        return 0
    missing = [n for n in (pair.maker, pair.taker) if not _creds_present(cfg, n)]
    if missing:
        print(f"missing credentials for {missing}", file=sys.stderr)
        return 2
    venues = {}
    actual: dict[str, dict] = {}
    try:
        for n in (pair.maker, pair.taker):
            v = build_venue(cfg.venues[n])
            await v.start(cfg.symbols_for_venue(n), trading=False)
            venues[n] = v
            actual[n] = await v.positions()
    finally:
        await _stop_venues(venues)
    ok = []
    for sp in open_sps:
        sc = cfg.symbols.get(sp.symbol)
        if sc is None:
            print(f"  {sp.symbol}: not in config, skipped")
            continue
        qm = actual[pair.maker].get(sc.venues[pair.maker])
        qt = actual[pair.taker].get(sc.venues[pair.taker])
        qm = qm.qty if qm else 0
        qt = qt.qty if qt else 0
        local = f"local maker {sp.expected_maker_qty()} / taker {sp.expected_taker_qty()}"
        if qm == 0 and qt == 0:
            ok.append(sp)
            print(f"  {sp.symbol}: both venues flat, {local} -> will reset to flat")
        else:
            print(f"  {sp.symbol}: NOT flat on the venues ({pair.maker} {qm}, {pair.taker} {qt}; {local}) - unchanged")
    if not ok:
        return 1
    if not confirm:
        print("dry run: nothing written (add --confirm; make sure no order is resting for these symbols)")
        return 0
    for sp in ok:
        fresh = SymbolPosition(pair_key=sp.pair_key, symbol=sp.symbol, realized_edge_usdc=sp.realized_edge_usdc,
                               n_open=sp.n_open, n_close=sp.n_close)
        book.items[PositionBook.key(sp.pair_key, sp.symbol)] = fresh
    book.save()
    with (d / "events.jsonl").open("a") as f:
        for sp in ok:
            f.write(json.dumps({"ts": time.time(), "kind": "operator_accept_flat", "pair": pair.key, "symbol": sp.symbol,
                                "previous": sp.to_json()}, default=str) + "\n")
    print(f"reset {len(ok)} symbol(s) to flat in {d / 'positions.json'}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="arb", description="Lighter / Lighter-RH / Entropy (io) / Arcus maker->taker spread arbitrage (one pair per process)")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--env-file", default=".env")
    ap.add_argument("--log-level", default="INFO")
    sub = ap.add_subparsers(dest="cmd", required=True)
    # the global options are also accepted after the sub-command (./start.sh check --config ...)
    g = argparse.ArgumentParser(add_help=False)
    g.add_argument("--config", default=argparse.SUPPRESS)
    g.add_argument("--env-file", default=argparse.SUPPRESS)
    g.add_argument("--log-level", default=argparse.SUPPRESS)
    _add_parser = sub.add_parser
    sub.add_parser = lambda *a, **k: _add_parser(*a, parents=[g, *k.pop("parents", [])], **k)

    def common(p):
        p.add_argument("--venues", help="comma list overriding venues.*.enabled, e.g. lighter,entropy")
        p.add_argument("--no-watch-all", action="store_true", help="only stream data for the enabled venues")
        p.add_argument("--watch-all", action="store_true", help="also stream/display the other venues (default for monitor)")
        p.add_argument("--all-symbols", action="store_true", help="add every symbol listed on >= 2 venues (auto_symbols)")
        p.add_argument("--maker", choices=VENUE_NAMES, help="maker (quoting) venue of the pair, overrides roles:")
        p.add_argument("--open-mode", choices=("maker_taker", "taker_taker"), help="override execution.open_mode")
        p.add_argument("--close-mode", choices=("maker_taker", "taker_taker"), help="override execution.close_mode")
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
    common(sub.add_parser("fees", help="fees per symbol from the exchanges / account tiers (read-only)"))
    ul = common(sub.add_parser("unlock", help="clear the manual-intervention lock of a pair after you fixed it"))
    ul.add_argument("--yes", action="store_true")
    af = common(sub.add_parser("accept-flat", help="reset local grids to flat when BOTH venues are verifiably flat (manual close)"))
    af.add_argument("--confirm", action="store_true", help="write the change (default: dry run)")
    mk = sub.add_parser("markets", help="which symbols are listed on which venue (public, read-only)")
    mk.add_argument("--symbols", help="comma list to show only these")
    mk.add_argument("--min-venues", type=int, default=2)
    mk.add_argument("--yaml", action="store_true", help="print config snippets for listed symbols that are not configured")
    r = sub.add_parser("report", help="spread statistics from recorded samples")
    r.add_argument("--threshold", type=float, default=0.30)
    r.add_argument("--days", help="comma list of YYYY-MM-DD")
    r.add_argument("--chart", action="store_true", help="also write PNG charts to data/charts/ (needs matplotlib)")
    r.add_argument("--symbols", help="comma list for --chart")
    rk = common(sub.add_parser("rank", help="score symbols from recorded samples (opportunities/day, excess edge, liquidity)"))
    rk.add_argument("--mode", choices=("maker_taker", "taker_taker"), default=None, help="open mode to score (default: config)")
    rk.add_argument("--days", help="comma list of YYYY-MM-DD")
    rk.add_argument("--pair", help="only this pair, e.g. lighter+arcus")
    rk.add_argument("--top", type=int, default=30)
    rk.add_argument("--save", action="store_true", help="write data/rank.json (used by auto_symbols.trade: ranked)")
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
        from arb.report import print_report, print_trades
        days = args.days.split(",") if args.days else None
        rc = print_report(Path(cfg.data_dir), args.threshold, days)
        print_trades(Path(cfg.data_dir))
        if args.chart:
            from arb.report import write_charts
            try:
                files = write_charts(Path(cfg.data_dir), Path(cfg.data_dir) / "charts", args.threshold, days,
                                     args.symbols.split(",") if args.symbols else None)
            except RuntimeError as exc:
                print(f"chart: {exc}", file=sys.stderr)
                return 2
            print(f"\n{len(files)} chart(s) written to {Path(cfg.data_dir) / 'charts'}")
        return rc
    if args.cmd == "rank":
        from arb.report import print_rank
        mode = args.mode or (next(iter(cfg.symbols.values())).execution.open_mode if cfg.symbols else "maker_taker")
        return print_rank(Path(cfg.data_dir), mode, args.days.split(",") if args.days else None, args.pair, args.top,
                          args.save, cfg)
    if args.cmd == "markets":
        from arb.markets import fetch_all, render
        listing = asyncio.run(fetch_all(cfg))
        print(render(cfg, listing, args.symbols.split(",") if args.symbols else None, args.min_venues, args.yaml))
        return 0
    if args.cmd == "unlock":
        try:
            lock = instance_dir(cfg, True) / "MANUAL_INTERVENTION.json"
        except ConfigError as exc:
            print(f"config error: {exc}", file=sys.stderr)
            return 2
        if not lock.exists():
            print(f"no lock at {lock}")
            return 0
        print(lock.read_text())
        if not args.yes and input("Positions verified/fixed on both venues and in positions.json? type 'yes': ").strip() != "yes":
            print("aborted")
            return 1
        lock.rename(lock.with_name(f"MANUAL_INTERVENTION.resolved-{int(time.time())}.json"))
        print("lock cleared")
        return 0

    from arb.util import setup_logging
    use_ui = args.cmd in ("monitor", "live") and not args.no_ui and sys.stdout.isatty()
    setup_logging(cfg.log_dir, args.log_level, console=not use_ui)

    if args.cmd == "positions":
        return asyncio.run(_positions(cfg))
    if args.cmd == "fees":
        return asyncio.run(_fees(cfg))
    if args.cmd == "accept-flat":
        try:
            with _lock(instance_dir(cfg, True)):
                return asyncio.run(_accept_flat(cfg, args.confirm))
        except ConfigError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2

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
