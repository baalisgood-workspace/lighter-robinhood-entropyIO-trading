"""Entropy adapter.

Entropy (https://entropy.io) is a Hyperliquid HIP-3 market deployer with dex name
"io"; its markets are traded through the Hyperliquid API with coin names such as
"io:OAI". Orders are signed with the official hyperliquid-python-sdk.

Credentials (environment / .env):
    ENTROPY_ACCOUNT_ADDRESS   master account address (0x...) that holds the funds
    ENTROPY_API_PRIVATE_KEY   private key of an API/agent wallet approved for that
                              account (https://app.hyperliquid.xyz/API) - or the
                              master key itself (not recommended)
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Optional

import aiohttp

from arb.models import Balance, Level, MarketInfo, OrderBook, OrderState, OrderStatus, Position, liq_price
from arb.util import D, RateLimiter, new_client_index
from arb.venues.base import Venue, VenueError
from arb.venues.transport import harden_requests_session, http_session, ws_connect

log = logging.getLogger("arb.entropy")

HL_MIN_NOTIONAL = Decimal("10")
BOOK_DEPTH = 20
NO_MATCH_HINT = "could not immediately match"
# v5.9.4 address action budget rejection, e.g. "Too many cumulative requests sent (41192 > 41086) for cumulative
# volume traded $31087.88. Place taker orders to free up 1 request per USDC traded."
CUMULATIVE_RE = re.compile(r"cumulative requests sent \((\d+)\s*>\s*(\d+)\)", re.I)


def hl_price_decimals(price: Decimal, sz_decimals: int) -> int:
    """Hyperliquid perps: <= 5 significant figures and <= 6 - szDecimals decimals (integers always valid)."""
    if price <= 0:
        raise ValueError("price must be positive")
    exp = price.adjusted()  # floor(log10(price))
    return max(0, min(6 - sz_decimals, 4 - exp))


def hl_round_price(price: Decimal, sz_decimals: int, up: bool) -> Decimal:
    dec = hl_price_decimals(price, sz_decimals)
    q = Decimal(1).scaleb(-dec)
    out = price.quantize(q, rounding=ROUND_CEILING if up else ROUND_FLOOR)
    # Rounding can carry into a new power of ten (e.g. 9999.95 -> 10000.0); re-check sig figs.
    if hl_price_decimals(out, sz_decimals) < dec:
        out = hl_round_price(out, sz_decimals, up)
    return out


def map_hl_status(raw: str) -> OrderStatus:
    raw = (raw or "").lower()
    if raw == "open" or raw == "triggered":
        return OrderStatus.OPEN
    if raw == "filled":
        return OrderStatus.FILLED
    if raw == "rejected" or raw.endswith("rejected"):
        return OrderStatus.REJECTED
    if "cancel" in raw:
        return OrderStatus.CANCELED
    return OrderStatus.UNKNOWN


UNIFIED_MODES = ("unifiedAccount", "portfolioMargin")


class EntropyVenue(Venue):
    def __init__(self, cfg, env=None):
        super().__init__(cfg, env)
        self.dex = cfg.dex
        self.address = (self.env.get("ENTROPY_ACCOUNT_ADDRESS") or "").strip()
        self._private_key = (self.env.get("ENTROPY_API_PRIVATE_KEY") or "").strip()
        self.limiter = RateLimiter(cfg.requests_per_minute)
        self.exchange = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._by_oid: dict[int, str] = {}  # oid -> client_id (cloid raw)
        self._fills: dict[int, dict[int, tuple[Decimal, Decimal]]] = {}  # oid -> tid -> (sz, px)
        self._collateral_token = 0  # dex collateral (0 = USDC), from meta
        self._dex_empty_warned = False
        # v5.9.4 action budget (Hyperliquid per-address limit): exchange numbers + actions sent since the last read
        self._act_used: Optional[int] = None
        self._act_cap: Optional[int] = None
        self._act_vlm: Optional[float] = None
        self._act_since = 0  # actions sent since the last userRateLimit read
        self._act_last = 0.0  # wall time of the last new order (place) sent
        self._act_mode = "ok"  # ok | low | over (logged on change)
        self._act_last_log = 0.0
        self._act_reject_log = 0.0

    @property
    def account_id(self) -> str:
        return (self.address or "none").lower()

    def has_credentials(self) -> bool:
        return bool(self.address and self._private_key)

    # ------------------------------------------------------------ lifecycle
    async def start(self, symbols: list[str], trading: bool) -> None:
        self._running = True
        self._session = http_session(15)
        await self._load_markets(symbols)
        if trading:
            if not self.has_credentials():
                raise VenueError("entropy: missing ENTROPY_ACCOUNT_ADDRESS / ENTROPY_API_PRIVATE_KEY")
            self.exchange = await asyncio.to_thread(self._build_exchange)
            for sym in self.markets:
                await self._ensure_leverage(sym)
        self.trading = trading
        await self._poll_funding()
        self._tasks.append(asyncio.create_task(self._ws_loop(), name="entropy-ws"))
        self._tasks.append(asyncio.create_task(self._funding_loop(), name="entropy-funding"))
        if trading and self.cfg.action_budget:
            try:
                await self.refresh_action_budget()
            except Exception as exc:  # noqa: BLE001 - tracked again by the loop
                log.warning("entropy: userRateLimit not readable at start: %s", exc)
            self._tasks.append(asyncio.create_task(self._action_loop(), name="entropy-actions"))

    # ------------------------------------------------------------ v5.9.4 action budget
    async def refresh_action_budget(self) -> None:
        d = await self._info({"type": "userRateLimit", "user": self.address})
        used, cap = d.get("nRequestsUsed"), d.get("nRequestsCap")
        if used is None or cap is None:
            raise VenueError(f"userRateLimit without nRequestsUsed/nRequestsCap: {str(d)[:200]}")
        self._act_used, self._act_cap, self._act_since = int(used), int(cap), 0
        self._act_vlm = float(d.get("cumVlm") or 0)
        self._log_action_mode()

    async def _action_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.cfg.action_refresh_s)
            try:
                await self.refresh_action_budget()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.debug("entropy userRateLimit: %s", exc)

    def actions_left(self) -> Optional[int]:
        if self._act_cap is None or self._act_used is None:
            return None
        return self._act_cap - self._act_used - self._act_since

    def _note_action(self, new_order: bool) -> None:
        self._act_since += 1
        if new_order:
            self._act_last = time.time()

    def _note_budget_rejection(self, text: str) -> bool:
        """The exchange refused an order for the address budget: take its numbers (no venue-wide backoff - the
        spacing in action_wait_s already paces new orders at the rate the exchange still accepts)."""
        m = CUMULATIVE_RE.search(text or "")
        if not m:
            return False
        self._act_used, self._act_cap, self._act_since = int(m.group(1)), int(m.group(2)), 0
        now = time.time()
        if now - self._act_reject_log >= 300:
            self._act_reject_log = now
            log.warning("entropy: action budget used up (%s); new orders now one per %.0fs - trading volume (or a "
                        "purchased budget) raises it", text[:160], self.cfg.action_over_gap_s)
        self._log_action_mode()
        return True

    def _log_action_mode(self) -> None:
        left = self.actions_left()
        if left is None:
            return
        mode = "over" if left <= 0 else ("low" if left <= self.cfg.action_soft_reserve else "ok")
        now = time.time()
        if mode != self._act_mode or (mode != "ok" and now - self._act_last_log >= 600):
            lvl = logging.INFO if mode == "ok" else logging.WARNING
            log.log(lvl, "entropy action budget: %d left (used %d / cap %d, cumulative volume %.0f USDC) -> %s",
                    left, self._act_cap - left, self._act_cap, self._act_vlm or 0.0,
                    {"ok": "normal", "low": f"new orders spaced {self.cfg.action_low_gap_s:.0f}s apart",
                     "over": f"over the budget, new orders one per {self.cfg.action_over_gap_s:.0f}s"}[mode])
            self._act_mode, self._act_last_log = mode, now

    def action_wait_s(self, now: Optional[float] = None) -> float:
        if not self.cfg.action_budget:
            return 0.0
        left = self.actions_left()
        if left is None or left > self.cfg.action_soft_reserve:
            return 0.0
        gap = self.cfg.action_low_gap_s if left > 0 else self.cfg.action_over_gap_s
        return max(0.0, self._act_last + gap - (time.time() if now is None else now))

    def action_budget_text(self) -> str:
        left = self.actions_left()
        if not self.cfg.action_budget or left is None or left > self.cfg.action_soft_reserve:
            return ""
        if left <= 0:
            return (f"entropy 下单次数额度已用完（超出 {-left} 次），每 {self.cfg.action_over_gap_s:.0f} 秒最多挂一单；"
                    f"成交越多额度越多")
        return f"entropy 下单次数额度只剩 {left} 次，挂单间隔放慢到 {self.cfg.action_low_gap_s:.0f} 秒"

    def _build_exchange(self):
        from eth_account import Account
        from hyperliquid.exchange import Exchange

        wallet = Account.from_key(self._private_key)
        ex = Exchange(wallet, self.cfg.rest_url, account_address=self.address, perp_dexs=[self.dex])
        harden_requests_session(getattr(ex, "session", None))  # signed actions never follow redirects
        harden_requests_session(getattr(getattr(ex, "info", None), "session", None))
        for sym in self.markets:
            if sym not in ex.info.name_to_coin:
                raise VenueError(f"entropy: SDK does not know {sym}")
        return ex

    async def _ensure_leverage(self, sym: str) -> None:
        lev = min(self.cfg.leverage, int(self.markets[sym].extra.get("max_leverage", self.cfg.leverage)))
        await self.limiter.acquire()
        res = await asyncio.to_thread(self.exchange.update_leverage, lev, sym, False)  # isolated
        if not isinstance(res, dict) or res.get("status") != "ok":
            log.warning("entropy: update_leverage %s x%s isolated -> %s", sym, lev, res)

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tasks.clear()
        if self._session:
            await self._session.close()

    # ------------------------------------------------------------ REST info
    async def _info(self, payload: dict, cost: float = 1.0) -> Any:
        await self.limiter.acquire(cost)
        assert self._session is not None
        try:
            async with self._session.post(self.cfg.rest_url.rstrip("/") + "/info", json=payload, allow_redirects=False) as r:
                text = await r.text()
                if r.status != 200:
                    raise VenueError(f"entropy /info {payload.get('type')} -> HTTP {r.status}: {text[:200]}")
                return json.loads(text)
        except Exception as exc:  # noqa: BLE001
            self.note_failure(f"info {payload.get('type')}", f"{type(exc).__name__}: {exc}")
            raise

    async def _load_markets(self, symbols: list[str]) -> None:
        meta = await self._info({"type": "meta", "dex": self.dex})
        self._collateral_token = int(meta.get("collateralToken") or 0)
        universe = {u["name"]: u for u in meta.get("universe", [])}
        for sym in symbols:
            u = universe.get(sym)
            if u is None:
                log.warning("entropy: market %s not listed on dex %s, skipped", sym, self.dex)
                continue
            if u.get("isDelisted"):
                log.warning("entropy: market %s is delisted, skipped", sym)
                continue
            szd = int(u["szDecimals"])
            step = Decimal(1).scaleb(-szd)
            self.markets[sym] = MarketInfo(
                venue=self.name, symbol=sym, size_step=step, min_qty=step, min_notional=HL_MIN_NOTIONAL,
                price_tick=None, extra={"sz_decimals": szd, "max_leverage": u.get("maxLeverage"),
                                        "margin_mode": u.get("marginMode"), "only_isolated": u.get("onlyIsolated")},
            )

    async def _poll_funding(self) -> None:
        try:
            meta, ctxs = await self._info({"type": "metaAndAssetCtxs", "dex": self.dex})
            for u, c in zip(meta.get("universe", []), ctxs):
                if u["name"] in self.markets and c.get("funding") not in (None, ""):
                    self.funding_hourly[u["name"]] = float(c["funding"])  # decimal per hour
        except Exception as exc:  # noqa: BLE001
            log.debug("entropy funding poll failed: %s", exc)

    async def _funding_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.cfg.funding_poll_seconds)
            await self._poll_funding()

    async def positions(self) -> dict[str, Position]:
        st = await self._info({"type": "clearinghouseState", "user": self.address, "dex": self.dex}, cost=2)
        out: dict[str, Position] = {}
        for ap in st.get("assetPositions") or []:
            p = ap.get("position") or {}
            sym = p.get("coin")
            if sym in self.markets:
                out[sym] = Position(self.name, sym, D(p.get("szi") or 0), D(p["entryPx"]) if p.get("entryPx") else None,
                                    liq_price(p.get("liquidationPx")))
        return out

    async def balance(self) -> Balance:
        """v5.7.3: unified / portfolio margin accounts keep all balances and holds in the spot clearinghouse state;
        the per-dex state is not meaningful there (Hyperliquid docs: account abstraction modes). Other modes
        have a separate balance per dex. An unreadable mode raises: never report funds we could not verify."""
        mode = await self._info({"type": "userAbstraction", "user": self.address})
        st = await self._info({"type": "clearinghouseState", "user": self.address, "dex": self.dex}, cost=2)
        if mode in UNIFIED_MODES:
            spot = await self._info({"type": "spotClearinghouseState", "user": self.address}, cost=2)
            row = next((b for b in spot.get("balances") or [] if b.get("token") == self._collateral_token), None) or {}
            total, hold = D(row.get("total") or 0), D(row.get("hold") or 0)
            # v5.9.2: the spot total already includes the open positions' unrealized PnL (its hold equals the
            # dex accountValue = margin + uPnL, and it moves with the mark price); adding uPnL again counted it twice
            return Balance(self.name, equity=total, available=max(D(0), total - hold))
        ms = st.get("marginSummary") or {}
        equity = D(ms.get("accountValue") or 0)
        if equity == 0 and not self._dex_empty_warned:
            self._dex_empty_warned = True
            log.warning("entropy: account mode %s keeps a separate balance per dex and dex %s holds 0: transfer USDC to it "
                        "(sendAsset) or switch the account to unified mode", mode, self.dex)
        return Balance(self.name, equity=equity, available=D(st.get("withdrawable") or 0))

    # ------------------------------------------------------------ rounding
    def round_price(self, symbol: str, price: Decimal, up: bool) -> Decimal:
        return hl_round_price(price, self.market(symbol).extra["sz_decimals"], up)

    def price_tick_at(self, symbol: str, price: Decimal) -> Decimal:
        return Decimal(1).scaleb(-hl_price_decimals(price, self.market(symbol).extra["sz_decimals"]))

    # ------------------------------------------------------------ WebSocket
    async def _ws_loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                async with ws_connect(self.cfg.ws_url, self.mark_alive, self.cfg.liveness_s, self.cfg.quick_resume_s) as ws:
                    for sym in self.markets:
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": sym}}))
                    if self.trading:
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "orderUpdates", "user": self.address}}))
                        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "userFills", "user": self.address}}))
                        asyncio.create_task(self._refresh_open_orders())
                    self.on_connected()
                    backoff = 1.0
                    last_ping = time.monotonic()
                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            raw = None
                        if raw is not None:
                            self.alive_ts = time.time()
                        if time.monotonic() - last_ping > (1.0 if raw is None else 15.0):
                            await ws.send('{"method":"ping"}')
                            last_ping = time.monotonic()
                        if raw is not None:
                            self._on_ws(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("entropy WS error: %s; reconnect in %.0fs", exc, backoff)
                self.alarm("connection_timeout" if "ping" in str(exc).lower() or "timeout" in str(exc).lower() else "reconnect",
                           detail=str(exc))
            self.connected = False
            self.reset_books()
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def _on_ws(self, m: dict) -> None:
        ch = m.get("channel")
        data = m.get("data")
        if ch == "l2Book" and isinstance(data, dict):
            sym = data.get("coin")
            if sym not in self.markets:
                return
            levels = data.get("levels") or [[], []]
            self.books[sym] = OrderBook(
                venue=self.name, symbol=sym,
                bids=[Level(D(l["px"]), D(l["sz"])) for l in levels[0][:BOOK_DEPTH]],
                asks=[Level(D(l["px"]), D(l["sz"])) for l in levels[1][:BOOK_DEPTH]],
                ts_exchange=float(data.get("time", 0)) / 1000.0, ts_local=time.time(),
            )
            self.market_event.set()
        elif ch == "orderUpdates" and isinstance(data, list):
            for upd in data:
                self._apply_order_update(upd)
        elif ch == "userFills" and isinstance(data, dict) and not data.get("isSnapshot"):
            for f in data.get("fills") or []:
                self._apply_fill(f)
        elif ch == "error":
            log.warning("entropy WS error message: %s", str(m)[:300])

    def _client_for(self, order: dict) -> Optional[str]:
        cloid = order.get("cloid")
        if cloid and self._lookup(cloid) is not None:
            return cloid
        oid = order.get("oid")
        return self._by_oid.get(int(oid)) if oid is not None else None

    def _apply_order_update(self, upd: dict) -> None:
        o = upd.get("order") or {}
        cid = self._client_for(o)
        st = self._lookup(cid) if cid is not None else None
        if st is None:
            return
        filled = D(o.get("origSz") or 0) - D(o.get("sz") or 0)
        self._by_oid[int(o["oid"])] = cid
        status = map_hl_status(str(upd.get("status", "")))
        st.apply(status=status, filled=filled if filled > 0 else None, exchange_id=str(o.get("oid")),
                 reason=str(upd.get("status")) if status != OrderStatus.OPEN else "")
        self._recompute_avg(st)
        self._notify(cid)

    def _apply_fill(self, f: dict) -> None:
        oid = f.get("oid")
        if oid is None:
            return
        oid = int(oid)
        per = self._fills.setdefault(oid, {})
        per[int(f.get("tid", len(per)))] = (D(f["sz"]), D(f["px"]))
        cid = self._by_oid.get(oid) or (f.get("cloid") if self._lookup(str(f.get("cloid"))) is not None else None)
        st = self._lookup(cid) if cid else None
        if st is not None:
            total = sum(sz for sz, _ in per.values())
            st.apply(filled=total)
            self._recompute_avg(st)
            self._notify(cid)

    def _recompute_avg(self, st: OrderState) -> None:
        if not st.exchange_id:
            return
        per = self._fills.get(int(st.exchange_id))
        if per:
            q = sum(sz for sz, _ in per.values())
            if q > 0:
                st.avg_price = sum(sz * px for sz, px in per.values()) / q

    # ------------------------------------------------------------ orders
    def _parse_status(self, st: OrderState, res: Any, ioc: bool) -> None:
        if not isinstance(res, dict) or res.get("status") != "ok":
            st.apply(status=OrderStatus.REJECTED, reason=str(res)[:300])
            return
        statuses = (((res.get("response") or {}).get("data") or {}).get("statuses")) or []
        s = statuses[0] if statuses else {}
        if "resting" in s:
            oid = int(s["resting"]["oid"])
            self._by_oid[oid] = st.client_id
            st.apply(status=OrderStatus.OPEN, exchange_id=str(oid))
        elif "filled" in s:
            f = s["filled"]
            oid = int(f["oid"])
            self._by_oid[oid] = st.client_id
            total = D(f.get("totalSz") or 0)
            done = OrderStatus.FILLED if total >= st.qty else (OrderStatus.CANCELED if ioc else OrderStatus.OPEN)
            st.apply(status=done, filled=total, avg_price=D(f["avgPx"]) if f.get("avgPx") else None, exchange_id=str(oid))
        elif "error" in s:
            msg = str(s["error"])
            if ioc and NO_MATCH_HINT in msg.lower():
                st.apply(status=OrderStatus.CANCELED, reason=msg)
            else:
                st.apply(status=OrderStatus.REJECTED, reason=msg)
        else:
            st.apply(status=OrderStatus.UNKNOWN, reason=str(s)[:200])

    async def _place(self, symbol: str, is_buy: bool, qty: Decimal, price: Decimal, reduce_only: bool, tif: str) -> OrderState:
        from hyperliquid.utils.types import Cloid

        cloid = Cloid.from_int(new_client_index())
        st = OrderState(cloid.to_raw(), self.name, symbol, is_buy, qty, price, post_only=(tif == "Alo"), reduce_only=reduce_only)
        self._track(st)
        await self.limiter.acquire()
        self._note_action(new_order=True)
        try:
            res = await asyncio.to_thread(self.exchange.order, symbol, is_buy, float(qty), float(price),
                                          {"limit": {"tif": tif}}, reduce_only, cloid)
        except Exception as exc:  # noqa: BLE001 - outcome unknown
            self.note_failure("order", f"{type(exc).__name__}: {exc}")
            st.apply(status=OrderStatus.UNKNOWN, reason=f"submit exception: {exc}")
            log.error("entropy submit exception (state unknown) %s: %s", st.client_id, exc)
            return st
        self._parse_status(st, res, ioc=(tif == "Ioc"))
        if st.status == OrderStatus.REJECTED:
            if self._note_budget_rejection(st.reason):
                log.info("entropy order rejected %s %s (action budget): %s", symbol, st.client_id, st.reason[:120])
            else:
                self.note_failure("order", st.reason)
                log.warning("entropy order rejected %s %s: %s", symbol, st.client_id, st.reason)
        return st

    async def place_post_only(self, symbol, is_buy, qty, price, reduce_only) -> OrderState:
        return await self._place(symbol, is_buy, qty, price, reduce_only, "Alo")

    async def place_ioc(self, symbol, is_buy, qty, limit_price, reduce_only) -> OrderState:
        st = await self._place(symbol, is_buy, qty, limit_price, reduce_only, "Ioc")
        if st.status == OrderStatus.UNKNOWN:
            await self.wait_final(st, timeout=max(8.0, self.cfg.unknown_lookup_s), poll=1.5)
        return st

    async def refresh_order(self, st: OrderState) -> OrderState:
        ref: Any = int(st.exchange_id) if st.exchange_id else st.client_id
        res = await self._info({"type": "orderStatus", "user": self.address, "oid": ref})
        if isinstance(res, dict) and res.get("status") == "order":
            wrap = res.get("order") or {}
            o = wrap.get("order") or {}
            if o.get("oid") is not None:
                self._by_oid[int(o["oid"])] = st.client_id
            filled = D(o.get("origSz") or 0) - D(o.get("sz") or 0)
            st.apply(status=map_hl_status(str(wrap.get("status", ""))), filled=filled if filled > 0 else None,
                     exchange_id=str(o.get("oid")) if o.get("oid") is not None else None)
            self._recompute_avg(st)
            if st.avg_price is None and st.filled > 0 and o.get("limitPx"):
                st.avg_price = D(o["limitPx"])  # conservative fallback (our limit)
        elif isinstance(res, dict) and res.get("status") == "unknownOid":
            if st.status in (OrderStatus.UNKNOWN, OrderStatus.PENDING) and time.time() - st.created > 30:
                st.apply(status=OrderStatus.REJECTED, reason="unknown oid")
        self._notify(st.client_id)
        return st

    async def _refresh_open_orders(self) -> None:
        for st in list(self.orders.values()):
            if not st.status.final:
                try:
                    await self.refresh_order(st)
                except Exception as exc:  # noqa: BLE001
                    log.debug("entropy refresh after reconnect failed: %s", exc)

    async def cancel(self, st: OrderState) -> None:
        if st.status.final:
            return
        if not st.exchange_id:
            await self.refresh_order(st)
            if st.status.final:
                return
        await self.limiter.acquire()
        self._note_action(new_order=False)
        if st.exchange_id:
            res = await asyncio.to_thread(self.exchange.cancel, st.symbol, int(st.exchange_id))
        else:
            from hyperliquid.utils.types import Cloid
            res = await asyncio.to_thread(self.exchange.cancel_by_cloid, st.symbol, Cloid.from_str(st.client_id))
        if not isinstance(res, dict) or res.get("status") != "ok":
            log.warning("entropy cancel %s -> %s", st.client_id, res)
        else:
            sts = (((res.get("response") or {}).get("data") or {}).get("statuses")) or []
            if sts and isinstance(sts[0], dict) and "error" in sts[0]:
                log.info("entropy cancel %s: %s", st.client_id, sts[0]["error"])  # likely already filled/canceled

    async def cancel_all(self, symbols: list[str]) -> None:
        if self.exchange is None:
            return
        orders = await self._info({"type": "openOrders", "user": self.address, "dex": self.dex})
        for o in orders or []:
            if o.get("coin") in symbols:
                await self.limiter.acquire()
                self._note_action(new_order=False)
                res = await asyncio.to_thread(self.exchange.cancel, o["coin"], int(o["oid"]))
                log.info("entropy cancel_all %s/%s -> %s", o["coin"], o["oid"], res)
