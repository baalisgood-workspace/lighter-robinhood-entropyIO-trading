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
import time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Optional

import aiohttp

from arb.models import Balance, Level, MarketInfo, OrderBook, OrderState, OrderStatus, Position
from arb.util import D, RateLimiter, new_client_index
from arb.venues.base import Venue, VenueError
from arb.venues.transport import harden_requests_session, http_session, ws_connect

log = logging.getLogger("arb.entropy")

HL_MIN_NOTIONAL = Decimal("10")
BOOK_DEPTH = 20
NO_MATCH_HINT = "could not immediately match"


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
        async with self._session.post(self.cfg.rest_url.rstrip("/") + "/info", json=payload, allow_redirects=False) as r:
            text = await r.text()
            if r.status != 200:
                raise VenueError(f"entropy /info {payload.get('type')} -> HTTP {r.status}: {text[:200]}")
            return json.loads(text)

    async def _load_markets(self, symbols: list[str]) -> None:
        meta = await self._info({"type": "meta", "dex": self.dex})
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
                out[sym] = Position(self.name, sym, D(p.get("szi") or 0), D(p["entryPx"]) if p.get("entryPx") else None)
        return out

    async def balance(self) -> Balance:
        st = await self._info({"type": "clearinghouseState", "user": self.address, "dex": self.dex}, cost=2)
        ms = st.get("marginSummary") or {}
        return Balance(self.name, equity=D(ms.get("accountValue") or 0), available=D(st.get("withdrawable") or 0))

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
        if cloid and cloid in self.orders:
            return cloid
        oid = order.get("oid")
        return self._by_oid.get(int(oid)) if oid is not None else None

    def _apply_order_update(self, upd: dict) -> None:
        o = upd.get("order") or {}
        cid = self._client_for(o)
        if cid is None or cid not in self.orders:
            return
        st = self.orders[cid]
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
        cid = self._by_oid.get(oid) or (f.get("cloid") if f.get("cloid") in self.orders else None)
        if cid and cid in self.orders:
            st = self.orders[cid]
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
        try:
            res = await asyncio.to_thread(self.exchange.order, symbol, is_buy, float(qty), float(price),
                                          {"limit": {"tif": tif}}, reduce_only, cloid)
        except Exception as exc:  # noqa: BLE001 - outcome unknown
            st.apply(status=OrderStatus.UNKNOWN, reason=f"submit exception: {exc}")
            log.error("entropy submit exception (state unknown) %s: %s", st.client_id, exc)
            return st
        self._parse_status(st, res, ioc=(tif == "Ioc"))
        if st.status == OrderStatus.REJECTED:
            log.warning("entropy order rejected %s %s: %s", symbol, st.client_id, st.reason)
        return st

    async def place_post_only(self, symbol, is_buy, qty, price, reduce_only) -> OrderState:
        return await self._place(symbol, is_buy, qty, price, reduce_only, "Alo")

    async def place_ioc(self, symbol, is_buy, qty, limit_price, reduce_only) -> OrderState:
        st = await self._place(symbol, is_buy, qty, limit_price, reduce_only, "Ioc")
        if st.status == OrderStatus.UNKNOWN:
            await self.wait_final(st, timeout=8.0, poll=1.5)
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
                res = await asyncio.to_thread(self.exchange.cancel, o["coin"], int(o["oid"]))
                log.info("entropy cancel_all %s/%s -> %s", o["coin"], o["oid"], res)
