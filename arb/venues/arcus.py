"""Arcus perpetuals adapter (https://arcus.xyz).

Credentials (environment / .env):
    ARCUS_ACCOUNT_ADDRESS    0x... wallet address of the account
    ARCUS_ACCOUNT_INDEX      sub-account 0..9 (default 0)
    ARCUS_API_KEY            Ed25519 API public key (hex, 64 chars), registered on Arcus
    ARCUS_API_SIGNING_KEY    matching Ed25519 32-byte seed (hex, 64 chars)

Protocol (same as the proven adapter in lighter-arcus-stock-trading):
  * REST  GET /v1/markets, /v1/l2OrderBook/{m}, /v1/order/{id}, /v1/openOrders, /v1/orders,
          /v1/positions, /v1/account  (query address + accountIndex)
  * REST  POST /v1/placeOrder, /v1/cancelOrder with headers X-API-Key, X-Timestamp (ns nonce),
          X-Signature = Ed25519(sorted compact JSON of the short-key payload)
  * WS    wss://api.arcus.xyz/v1/ws: l2Orderbook (full snapshot per message, lastSequenceId),
          markets (status / funding / mark every few seconds), orders + userFills (by address).
Writes are never replayed automatically: a transport error leaves the order UNKNOWN until
REST/WS resolves it.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from decimal import Decimal
from typing import Any, Optional

import aiohttp

from arb.models import Balance, Level, MarketInfo, OrderBook, OrderState, OrderStatus, Position
from arb.util import D, RateLimiter, new_client_index, round_to_step
from arb.venues.base import Venue, VenueError
from arb.venues.transport import http_session, ws_connect

log = logging.getLogger("arb.arcus")

BOOK_DEPTH = 20
TIF = {"GTT": 0, "FOK": 1, "IOC": 2, "ALO": 3}
_STATUS = {
    "OPEN": OrderStatus.OPEN, "PLACED": OrderStatus.OPEN, "PARTIALLY_FILLED": OrderStatus.OPEN, "ACK": OrderStatus.PENDING,
    "FILLED": OrderStatus.FILLED, "CANCELED": OrderStatus.CANCELED, "MARGIN_CANCELED": OrderStatus.CANCELED,
    "EXPIRED": OrderStatus.CANCELED, "REJECTED": OrderStatus.REJECTED,
}


def map_arcus_status(row: dict) -> OrderStatus:
    raw = str(row.get("status") or row.get("state") or "").upper()
    st = _STATUS.get(raw, OrderStatus.PENDING if raw else OrderStatus.UNKNOWN)
    # an IOC that is "partially filled" is done: the rest was canceled
    if raw == "PARTIALLY_FILLED" and str(row.get("timeInForce", "")).upper() in ("IOC", "FOK"):
        st = OrderStatus.CANCELED
    if row.get("cancelReason") == "MODIFY_CANCELED":
        st = OrderStatus.PENDING
    return st


def tick_for(tiers: list[dict], base_tick: Decimal, price: Decimal) -> Decimal:
    for band in tiers or []:
        if "upToPrice" not in band or price < D(band["upToPrice"]):
            return max(D(band["tick"]), base_tick)
    return base_tick


class ArcusRejected(VenueError):
    pass


class ArcusUnknown(VenueError):
    pass


class ArcusVenue(Venue):
    def __init__(self, cfg, env=None):
        super().__init__(cfg, env)
        p = cfg.env_prefix
        self.address = (self.env.get(f"{p}_ACCOUNT_ADDRESS") or "").strip().lower()
        self.account_index = int((self.env.get(f"{p}_ACCOUNT_INDEX") or "0").strip() or 0)
        self.api_key = (self.env.get(f"{p}_API_KEY") or "").strip().lower()
        self._seed = (self.env.get(f"{p}_API_SIGNING_KEY") or "").strip()
        self.limiter = RateLimiter(cfg.requests_per_minute)
        self._session: Optional[aiohttp.ClientSession] = None
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._raw_markets: dict[str, dict] = {}
        self._symbol_by_mid: dict[str, str] = {}
        self._seq: dict[str, int] = {}
        self._by_oid: dict[str, str] = {}  # exchange order id -> client id
        self._raw_oid: dict[str, Any] = {}  # client id -> order id exactly as returned (signature input)
        self._fills: dict[str, dict] = {}  # client id -> {trade_id: (qty, px)}
        self._has_state: set[str] = set()  # client ids with at least one real (sized) order row from WS/REST
        self._cancel_sent: set[str] = set()
        self._refreshing: set[str] = set()
        self._signer = None
        self._last_nonce = 0
        self._write_lock = asyncio.Lock()
        self.private_connected = False
        self._watch: list[str] = []

    # ------------------------------------------------------------ identity / lifecycle
    def has_credentials(self) -> bool:
        return bool(self.address and self.api_key and self._seed)

    @property
    def account_id(self) -> str:
        return f"{self.address}:{self.account_index}"

    def _check_credentials(self) -> None:
        if not re.fullmatch(r"0x[0-9a-f]{40}", self.address):
            raise VenueError("arcus: ARCUS_ACCOUNT_ADDRESS must be a 0x-prefixed 40-hex address")
        if not 0 <= self.account_index <= 9:
            raise VenueError("arcus: ARCUS_ACCOUNT_INDEX must be 0..9")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", self._seed):
            raise VenueError("arcus: ARCUS_API_SIGNING_KEY must be a 32-byte Ed25519 seed in hex")
        from nacl.signing import SigningKey  # lazy

        key = SigningKey(bytes.fromhex(self._seed))
        if key.verify_key.encode().hex() != self.api_key:
            raise VenueError("arcus: ARCUS_API_KEY does not match ARCUS_API_SIGNING_KEY")
        self._signer = key

    async def start(self, symbols: list[str], trading: bool) -> None:
        self._running = True
        self._session = http_session(15)
        await self._load_markets(symbols)
        self._watch = [s for s in symbols if s in self.markets]
        if trading:
            if not self.has_credentials():
                raise VenueError("arcus: missing ARCUS_ACCOUNT_ADDRESS / ARCUS_API_KEY / ARCUS_API_SIGNING_KEY")
            self._check_credentials()
        self.trading = trading
        self._tasks.append(asyncio.create_task(self._ws_loop(private=trading), name="arcus-ws"))

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
            self._session = None

    # ------------------------------------------------------------ REST
    async def _request(self, method: str, path: str, *, query=None, body=None, nonce=None, signature=None):
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        if signature:
            headers.update({"X-Timestamp": str(nonce), "X-Signature": signature})
        await self.limiter.acquire()
        try:
            async with self._session.request(method, self.cfg.rest_url + path, params=query, json=body, headers=headers,
                                             allow_redirects=False) as r:
                text = await r.text()
                if 300 <= r.status < 400:  # never follow: a redirected write has an unknown outcome
                    if method != "GET":
                        raise ArcusUnknown(f"arcus HTTP {r.status} redirect refused")
                    raise VenueError(f"arcus HTTP {r.status} redirect refused")
                if r.status >= 400:
                    self.note_failure(f"{method} {path}", f"HTTP {r.status}: {text[:150]}")
                    if method != "GET" and r.status not in (400, 401, 403, 429):
                        raise ArcusUnknown(f"arcus HTTP {r.status}: {text[:200]}")
                    raise ArcusRejected(f"arcus HTTP {r.status}: {text[:200]}")
                return json.loads(text) if text else {}
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
            if isinstance(exc, VenueError):
                raise
            self.note_failure(f"{method} {path}", f"transport {type(exc).__name__}: {exc}")
            if method != "GET":
                raise ArcusUnknown(f"arcus transport error: {exc}") from exc
            raise VenueError(f"arcus read failed: {exc}") from exc

    def _q(self) -> dict:
        return {"address": self.address, "accountIndex": self.account_index}

    def _nonce(self) -> int:
        self._last_nonce = max(time.time_ns(), self._last_nonce + 1)
        return self._last_nonce

    def _sign(self, payload: dict) -> str:
        if self._signer is None:
            self._check_credentials()
        msg = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return self._signer.sign(msg).signature.hex()

    def _set_market(self, row: dict) -> None:
        name = row.get("marketDisplayName")
        if not name:
            return
        self._raw_markets[name] = row
        self._symbol_by_mid[str(row.get("marketId"))] = name
        if name in self.markets:
            m = self.markets[name]
            m.extra["status"] = row.get("status")
            m.extra["mark"] = row.get("markPrice")
            m.extra["bounds"] = (row.get("lowerTradingBound"), row.get("upperTradingBound"))
        if name in self.markets and row.get("fundingRate") is not None:
            try:
                self.funding_hourly[name] = float(row["fundingRate"])  # decimal per hour (hourly funding)
            except (TypeError, ValueError):
                pass

    async def _load_markets(self, symbols: list[str]) -> None:
        data = await self._request("GET", "/v1/markets")
        rows = data.get("markets", data)
        rows = list(rows.values()) if isinstance(rows, dict) else rows
        by_name = {r["marketDisplayName"]: r for r in rows if r.get("marketDisplayName")}
        for sym in symbols:
            r = by_name.get(sym)
            if r is None:
                log.warning("arcus: market %s not listed, skipped", sym)
                continue
            if r.get("status") != "ONLINE":
                log.warning("arcus: market %s is %s, skipped", sym, r.get("status"))
                continue
            step = D(r["stepSize"])
            self.markets[sym] = MarketInfo(
                venue=self.name, symbol=sym, size_step=step, min_qty=max(D(r["minOrderSize"]), step),
                min_notional=D(r.get("minOrderNotional") or "0"), price_tick=D(r["tickSize"]),
                extra={"market_id": int(r["marketId"]), "tick_tiers": r.get("tickTiers") or [],
                       "max_qty": D(r.get("maxOrderSize") or "1e12"), "status": r.get("status"),
                       "mark": r.get("markPrice"), "bounds": (r.get("lowerTradingBound"), r.get("upperTradingBound"))})
        for r in rows:
            self._set_market(r)

    # ------------------------------------------------------------ prices
    def price_tick_at(self, symbol: str, price: Decimal) -> Decimal:
        m = self.market(symbol)
        return tick_for(m.extra.get("tick_tiers"), m.price_tick, price)

    def round_price(self, symbol: str, price: Decimal, up: bool) -> Decimal:
        return round_to_step(price, self.price_tick_at(symbol, price), up)

    # ------------------------------------------------------------ WebSocket
    async def _ws_loop(self, private: bool) -> None:
        backoff = 1.0
        while self._running:
            try:
                async with ws_connect(self.cfg.ws_url, self.mark_alive, self.cfg.liveness_s, self.cfg.quick_resume_s) as ws:
                    self._seq.clear()
                    for sym in self._watch:
                        await ws.send(json.dumps({"type": "subscribe", "channel": "l2Orderbook", "id": sym, "nLevels": BOOK_DEPTH}))
                    await ws.send(json.dumps({"type": "subscribe", "channel": "markets"}))
                    if private:
                        for ch in ("orders", "userFills"):
                            await ws.send(json.dumps({"type": "subscribe", "channel": ch, "id": self.address,
                                                      "accountIndex": self.account_index}))
                        asyncio.create_task(self._refresh_open_orders())
                    self.on_connected()
                    backoff = 1.0
                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            raise VenueError("arcus WS silent for 30s")
                        self._on_ws(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("arcus WS error: %s; reconnect in %.0fs", exc, backoff)
                self.alarm("connection_timeout" if "ping" in str(exc).lower() or "silent" in str(exc).lower() else "reconnect",
                           detail=str(exc))
            self.connected = False
            self.private_connected = False
            self.reset_books()
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def _on_ws(self, m: dict) -> None:
        typ, ch, data = m.get("type"), m.get("channel"), m.get("contents")
        if typ == "degraded":
            if ch == "l2Orderbook" and m.get("id") in self.books:
                self.reset_books([m["id"]])  # wait for a fresh snapshot
                self.alarm("degraded", m["id"])
            if ch in ("orders", "userFills"):
                self.private_connected = False
            return
        if typ not in ("subscribed", "channel_data") or not isinstance(data, dict):
            return
        if ch == "l2Orderbook":
            self._on_book(m.get("id"), data)
        elif ch == "markets":
            rows = data.get("markets", {})
            for r in rows.values() if isinstance(rows, dict) else rows:
                self._set_market(r)
        elif ch in ("orders", "userFills"):
            if str(m.get("id", "")).lower() != self.address or m.get("accountIndex", 0) != self.account_index:
                return
            if typ == "subscribed":
                self.private_connected = True
            if ch == "orders":
                rows: list = []
                for f in ("orders", "openOrders", "recentClosedOrders"):
                    v = data.get(f, [])
                    rows.extend(v.values() if isinstance(v, dict) else v)
                if "orderId" in data:
                    rows.append(data)
                for r in rows:
                    self._apply_order_row(r)
            else:
                rows = data.get("fills", [data] if "orderId" in data else [])
                for r in rows.values() if isinstance(rows, dict) else rows:
                    self._apply_fill_row(r)

    def _on_book(self, sym: Optional[str], data: dict) -> None:
        if sym not in self.markets:
            return
        seq = int(data.get("lastSequenceId") or 0)
        if seq and seq < self._seq.get(sym, -1):
            return
        self._seq[sym] = seq
        bids = [Level(D(p), D(q)) for p, q in data.get("bids", []) if D(q) > 0]
        asks = [Level(D(p), D(q)) for p, q in data.get("asks", []) if D(q) > 0]
        bids.sort(key=lambda lv: lv.price, reverse=True)
        asks.sort(key=lambda lv: lv.price)
        ts = float(data.get("timestamp") or 0) / 1e6
        self.books[sym] = OrderBook(self.name, sym, bids=bids, asks=asks, ts_exchange=ts, ts_local=time.time())
        self.market_event.set()

    # ------------------------------------------------------------ order state
    def _state_for(self, row: dict) -> Optional[OrderState]:
        cid = row.get("clientId")
        if cid is not None and str(cid) in self.orders:
            return self.orders[str(cid)]
        oid = row.get("orderId")
        if oid is not None and str(oid) in self._by_oid:
            return self.orders.get(self._by_oid[str(oid)])
        return None

    def _foreign(self, st: OrderState, r: dict) -> bool:
        """A row that matched one of our orders only by clientId but contradicts it (exchange order id or
        market) belongs to another order reusing the clientId (older / reset session on the same account)."""
        oid = r.get("orderId")
        known = self._raw_oid.get(st.client_id) or st.exchange_id
        if oid is not None and known is not None and str(oid) != str(known):
            return True
        mid = r.get("marketId")
        try:
            ours = int(self.markets[st.symbol].extra["market_id"]) if st.symbol in self.markets else None
            if mid not in (None, "", 0, "0") and ours is not None and int(mid) != ours:
                return True
        except (TypeError, ValueError, KeyError):
            pass
        return False

    def forget(self, st: OrderState) -> None:
        super().forget(st)
        if st.status.final:
            self._has_state.discard(st.client_id)
            self._cancel_sent.discard(st.client_id)

    def _schedule_refresh(self, st: OrderState) -> None:
        if st.client_id in self._refreshing or st.status.final:
            return
        self._refreshing.add(st.client_id)

        async def run() -> None:
            try:
                await asyncio.sleep(0.3)
                await self.refresh_order(st)
            except Exception as exc:  # noqa: BLE001
                log.debug("arcus refresh after rejection push %s: %s", st.client_id, exc)
            finally:
                self._refreshing.discard(st.client_id)
                self._notify(st.client_id)
        try:
            asyncio.get_running_loop().create_task(run())
        except RuntimeError:
            self._refreshing.discard(st.client_id)

    def _apply_order_row(self, r: dict) -> None:
        st = self._state_for(r)
        if st is None:
            return
        if self._foreign(st, r):
            log.warning("arcus: row for %s contradicts the order (orderId %s / market %s): foreign order reusing the clientId, ignored",
                        st.client_id, r.get("orderId"), r.get("marketId"))
            self.alarm("client_id_reused_by_foreign_order", st.symbol, f"{st.client_id} orderId={r.get('orderId')}")
            return
        raw_status = str(r.get("status") or r.get("state") or "").upper()
        size = r.get("originalSize")
        try:
            zero_size = size is not None and D(str(size)) <= 0
        except Exception:  # noqa: BLE001
            zero_size = True
        if zero_size and raw_status == "REJECTED":
            # Arcus pushes a zero-value REJECTED row (size 0, market 0, side BUY) when a REQUEST is rejected.
            # Only an order we have nothing but the placeOrder ACK for (no real row, nothing filled, no cancel
            # sent) is itself rejected; otherwise it was our cancel that got rejected (often: the order had just
            # filled) and the row must never be merged - re-read the real state from REST instead.
            if st.client_id not in self._has_state and st.filled == 0 and st.client_id not in self._cancel_sent:
                st.apply(status=OrderStatus.REJECTED, reason=str(r.get("rejectReason") or "rejected (zero-value push)"))
                self._notify(st.client_id)
            else:
                log.info("arcus: request rejection push for %s ignored, re-reading order state", st.client_id)
                self._schedule_refresh(st)
            return
        oid = r.get("orderId")
        if oid is not None:
            self._by_oid[str(oid)] = st.client_id
            self._raw_oid.setdefault(st.client_id, oid)
        filled = None
        if zero_size:
            pass  # status-only row: a zero size is no quantity evidence (size/fill come from our order)
        elif "originalSize" in r and "remainingSize" in r:
            filled = D(str(r["originalSize"])) - D(str(r["remainingSize"]))
            self._has_state.add(st.client_id)
        elif r.get("filledSize") is not None:
            filled = D(str(r["filledSize"]))
            self._has_state.add(st.client_id)
        avg = r.get("avgFillPrice") or r.get("averageFillPrice")
        avg = D(str(avg)) if avg not in (None, "", "0") else self._fill_avg(st.client_id)
        status = map_arcus_status(r)
        if status == OrderStatus.FILLED:
            filled = st.qty if filled is None else max(filled, st.qty)
        if filled is not None and filled > 0 and avg is None and st.post_only:
            avg = st.price  # a resting post-only order can only fill at its own price
        st.apply(status=status if status != OrderStatus.UNKNOWN else None, filled=filled, avg_price=avg,
                 exchange_id=str(oid) if oid is not None else None,
                 reason=str(r.get("cancelReason") or r.get("rejectReason") or "") if status in (OrderStatus.CANCELED, OrderStatus.REJECTED) else "")
        self._notify(st.client_id)

    def _fill_avg(self, cid: str) -> Optional[Decimal]:
        f = self._fills.get(cid)
        if not f:
            return None
        q = sum(x for x, _ in f.values())
        return sum(x * p for x, p in f.values()) / q if q > 0 else None

    def _apply_fill_row(self, r: dict) -> None:
        st = self._state_for(r)
        if st is None or not r.get("tradeId"):
            return
        oid = r.get("orderId")
        known = self._raw_oid.get(st.client_id) or st.exchange_id
        if oid is not None and known is not None and str(oid) != str(known):
            return  # fill of a foreign order reusing our clientId
        self._has_state.add(st.client_id)
        f = self._fills.setdefault(st.client_id, {})
        f[str(r["tradeId"])] = (D(str(r.get("size", "0"))), D(str(r.get("price", "0"))))
        total = sum(x for x, _ in f.values())
        if "originalSize" in r and "remainingSize" in r:
            total = max(total, D(str(r["originalSize"])) - D(str(r["remainingSize"])))
        st.apply(filled=total, avg_price=self._fill_avg(st.client_id),
                 exchange_id=str(r["orderId"]) if r.get("orderId") is not None else None)
        self._notify(st.client_id)

    # ------------------------------------------------------------ orders
    def _check_market(self, symbol: str) -> dict:
        m = self.market(symbol)
        if m.extra.get("status") not in (None, "ONLINE"):
            raise VenueError(f"arcus: {symbol} is {m.extra.get('status')}")
        return m.extra

    async def _submit(self, st: OrderState, order_type: str, tif: str) -> OrderState:
        m = self.market(st.symbol)
        ex = self._check_market(st.symbol)
        tick = self.price_tick_at(st.symbol, st.price)
        if st.price % tick or st.price % m.price_tick or st.qty % m.size_step or st.qty <= 0:
            st.apply(status=OrderStatus.REJECTED, reason=f"price/qty off grid ({st.price} / {st.qty})")
            return st
        if not (m.min_qty <= st.qty <= ex["max_qty"]):
            st.apply(status=OrderStatus.REJECTED, reason="size outside market limits")
            return st
        if not st.reduce_only and st.qty * st.price < m.min_notional:
            st.apply(status=OrderStatus.REJECTED, reason="notional below minimum")
            return st
        self._track(st)
        async with self._write_lock:
            # readiness re-check after waiting for the write lock, right before signing
            if not self.connected or not self.trading:
                st.apply(status=OrderStatus.REJECTED, reason="arcus not ready (disconnected) at sign time")
                return st
            self._check_market(st.symbol)
            nonce = self._nonce()
            expiry = nonce // 1000 + 32 * 24 * 3600 * 1_000_000  # microseconds
            payload = {"ad": self.address, "ai": self.account_index, "c": st.client_id, "ct": nonce, "g": expiry * 1000,
                       "m": int(ex["market_id"]), "op": 1, "p": int(st.price / m.price_tick), "q": int(st.qty / m.size_step),
                       "r": int(st.reduce_only), "s": int(not st.is_buy), "t": TIF[tif], "v": 1}
            body = {"address": self.address, "accountIndex": self.account_index, "marketId": payload["m"],
                    "orderSide": "BUY" if st.is_buy else "SELL", "orderType": order_type,
                    "quantity": format(st.qty, "f"), "price": format(st.price, "f"), "timeInForce": tif,
                    "goodTilTime": str(expiry), "clientId": st.client_id, "timestamp": nonce, "reduceOnly": st.reduce_only}
            try:
                ack = await self._request("POST", "/v1/placeOrder", query={"address": self.address}, body=body,
                                          nonce=nonce, signature=self._sign(payload))
            except ArcusRejected as exc:
                st.apply(status=OrderStatus.REJECTED, reason=str(exc))
                return st
            except Exception as exc:  # noqa: BLE001 - outcome unknown, resolved via refresh
                st.apply(status=OrderStatus.UNKNOWN, reason=f"submit: {exc}")
                log.error("arcus submit %s state unknown: %s", st.client_id, exc)
                return st
        oid = ack.get("orderId") if isinstance(ack, dict) else None
        if not oid:
            st.apply(status=OrderStatus.UNKNOWN, reason=f"no orderId in ack: {str(ack)[:200]}")
            return st
        self._raw_oid[st.client_id] = oid
        self._by_oid[str(oid)] = st.client_id
        st.apply(exchange_id=str(oid))
        if st.status == OrderStatus.PENDING:
            st.status = OrderStatus.OPEN if tif in ("ALO", "GTT") else OrderStatus.PENDING
        return st

    async def place_post_only(self, symbol, is_buy, qty, price, reduce_only) -> OrderState:
        st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, price, post_only=True, reduce_only=reduce_only)
        return await self._submit(st, "LIMIT", "ALO")

    async def place_ioc(self, symbol, is_buy, qty, limit_price, reduce_only) -> OrderState:
        mark = D(str(self.market(symbol).extra.get("mark") or 0))
        if mark > 0 and abs(limit_price / mark - 1) > D("0.1"):
            st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, limit_price, False, reduce_only)
            st.apply(status=OrderStatus.REJECTED, reason="protective price more than 10% from mark")
            return st
        st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, limit_price, post_only=False, reduce_only=reduce_only)
        await self._submit(st, "MARKET", "IOC")
        if st.status == OrderStatus.REJECTED:
            return st
        timeout = max(8.0, self.cfg.unknown_lookup_s) if st.status == OrderStatus.UNKNOWN else 8.0
        return await self.wait_final(st, timeout=timeout, poll=1.0)

    async def refresh_order(self, st: OrderState) -> OrderState:
        row = None
        oid = self._raw_oid.get(st.client_id) or st.exchange_id
        try:
            if oid is not None:
                row = await self._request("GET", f"/v1/order/{oid}", query=self._q())
            else:
                for path in ("/v1/openOrders", "/v1/orders"):
                    data = await self._request("GET", path, query=self._q())
                    rows = data.get("orders", [])
                    rows = rows.values() if isinstance(rows, dict) else rows
                    row = next((r for r in rows if str(r.get("clientId")) == st.client_id), None)
                    if row:
                        break
        except ArcusRejected as exc:
            log.debug("arcus refresh %s: %s", st.client_id, exc)
        if row:
            self._apply_order_row(row)
        elif st.status == OrderStatus.UNKNOWN and oid is None and time.time() - st.created > 30:
            st.apply(status=OrderStatus.REJECTED, reason="not found on exchange")
        return st

    async def _refresh_open_orders(self) -> None:
        for st in list(self.orders.values()):
            if not st.status.final:
                try:
                    await self.refresh_order(st)
                except Exception as exc:  # noqa: BLE001
                    log.debug("arcus refresh after reconnect failed: %s", exc)

    async def _cancel_oid(self, symbol: str, oid: Any) -> None:
        ex = self._check_market(symbol) if symbol in self.markets else None
        mid = int(ex["market_id"]) if ex else None
        if mid is None:
            return
        async with self._write_lock:
            nonce = self._nonce()
            payload = {"ad": self.address, "ai": self.account_index, "ct": nonce, "m": mid, "op": 2, "v": 1, "id": oid}
            body = {**self._q(), "marketId": mid, "timestamp": nonce, "orderId": oid, "kind": "orderId"}
            await self._request("POST", "/v1/cancelOrder", query={"address": self.address}, body=body, nonce=nonce,
                                signature=self._sign(payload))

    async def cancel(self, st: OrderState) -> None:
        if st.status.final:
            return
        if not (self._raw_oid.get(st.client_id) or st.exchange_id):
            await self.refresh_order(st)
        oid = self._raw_oid.get(st.client_id) or st.exchange_id
        if st.status.final:
            return
        if oid is None:
            raise VenueError(f"arcus: cannot cancel {st.client_id}, order id unknown")
        self._cancel_sent.add(st.client_id)
        try:
            await self._cancel_oid(st.symbol, oid)
        except ArcusRejected as exc:
            log.warning("arcus cancel %s rejected: %s", st.client_id, exc)  # often: already filled/canceled
        except ArcusUnknown as exc:
            log.warning("arcus cancel %s outcome unknown: %s", st.client_id, exc)

    async def cancel_all(self, symbols: list[str]) -> None:
        if not self.trading:
            return
        data = await self._request("GET", "/v1/openOrders", query=self._q())
        rows = data.get("orders", [])
        for r in rows.values() if isinstance(rows, dict) else rows:
            sym = r.get("marketDisplayName") or self._symbol_by_mid.get(str(r.get("marketId")))
            if sym in symbols:
                try:
                    await self._cancel_oid(sym, r["orderId"])
                    log.info("arcus cancel_all %s/%s", sym, r["orderId"])
                except VenueError as exc:
                    log.warning("arcus cancel_all %s/%s: %s", sym, r.get("orderId"), exc)

    # ------------------------------------------------------------ account
    async def positions(self) -> dict[str, Position]:
        data = await self._request("GET", "/v1/positions", query=self._q())
        rows = data.get("positions", [])
        out: dict[str, Position] = {}
        for r in rows.values() if isinstance(rows, dict) else rows:
            sym = r.get("marketDisplayName") or self._symbol_by_mid.get(str(r.get("marketId")))
            if not sym:
                continue
            size = abs(D(str(r.get("size", "0"))))
            if size == 0:
                continue
            qty = -size if str(r.get("side", "")).upper() == "SHORT" else size
            out[sym] = Position(self.name, sym, qty, D(str(r["averageEntryPrice"])) if r.get("averageEntryPrice") else None)
        return out

    async def balance(self) -> Balance:
        try:
            data = await self._request("GET", "/v1/account", query=self._q())
        except ArcusRejected as exc:
            if "404" in str(exc):
                return Balance(self.name, equity=D(0), available=D(0))
            raise
        return Balance(self.name, equity=D(str(data.get("equity", "0"))), available=D(str(data.get("freeCollateral", "0"))))
