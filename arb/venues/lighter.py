"""Lighter mainnet and Lighter on Robinhood Chain adapter.

Both venues share the same signer, REST and WebSocket protocol but are separate
exchanges: accounts, API keys, nonces and market ids are not shared.

Credentials (environment / .env):
    <PREFIX>_ACCOUNT_INDEX, <PREFIX>_API_KEY_INDEX, <PREFIX>_API_KEY_PRIVATE_KEY
with PREFIX = LIGHTER (mainnet) or LIGHTER_RH (Robinhood Chain).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import Any, Optional

import aiohttp
import websockets

from arb.models import Balance, Level, MarketInfo, OrderBook, OrderState, OrderStatus, Position
from arb.util import D, RateLimiter, new_client_index
from arb.venues.base import Venue, VenueError

log = logging.getLogger("arb.lighter")

BOOK_DEPTH = 20
RH_RESERVED_KEY_INDEXES = {0, 1, 2, 3, 157}
TOKEN_REFRESH_S = 8 * 60

_STATUS = {
    "in-progress": OrderStatus.PENDING,
    "pending": OrderStatus.PENDING,
    "open": OrderStatus.OPEN,
    "filled": OrderStatus.FILLED,
}


def map_status(raw: str) -> OrderStatus:
    raw = (raw or "").lower()
    if raw in _STATUS:
        return _STATUS[raw]
    if raw.startswith("canceled") or raw.startswith("cancelled"):
        return OrderStatus.CANCELED
    return OrderStatus.UNKNOWN


class _RawBook:
    __slots__ = ("bids", "asks", "nonce", "ts")

    def __init__(self):
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.nonce: Optional[int] = None
        self.ts = 0.0

    def apply(self, side: dict, levels: list[dict]) -> None:
        for lv in levels:
            px, sz = D(lv["price"]), D(lv["size"])
            if sz <= 0:
                side.pop(px, None)
            else:
                side[px] = sz


class LighterVenue(Venue):
    def __init__(self, cfg, env=None):
        super().__init__(cfg, env)
        p = cfg.env_prefix
        self.account_index = (self.env.get(f"{p}_ACCOUNT_INDEX") or "").strip()
        self.api_key_index = (self.env.get(f"{p}_API_KEY_INDEX") or "").strip()
        self._api_private_key = (self.env.get(f"{p}_API_KEY_PRIVATE_KEY") or "").strip()
        self.limiter = RateLimiter(cfg.requests_per_minute)
        self.signer = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self.market_ids: dict[str, int] = {}
        self._symbol_by_mid: dict[int, str] = {}
        self._raw: dict[int, _RawBook] = {}
        self._public_ws = None
        self._account_cache: tuple[float, dict] | None = None
        self._account_lock = asyncio.Lock()
        self.private_connected = False

    # ------------------------------------------------------------ lifecycle
    @property
    def account_id(self) -> str:
        return self.account_index or "none"

    def has_credentials(self) -> bool:
        return bool(self.account_index and self.api_key_index and self._api_private_key)

    async def start(self, symbols: list[str], trading: bool) -> None:
        self._running = True
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        await self._load_markets(symbols)
        if trading:
            if not self.has_credentials():
                raise VenueError(f"{self.name}: missing {self.cfg.env_prefix}_ACCOUNT_INDEX / _API_KEY_INDEX / _API_KEY_PRIVATE_KEY")
            key_index = int(self.api_key_index)
            if self.name == "lighter_rh" and key_index in RH_RESERVED_KEY_INDEXES:
                raise VenueError("lighter_rh: API key indexes 0-3 and 157 are reserved, use 4-156 or 158-254")
            import lighter  # lazy: native signer library

            self.signer = lighter.SignerClient(
                url=self.cfg.rest_url,
                account_index=int(self.account_index),
                api_private_keys={key_index: self._api_private_key},
                chain_id=self.cfg.chain_id,
            )
            err = self.signer.check_client()
            if err is not None:
                raise VenueError(f"{self.name}: API key check failed: {err}")
            self._tasks.append(asyncio.create_task(self._private_ws_loop(), name=f"{self.name}-private-ws"))
        self.trading = trading
        self._tasks.append(asyncio.create_task(self._public_ws_loop(), name=f"{self.name}-public-ws"))

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
        if self.signer is not None:
            try:
                await self.signer.close()
            except Exception:  # noqa: BLE001
                pass
        if self._session:
            await self._session.close()

    # ------------------------------------------------------------ REST
    async def _get(self, path: str, params: Optional[dict] = None, cost: float = 1.0) -> Any:
        await self.limiter.acquire(cost)
        assert self._session is not None
        async with self._session.get(self.cfg.rest_url.rstrip("/") + path, params=params) as r:
            text = await r.text()
            if r.status != 200:
                raise VenueError(f"{self.name} GET {path} -> HTTP {r.status}: {text[:200]}")
            return json.loads(text)

    async def _load_markets(self, symbols: list[str]) -> None:
        data = await self._get("/api/v1/orderBooks")
        by_symbol = {m["symbol"]: m for m in data.get("order_books", []) if m.get("market_type", "perp") == "perp"}
        for sym in symbols:
            m = by_symbol.get(sym)
            if m is None:
                log.warning("%s: market %s not listed, skipped", self.name, sym)
                continue
            if m.get("status") != "active":
                log.warning("%s: market %s status=%s, skipped", self.name, sym, m.get("status"))
                continue
            size_dec, price_dec = int(m["supported_size_decimals"]), int(m["supported_price_decimals"])
            mid = int(m["market_id"])
            self.market_ids[sym] = mid
            self._symbol_by_mid[mid] = sym
            self.markets[sym] = MarketInfo(
                venue=self.name,
                symbol=sym,
                size_step=Decimal(1).scaleb(-size_dec),
                min_qty=D(m.get("min_base_amount") or 0),
                min_notional=D(m.get("min_quote_amount") or 0),
                price_tick=Decimal(1).scaleb(-price_dec),
                maker_fee_pct=float(m.get("maker_fee") or 0),
                taker_fee_pct=float(m.get("taker_fee") or 0),
                extra={"market_id": mid, "size_decimals": size_dec, "price_decimals": price_dec},
            )

    async def _account(self, max_age: float = 2.0) -> dict:
        async with self._account_lock:
            now = time.time()
            if self._account_cache and now - self._account_cache[0] <= max_age:
                return self._account_cache[1]
            if not self.account_index:
                raise VenueError(f"{self.name}: {self.cfg.env_prefix}_ACCOUNT_INDEX not set")
            data = await self._get("/api/v1/account", {"by": "index", "value": self.account_index})
            accounts = data.get("accounts") or []
            if not accounts:
                raise VenueError(f"{self.name}: account {self.account_index} not found")
            self._account_cache = (now, accounts[0])
            return accounts[0]

    async def positions(self) -> dict[str, Position]:
        acct = await self._account(max_age=0.0)
        out: dict[str, Position] = {}
        for p in acct.get("positions") or []:
            sym = self._symbol_by_mid.get(int(p.get("market_id", -1)))
            if sym is None:
                continue
            qty = D(p.get("position") or 0) * (1 if int(p.get("sign", 1)) >= 0 else -1)
            out[sym] = Position(self.name, sym, qty, D(p["avg_entry_price"]) if p.get("avg_entry_price") else None)
        return out

    async def balance(self) -> Balance:
        acct = await self._account()
        equity = D(acct.get("total_asset_value") or acct.get("collateral") or 0)
        return Balance(self.name, equity=equity, available=D(acct.get("available_balance") or 0))

    # ------------------------------------------------------------ public WS
    def _public_url(self) -> str:
        return self.cfg.ws_url + ("?readonly=true" if self.cfg.public_ws_readonly else "")

    async def _public_ws_loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                async with websockets.connect(self._public_url(), ping_interval=None, max_size=None, open_timeout=15) as ws:
                    self._public_ws = ws
                    for mid in self._symbol_by_mid:
                        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"}))
                        await ws.send(json.dumps({"type": "subscribe", "channel": f"market_stats/{mid}"}))
                    self.connected = True
                    backoff = 1.0
                    last_ping = time.monotonic()
                    while self._running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            raw = None
                        if time.monotonic() - last_ping > 25:
                            await ws.send('{"type":"ping"}')
                            last_ping = time.monotonic()
                        if raw is None:
                            continue
                        self.alive_ts = time.time()
                        await self._on_public(ws, json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("%s public WS error: %s; reconnect in %.0fs", self.name, exc, backoff)
            self.connected = False
            self._public_ws = None
            self._raw.clear()
            self.books.clear()
            self.market_event.set()
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _on_public(self, ws, m: dict) -> None:
        typ = m.get("type", "")
        if typ == "ping":
            await ws.send('{"type":"pong"}')
            return
        chan = str(m.get("channel", ""))
        if typ in ("subscribed/order_book", "update/order_book"):
            mid = int(chan.split(":")[1])
            ob = m.get("order_book") or {}
            book = self._raw.get(mid)
            if typ == "subscribed/order_book" or book is None:
                if typ == "update/order_book":
                    return  # wait for snapshot
                book = _RawBook()
                self._raw[mid] = book
            elif ob.get("begin_nonce") is not None and book.nonce is not None and int(ob["begin_nonce"]) != book.nonce:
                log.info("%s order book gap on market %s, resubscribing", self.name, mid)
                self._raw.pop(mid, None)
                self.books.pop(self._symbol_by_mid.get(mid, ""), None)
                await ws.send(json.dumps({"type": "unsubscribe", "channel": f"order_book/{mid}"}))
                await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"}))
                return
            book.apply(book.bids, ob.get("bids") or [])
            book.apply(book.asks, ob.get("asks") or [])
            if ob.get("nonce") is not None:
                book.nonce = int(ob["nonce"])
            ts_ms = m.get("timestamp") or 0
            self._publish(mid, book, float(ts_ms) / 1000.0 if ts_ms else time.time())
        elif typ in ("subscribed/market_stats", "update/market_stats"):
            st = m.get("market_stats") or {}
            sym = self._symbol_by_mid.get(int(st.get("market_id", -1)))
            rate = st.get("current_funding_rate")
            if sym and rate not in (None, ""):
                self.funding_hourly[sym] = float(rate) / 100.0  # Lighter reports percent per hour
        elif typ == "error" or "error" in m:
            log.warning("%s WS error message: %s", self.name, str(m)[:300])

    def _publish(self, mid: int, raw: _RawBook, ts_exchange: float) -> None:
        sym = self._symbol_by_mid.get(mid)
        if sym is None:
            return
        bids = sorted(raw.bids.items(), key=lambda kv: kv[0], reverse=True)[:BOOK_DEPTH]
        asks = sorted(raw.asks.items(), key=lambda kv: kv[0])[:BOOK_DEPTH]
        self.books[sym] = OrderBook(
            venue=self.name, symbol=sym,
            bids=[Level(p, s) for p, s in bids], asks=[Level(p, s) for p, s in asks],
            ts_exchange=ts_exchange, ts_local=time.time(),
        )
        self.market_event.set()

    # ------------------------------------------------------------ private WS
    def _auth_token(self) -> str:
        token, err = self.signer.create_auth_token_with_expiry(api_key_index=int(self.api_key_index))
        if err:
            raise VenueError(f"{self.name}: auth token error: {err}")
        return token

    async def _private_ws_loop(self) -> None:
        backoff = 1.0
        acct = int(self.account_index)
        while self._running:
            try:
                token = self._auth_token()
                async with websockets.connect(self.cfg.ws_url, ping_interval=None, max_size=None, open_timeout=15) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"account_all_orders/{acct}", "auth": token}))
                    self.private_connected = True
                    backoff = 1.0
                    opened = last_ping = time.monotonic()
                    asyncio.create_task(self._refresh_open_orders())  # cover any gap while disconnected
                    while self._running and time.monotonic() - opened < TOKEN_REFRESH_S:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=20)
                        except asyncio.TimeoutError:
                            raw = None
                        if time.monotonic() - last_ping > 25:
                            await ws.send('{"type":"ping"}')
                            last_ping = time.monotonic()
                        if raw is None:
                            continue
                        m = json.loads(raw)
                        if m.get("type") == "ping":
                            await ws.send('{"type":"pong"}')
                        elif "orders" in m:
                            self._on_orders(m.get("orders") or {})
                        elif m.get("type") == "error" or "error" in m:
                            log.warning("%s private WS error message: %s", self.name, str(m)[:300])
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("%s private WS error: %s; reconnect in %.0fs", self.name, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            self.private_connected = False

    def _on_orders(self, orders: dict) -> None:
        for _mid, lst in orders.items():
            for o in lst or []:
                self._apply_order_json(o)

    def _apply_order_json(self, o: dict) -> bool:
        coi = str(o.get("client_order_index", o.get("client_order_id", "")))
        st = self.orders.get(coi)
        if st is None:
            return False
        filled = D(o.get("filled_base_amount") or 0)
        quote = D(o.get("filled_quote_amount") or 0)
        avg = quote / filled if filled > 0 and quote > 0 else None
        status = map_status(str(o.get("status", "")))
        st.apply(status=status, filled=filled, avg_price=avg, exchange_id=str(o.get("order_index", "")) or None,
                 reason=str(o.get("status", "")) if status in (OrderStatus.CANCELED, OrderStatus.UNKNOWN) else "")
        self._notify(coi)
        return True

    # ------------------------------------------------------------ orders
    def _ints(self, symbol: str, qty: Decimal, price: Decimal) -> tuple[int, int, int]:
        m = self.market(symbol)
        base = int((qty / m.size_step).to_integral_value())
        px = int((price / m.price_tick).to_integral_value())
        if base <= 0 or px <= 0:
            raise VenueError(f"{self.name}: invalid order qty={qty} price={price}")
        return m.extra["market_id"], base, px

    async def _submit(self, st: OrderState, symbol: str, *, order_type: int, tif: int, expiry: int) -> OrderState:
        mid, base, px = self._ints(symbol, st.qty, st.price)
        self._track(st)
        await self.limiter.acquire()
        try:
            _tx, resp, err = await self.signer.create_order(
                market_index=mid, client_order_index=int(st.client_id), base_amount=base, price=px,
                is_ask=not st.is_buy, order_type=order_type, time_in_force=tif,
                reduce_only=st.reduce_only, order_expiry=expiry,
            )
        except Exception as exc:  # noqa: BLE001 - network: state unknown, must be resolved by refresh
            st.apply(status=OrderStatus.UNKNOWN, reason=f"submit exception: {exc}")
            log.error("%s submit exception (state unknown) %s: %s", self.name, st.client_id, exc)
            return st
        code = getattr(resp, "code", None)
        if err is not None or resp is None or (code is not None and int(code) != 200):
            st.apply(status=OrderStatus.REJECTED, reason=str(err or getattr(resp, "message", resp)))
            log.warning("%s order rejected %s: %s", self.name, st.client_id, st.reason)
        return st

    async def place_post_only(self, symbol, is_buy, qty, price, reduce_only) -> OrderState:
        import lighter

        st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, price, post_only=True, reduce_only=reduce_only)
        return await self._submit(st, symbol, order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
                                  tif=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
                                  expiry=lighter.SignerClient.DEFAULT_28_DAY_ORDER_EXPIRY)

    async def place_ioc(self, symbol, is_buy, qty, limit_price, reduce_only) -> OrderState:
        import lighter

        st = OrderState(str(new_client_index()), self.name, symbol, is_buy, qty, limit_price, post_only=False, reduce_only=reduce_only)
        # Market order: `price` is the worst acceptable execution price (same as SDK create_market_order).
        await self._submit(st, symbol, order_type=lighter.SignerClient.ORDER_TYPE_MARKET,
                           tif=lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                           expiry=lighter.SignerClient.DEFAULT_IOC_EXPIRY)
        if st.status == OrderStatus.REJECTED:
            return st
        return await self.wait_final(st, timeout=8.0, poll=1.5)

    async def _find_order(self, st: OrderState) -> Optional[dict]:
        """REST lookup by client_order_index: active orders first, then recent inactive ones."""
        mid = self.market(st.symbol).extra["market_id"]
        api = self.signer.order_api
        await self.limiter.acquire()
        res = await api.account_active_orders(authorization=self._auth_token(), account_index=int(self.account_index), market_id=mid)
        for o in getattr(res, "orders", None) or []:
            d = o.to_dict() if hasattr(o, "to_dict") else dict(o)
            if str(d.get("client_order_index")) == st.client_id:
                return d
        await self.limiter.acquire()
        res = await api.account_inactive_orders(authorization=self._auth_token(), account_index=int(self.account_index), limit=50, market_id=mid)
        for o in getattr(res, "orders", None) or []:
            d = o.to_dict() if hasattr(o, "to_dict") else dict(o)
            if str(d.get("client_order_index")) == st.client_id:
                return d
        return None

    async def refresh_order(self, st: OrderState) -> OrderState:
        d = await self._find_order(st)
        if d is not None:
            self._apply_order_json(d)
        elif st.status == OrderStatus.UNKNOWN and time.time() - st.created > 30:
            # never reached the sequencer: not active, not inactive after 30s
            st.apply(status=OrderStatus.REJECTED, reason="not found on exchange")
        return st

    async def _refresh_open_orders(self) -> None:
        for st in list(self.orders.values()):
            if not st.status.final:
                try:
                    await self.refresh_order(st)
                except Exception as exc:  # noqa: BLE001
                    log.debug("%s refresh after reconnect failed: %s", self.name, exc)

    async def cancel(self, st: OrderState) -> None:
        if st.status.final:
            return
        if not st.exchange_id:
            await self.wait_order(st, 2.0)
        if not st.exchange_id:
            await self.refresh_order(st)
        if st.status.final:
            return
        if not st.exchange_id:
            raise VenueError(f"{self.name}: cannot cancel {st.client_id}, exchange order index unknown")
        await self.limiter.acquire()
        _tx, resp, err = await self.signer.cancel_order(market_index=self.market(st.symbol).extra["market_id"], order_index=int(st.exchange_id))
        if err is not None:
            log.warning("%s cancel %s returned: %s", self.name, st.client_id, err)

    async def cancel_all(self, symbols: list[str]) -> None:
        if self.signer is None:
            return
        for sym in symbols:
            if sym not in self.markets:
                continue
            mid = self.markets[sym].extra["market_id"]
            await self.limiter.acquire()
            res = await self.signer.order_api.account_active_orders(authorization=self._auth_token(), account_index=int(self.account_index), market_id=mid)
            for o in getattr(res, "orders", None) or []:
                d = o.to_dict() if hasattr(o, "to_dict") else dict(o)
                await self.limiter.acquire()
                _tx, _resp, err = await self.signer.cancel_order(market_index=mid, order_index=int(d["order_index"]))
                if err:
                    log.warning("%s cancel_all %s/%s: %s", self.name, sym, d.get("order_index"), err)
