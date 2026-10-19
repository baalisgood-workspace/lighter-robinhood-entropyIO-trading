"""Hardened network transport shared by all adapters (ported from arcus-maker-lighter-taker).

* WebSocket liveness counts EVERY received frame, including protocol ping/pong control frames,
  so a quiet but healthy connection is not mistaken for a stale one, and a dead one is detected.
* Active protocol pings every `liveness_s / 2`; the socket is closed when no pong arrives within
  `liveness_s + quick_resume_s` (the adapter then reconnects and resets its books).
* Redirects are refused on WebSocket handshakes and on every HTTP call: a redirected write is
  never followed (credentials / signed payloads must only reach the configured endpoint).
"""
from __future__ import annotations

import logging
from typing import Callable

import aiohttp
from websockets.asyncio.client import ClientConnection, connect
from websockets.frames import Frame

log = logging.getLogger("arb.transport")


class RedirectRefused(RuntimeError):
    pass


class _ReceivedConnection(ClientConnection):
    def __init__(self, *args, on_received: Callable[[], None], **kwargs):
        self._on_received = on_received
        super().__init__(*args, **kwargs)

    def process_event(self, event):  # called by websockets for every parsed incoming frame
        super().process_event(event)
        if isinstance(event, Frame):
            try:
                self._on_received()
            except Exception:  # noqa: BLE001 - liveness hook must never break the reader
                pass


class _NoRedirectConnect(connect):
    def process_redirect(self, exc):  # returning the exception forbids following any redirect
        return exc


def ws_connect(url: str, on_received: Callable[[], None], liveness_s: float = 5.0, quick_resume_s: float = 10.0,
               **kwargs) -> _NoRedirectConnect:
    """`async with ws_connect(...) as ws:` - websockets connection with frame liveness + pings."""
    opts = dict(ping_interval=max(liveness_s / 2, 0.5), ping_timeout=liveness_s + quick_resume_s,
                max_size=None, open_timeout=15, close_timeout=2, proxy=None,
                create_connection=lambda *a, **k: _ReceivedConnection(*a, on_received=on_received, **k))
    opts.update(kwargs)
    return _NoRedirectConnect(url, **opts)


async def _refuse_redirect(session, ctx, params) -> None:
    raise RedirectRefused(f"redirect refused: {params.method} {params.url}")


def no_redirect_trace() -> aiohttp.TraceConfig:
    tc = aiohttp.TraceConfig()
    tc.on_request_redirect.append(_refuse_redirect)
    return tc


def http_session(total_timeout: float = 15.0) -> aiohttp.ClientSession:
    """aiohttp session that refuses redirects (also pass allow_redirects=False per request)."""
    return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=total_timeout), trace_configs=[no_redirect_trace()])


def harden_foreign_session(session) -> bool:
    """Best effort: make a third-party (SDK-owned) aiohttp session refuse redirects."""
    try:
        tc = no_redirect_trace()
        tc.freeze()
        session._trace_configs.append(tc)  # noqa: SLF001 - SDK does not expose the option
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("could not harden SDK HTTP session against redirects: %s", exc)
        return False


def harden_requests_session(session) -> None:
    """requests.Session (hyperliquid SDK): any redirect raises TooManyRedirects."""
    try:
        session.max_redirects = 0
    except Exception:  # noqa: BLE001
        pass
