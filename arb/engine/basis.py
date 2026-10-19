"""Time-weighted EMA of the natural price basis between two venues.

basis = (mid_maker - mid_taker) / mid_taker * 100   (percent)

One state per (pair, symbol). Only valid paired samples with gaps <= max_gap
accumulate warmup time. Gaps longer than max_gap require `recovery_seconds` of
fresh samples before the EMA is trusted again; gaps longer than reset_after
discard the state entirely (full warmup).
"""
from __future__ import annotations

import json
import math
import os
import tempfile
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from arb.config import BasisConfig


@dataclass
class BasisState:
    cfg: BasisConfig
    ema: Optional[float] = None
    last_ts: Optional[float] = None
    last_value: Optional[float] = None
    accumulated: float = 0.0
    recovery_left: float = 0.0
    history: deque = field(default_factory=deque)  # (ts, ema) for drift check

    def observe(self, ts: float, value: Optional[float]) -> None:
        c = self.cfg
        if value is None or not math.isfinite(value):
            return  # invalid sample: no accumulation; the gap is handled on the next valid one
        if self.last_ts is not None and ts - self.last_ts < c.sample_interval_seconds * 0.999:
            return
        if self.last_ts is None or self.ema is None:
            self.ema = value
            self.accumulated = 0.0
        else:
            dt = ts - self.last_ts
            if dt > c.reset_after_seconds:
                self.ema, self.accumulated, self.recovery_left = value, 0.0, 0.0
                self.history.clear()
            elif dt > c.max_gap_seconds:
                self.recovery_left = c.recovery_seconds
                alpha = 1.0 - math.exp(-c.sample_interval_seconds / c.ema_seconds)
                self.ema += alpha * (value - self.ema)
            else:
                alpha = 1.0 - math.exp(-dt / c.ema_seconds)
                self.ema += alpha * (value - self.ema)
                self.accumulated += dt
                self.recovery_left = max(0.0, self.recovery_left - dt)
        self.last_ts = ts
        self.last_value = value
        self.history.append((ts, self.ema))
        cutoff = ts - c.drift_seconds - 60
        while self.history and self.history[0][0] < cutoff:
            self.history.popleft()

    @property
    def warm(self) -> bool:
        return self.ema is not None and self.accumulated >= self.cfg.warmup_seconds

    def drift(self) -> Optional[float]:
        if not self.history or self.last_ts is None:
            return None
        target = self.last_ts - self.cfg.drift_seconds
        old = None
        for ts, e in self.history:
            if ts <= target:
                old = e
            else:
                break
        if old is None:
            old = self.history[0][1]
        return abs(self.ema - old)

    def ready(self, now: float) -> tuple[bool, str]:
        c = self.cfg
        if self.ema is None or self.last_ts is None:
            return False, "basis: no data"
        if now - self.last_ts > c.max_gap_seconds:
            return False, "basis: stale"
        if not self.warm:
            return False, f"basis warmup {self.accumulated / c.warmup_seconds:.0%}"
        if self.recovery_left > 0:
            return False, f"basis recovering {self.recovery_left:.0f}s"
        d = self.drift()
        if d is not None and d > c.max_drift_pct:
            return False, f"basis drift {d:.3f}% > {c.max_drift_pct}%"
        return True, ""

    def to_json(self) -> dict:
        return {"ema": self.ema, "last_ts": self.last_ts, "accumulated": self.accumulated,
                "recovery_left": self.recovery_left, "history": list(self.history)[-4000:]}

    @classmethod
    def from_json(cls, cfg: BasisConfig, d: dict, now: float) -> "BasisState":
        st = cls(cfg)
        last = d.get("last_ts")
        if d.get("ema") is None or last is None or now - last > cfg.reset_after_seconds:
            return st  # too old: full warmup
        st.ema = float(d["ema"])
        st.last_ts = float(last)
        st.accumulated = float(d.get("accumulated", 0.0))
        st.recovery_left = cfg.recovery_seconds if now - last > cfg.max_gap_seconds else float(d.get("recovery_left", 0.0))
        st.history = deque((float(a), float(b)) for a, b in d.get("history", []))
        return st


class BasisBook:
    """All basis states, persisted to data/basis_state.json."""

    def __init__(self, path: Path):
        self.path = path
        self.states: dict[str, BasisState] = {}
        self._loaded: dict = {}
        if path.exists():
            try:
                self._loaded = json.loads(path.read_text())
            except (OSError, ValueError):
                self._loaded = {}

    @staticmethod
    def key(pair_key: str, symbol: str) -> str:
        return f"{pair_key}|{symbol}"

    def get(self, pair_key: str, symbol: str, cfg: BasisConfig, now: float) -> BasisState:
        k = self.key(pair_key, symbol)
        st = self.states.get(k)
        if st is None:
            raw = self._loaded.get(k)
            st = BasisState.from_json(cfg, raw, now) if raw else BasisState(cfg)
            self.states[k] = st
        return st

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {k: s.to_json() for k, s in self.states.items()}
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), prefix=".basis", suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp, self.path)
