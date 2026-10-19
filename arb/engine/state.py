"""Persistent grid positions per (pair, symbol)."""
from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Optional

from arb.engine.signals import LONG_MAKER, SHORT_MAKER

ZERO = Decimal("0")


@dataclass
class Grid:
    level: int
    qty: Decimal  # hedged quantity (positive)
    target_qty: Decimal
    open_dev_sum: float = 0.0  # qty-weighted sum of realised opening edge (pct)
    opened_ts: float = field(default_factory=time.time)
    # FIFO opening lots [qty, raw two-leg opening edge pct (not basis-adjusted), fill time] for the close floor
    lots: list = field(default_factory=list)

    def take_lots(self, qty: Decimal) -> list:
        """Remove `qty` FIFO from the lots; returns the removed [qty, raw, ts] parts."""
        out, left = [], qty
        while left > 0 and self.lots:
            q, raw, ts = self.lots[0]
            part = min(q, left)
            out.append([part, raw, ts])
            if part >= q:
                self.lots.pop(0)
            else:
                self.lots[0] = [q - part, raw, ts]
            left -= part
        return out

    def close_floor(self, qty: Decimal, now: float, timeout_s: float, min_profit_pct: float) -> Optional[float]:
        """Minimum RAW closing edge so that the young (< timeout) FIFO lots closed by `qty` earn
        >= min_profit_pct on both legs together (before fees). None when no young attributed lot."""
        left, wq, wraw = qty, 0.0, 0.0
        for q, raw, ts in self.lots:
            if left <= 0:
                break
            part = min(q, left)
            left -= part
            if raw is not None and now - ts < timeout_s:
                wq += float(part)
                wraw += float(part) * raw
        if wq <= 0:
            return None
        return min_profit_pct - wraw / wq

    @property
    def open_dev(self) -> Optional[float]:
        return self.open_dev_sum / float(self.qty) if self.qty > 0 else None


@dataclass
class SymbolPosition:
    pair_key: str
    symbol: str
    direction: Optional[str] = None  # short_maker | long_maker
    grids: list[Grid] = field(default_factory=list)
    residual_maker: Decimal = ZERO  # signed unhedged qty on maker venue (venue-native units)
    residual_taker: Decimal = ZERO  # signed unhedged qty on taker venue
    realized_edge_usdc: float = 0.0  # gross price capture estimate (before fees/funding)
    updated: float = field(default_factory=time.time)

    @property
    def total_qty(self) -> Decimal:
        return sum((g.qty for g in self.grids), ZERO)

    @property
    def sign(self) -> int:
        return -1 if self.direction == SHORT_MAKER else 1

    def expected_maker_qty(self) -> Decimal:
        return (self.sign * self.total_qty if self.direction else ZERO) + self.residual_maker

    def expected_taker_qty(self) -> Decimal:
        return (-self.sign * self.total_qty if self.direction else ZERO) + self.residual_taker

    def is_flat(self) -> bool:
        return not self.grids and self.residual_maker == 0 and self.residual_taker == 0

    def has_exposure(self, eps: Decimal = ZERO) -> bool:
        return self.total_qty > eps or abs(self.residual_maker) > eps or abs(self.residual_taker) > eps

    # -------------------------------------------------------------- mutations
    def add_open(self, direction: str, level: int, hedged: Decimal, target: Decimal, dev: float,
                 raw: Optional[float] = None, ts: Optional[float] = None) -> None:
        if hedged <= 0:
            return
        if self.direction is None:
            self.direction = direction
        assert self.direction == direction, "cannot add opposite direction"
        g = self.grids[-1] if self.grids and self.grids[-1].level == level else None
        if g is None:
            g = Grid(level=level, qty=ZERO, target_qty=target)
            self.grids.append(g)
        g.qty += hedged
        g.open_dev_sum += dev * float(hedged)
        g.lots.append([hedged, raw, ts if ts is not None else time.time()])
        self.updated = time.time()

    def reduce_close(self, hedged: Decimal) -> None:
        left = hedged
        while left > 0 and self.grids:
            g = self.grids[-1]
            take = min(left, g.qty)
            if g.qty > 0:
                g.open_dev_sum -= (g.open_dev_sum / float(g.qty)) * float(take)
            g.qty -= take
            g.take_lots(take)
            left -= take
            if g.qty <= 0:
                self.grids.pop()
        if not self.grids:
            self.direction = None if self.residual_maker == 0 and self.residual_taker == 0 else self.direction
        self.updated = time.time()

    def merge_dust(self, min_qty: Decimal) -> None:
        """Fold a top grid that is too small to trade into the grid below it."""
        while len(self.grids) >= 2 and self.grids[-1].qty < min_qty:
            top = self.grids.pop()
            self.grids[-1].qty += top.qty
            self.grids[-1].open_dev_sum += top.open_dev_sum
            self.grids[-1].lots.extend(top.lots)

    # -------------------------------------------------------------- json
    def to_json(self) -> dict:
        d = asdict(self)
        d["residual_maker"] = str(self.residual_maker)
        d["residual_taker"] = str(self.residual_taker)
        d["grids"] = [{**asdict(g), "qty": str(g.qty), "target_qty": str(g.target_qty),
                       "lots": [[str(q), r, t] for q, r, t in g.lots]} for g in self.grids]
        return d

    @classmethod
    def from_json(cls, d: dict) -> "SymbolPosition":
        sp = cls(pair_key=d["pair_key"], symbol=d["symbol"], direction=d.get("direction"),
                 residual_maker=Decimal(d.get("residual_maker", "0")), residual_taker=Decimal(d.get("residual_taker", "0")),
                 realized_edge_usdc=float(d.get("realized_edge_usdc", 0.0)), updated=float(d.get("updated", time.time())))
        for g in d.get("grids", []):
            sp.grids.append(Grid(level=int(g["level"]), qty=Decimal(g["qty"]), target_qty=Decimal(g["target_qty"]),
                                 open_dev_sum=float(g.get("open_dev_sum", 0.0)), opened_ts=float(g.get("opened_ts", time.time())),
                                 lots=[[Decimal(q), (None if r is None else float(r)), float(t)] for q, r, t in g.get("lots", [])]))
        if sp.direction not in (None, SHORT_MAKER, LONG_MAKER):
            raise ValueError(f"bad direction in state: {sp.direction}")
        return sp


class PositionBook:
    def __init__(self, path: Path):
        self.path = path
        self.items: dict[str, SymbolPosition] = {}
        if path.exists():
            raw = json.loads(path.read_text())
            for k, v in raw.items():
                self.items[k] = SymbolPosition.from_json(v)

    @staticmethod
    def key(pair_key: str, symbol: str) -> str:
        return f"{pair_key}|{symbol}"

    def get(self, pair_key: str, symbol: str) -> SymbolPosition:
        k = self.key(pair_key, symbol)
        if k not in self.items:
            self.items[k] = SymbolPosition(pair_key=pair_key, symbol=symbol)
        return self.items[k]

    def open_pairs(self) -> set[str]:
        return {sp.pair_key for sp in self.items.values() if not sp.is_flat()}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {k: v.to_json() for k, v in self.items.items() if not v.is_flat() or v.realized_edge_usdc}
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), prefix=".positions", suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=1)
        os.replace(tmp, self.path)
