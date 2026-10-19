"""Coordination between parallel instances (one process per venue pair).

Every instance trades exactly two venues. Several instances may share an exchange
account (e.g. `lighter+arcus` and `lighter+entropy` both on the same Lighter account).
To keep them from fighting over the same position (netting, reduce-only rejections,
cancel_all hitting the other process' orders, false reconcile mismatches) a symbol on
a given venue account is owned by at most one instance at a time:

  * an exclusive, non-blocking flock on data/claims/<venue>__<account>__<symbol>.lock,
    held for the life of the process (released automatically on crash), and
  * a check of the other instances' persisted state: a pair that still holds a position
    in that symbol on the same account keeps it even while that instance is stopped.
"""
from __future__ import annotations

import fcntl
import json
import logging
import re
from pathlib import Path
from typing import IO

log = logging.getLogger("arb.claims")


def _safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s)


class Claims:
    def __init__(self, root: Path, my_pair: str):
        self.root = Path(root)
        self.my_pair = my_pair
        self.dir = self.root / "claims"
        self.dir.mkdir(parents=True, exist_ok=True)
        self._held: dict[str, IO] = {}

    def write_accounts(self, accounts: dict[str, str]) -> None:
        d = self.root / self.my_pair
        d.mkdir(parents=True, exist_ok=True)
        (d / "accounts.json").write_text(json.dumps(accounts))

    def held_elsewhere(self, venue: str, account: str, symbol_key: str) -> str:
        """Pair key of another instance that still has state for this symbol on this account, else ''."""
        for f in self.root.glob("*/positions.json"):
            pair = f.parent.name
            if pair == self.my_pair or venue not in pair.split("+"):
                continue
            try:
                accts = json.loads((f.parent / "accounts.json").read_text())
                if accts.get(venue) != account:
                    continue
                for k, v in json.loads(f.read_text()).items():
                    if k.split("|")[-1] == symbol_key and (v.get("grids") or float(v.get("residual_maker") or 0)
                                                           or float(v.get("residual_taker") or 0)):
                        return pair
            except (OSError, ValueError):
                continue
        return ""

    def acquire(self, venue: str, account: str, symbol_key: str) -> str:
        """Returns '' on success, otherwise the reason the symbol is not available."""
        other = self.held_elsewhere(venue, account, symbol_key)
        if other:
            return f"{venue} {symbol_key} has open state in instance {other}"
        name = f"{_safe(venue)}__{_safe(account)}__{_safe(symbol_key)}"
        if name in self._held:
            return ""
        fh = (self.dir / f"{name}.lock").open("a+")
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fh.seek(0)
            owner = fh.read().strip() or "another instance"
            fh.close()
            return f"{venue} {symbol_key} is traded by {owner}"
        fh.seek(0)
        fh.truncate()
        fh.write(self.my_pair)
        fh.flush()
        self._held[name] = fh
        return ""

    def release_all(self) -> None:
        for fh in self._held.values():
            try:
                fcntl.flock(fh, fcntl.LOCK_UN)
                fh.close()
            except OSError:
                pass
        self._held.clear()
