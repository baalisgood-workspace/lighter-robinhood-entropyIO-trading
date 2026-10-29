"""Configuration loading and validation.

One YAML file (config/config.yaml) describes the four venues, which two are
enabled for trading, the maker venue of every pair, symbols and parameters.
"""
from __future__ import annotations

import copy
import itertools
from dataclasses import dataclass, field, fields
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import yaml

VENUE_NAMES = ("lighter", "lighter_rh", "entropy", "arcus")
VENUE_KINDS = {"lighter": "lighter", "lighter_rh": "lighter", "entropy": "hyperliquid", "arcus": "arcus"}


class ConfigError(ValueError):
    pass


def _build(cls, raw: dict, where: str):
    raw = dict(raw or {})
    names = {f.name for f in fields(cls)}
    unknown = set(raw) - names
    if unknown:
        raise ConfigError(f"{where}: unknown keys {sorted(unknown)}")
    try:
        return cls(**raw)
    except TypeError as exc:
        raise ConfigError(f"{where}: {exc}") from None


def _deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


@dataclass
class VenueConfig:
    name: str = ""
    enabled: bool = False
    rest_url: str = ""
    ws_url: str = ""
    chain_id: Optional[int] = None
    env_prefix: str = ""
    public_ws_readonly: bool = True  # Lighter: read-only market data stream works from any region
    requests_per_minute: float = 60.0
    maker_fee_pct: Optional[float] = None  # None -> use exchange-reported fee (Lighter) / required (entropy)
    taker_fee_pct: Optional[float] = None
    # v5.4: auto = use the exchange/account-reported fee per market (configured fees are the fallback); config = old
    fee_source: str = "auto"
    fee_refresh_s: float = 3600.0  # re-read the account fee tier / market fee schedule this often (0 = start only)
    dex: str = "io"  # entropy only: HIP-3 dex name
    leverage: int = 3  # entropy only: isolated leverage set at start in live mode
    funding_poll_seconds: float = 60.0
    unknown_lookup_s: float = 20.0  # IOC with ambiguous send result: keep looking it up this long before halting
    liveness_s: float = 5.0  # WS: protocol ping every liveness/2; any received frame counts as alive
    quick_resume_s: float = 10.0  # WS: socket closed when silent for liveness + quick_resume
    # v5.2: finished orders keep receiving exchange pushes this long; a fill that arrives after the executor
    # forgot the order is a "late fill" and is booked as residual (then hedged / flattened by the engine)
    late_fill_grace_s: float = 120.0
    # re-read every canceled maker order from REST after these delays (null: on for arcus, off for lighter)
    verify_cancel_rest: Optional[bool] = None
    verify_cancel_delays_s: list = field(default_factory=lambda: [2.0, 8.0])

    @property
    def kind(self) -> str:
        return VENUE_KINDS[self.name]


@dataclass
class StrategyConfig:
    initial_threshold_pct: float = 0.30  # T1: first grid opens when basis-adjusted edge (net of fees) >= T1
    grid_step_pct: float = 0.15  # grid k opens at T1 + (k-1)*step
    max_grids: int = 3
    first_close_ratio: float = 0.5  # grid 1 closes at T1*ratio; grid k>1 closes at grid k-1 open level
    grid_value_usdc: float = 200.0  # notional per grid per leg
    order_value_usdc: float = 100.0  # max notional per maker order (slice)
    session: str = "24x7"  # 24x7 | us_equity
    offhours_threshold_pct: Optional[float] = None  # us_equity: T1 outside NY regular hours
    # close-profit floor (FIFO two-leg): a grid younger than close_floor_timeout_s only closes when the
    # realised raw spread of its opening fills + the closing quote >= min_close_profit_pct (before fees).
    min_close_profit_pct: Optional[float] = 0.01  # null disables the floor
    close_floor_timeout_s: float = 600.0
    # signal persistence (lighter-rh spread_persistence_seconds): join/improve modes open only after the
    # edge stayed above the trigger continuously for this many seconds. 0 = immediately (previous behaviour)
    signal_confirm_s: float = 0.0
    # us_equity session: false = no new opens outside NY regular hours at all (closes keep working)
    offhours_open: bool = True
    # v5.7 funding carry (off by default). When the funding difference pays the hedged position at least
    # carry_min_hourly_pct (%/h, net of both legs), that direction opens earlier: the trigger is lowered by the
    # funding expected over carry_credit_hours, but never below fees + carry_min_open_pct. An open grid is then
    # held (no close) while the funding still pays >= carry_exit_hourly_pct and the oldest grid is younger than
    # carry_max_hold_hours. close_only (global or symbol) always closes normally. Holding means less volume.
    funding_carry: bool = False
    carry_min_hourly_pct: float = 0.005  # 0.005 %/h = 0.12 %/day ~ 44 %/year on the notional of one leg
    carry_exit_hourly_pct: float = 0.001
    carry_credit_hours: float = 8.0
    carry_min_open_pct: float = 0.0
    carry_max_hold_hours: Optional[float] = 72.0


@dataclass
class BasisConfig:
    enabled: bool = True
    ema_seconds: float = 3600.0
    warmup_seconds: float = 3600.0
    drift_seconds: float = 3600.0
    max_drift_pct: float = 0.30
    max_gap_seconds: float = 5.0
    recovery_seconds: float = 60.0
    reset_after_seconds: float = 600.0
    sample_interval_seconds: float = 1.0


@dataclass
class ExecutionConfig:
    # join: rest at best bid/ask; improve: one tick inside if spread allows;
    # threshold: rest at the exact price that yields the required edge vs the taker book (centred on the
    # basis EMA), clamped to the nearest passive tick - the order waits for the market to come to it.
    maker_price_mode: str = "join"
    requote_threshold_pct: float = 0.006  # threshold mode: reprice when the fair price moves more than this
    max_order_age_s: float = 30.0  # threshold mode: refresh a resting order at least this often
    max_requotes_per_s: float = 2.0  # cancel/replace rate limit per symbol (risk cancels bypass it)
    threshold_max_gap_pct: Optional[float] = None  # threshold mode: only rest when the edge is within this of the trigger
    maker_timeout_s: float = 20.0
    requote_when_not_best: bool = True
    max_requotes: int = 5
    requote_min_interval_s: float = 1.0
    cancel_adverse_pct: float = 0.03  # cancel maker when edge at our price drops below threshold - this
    hedge_slippage_pct: float = 0.05  # IOC limit = best price +/- this
    hedge_slippage_step_pct: float = 0.05  # extra slippage per retry
    hedge_max_attempts: int = 4  # counts only confirmed ZERO-fill IOCs; partial fills do not use up the budget
    hedge_max_orders: int = 12  # hard cap of hedge IOCs per hedge call (partials included)
    hedge_min_notional_usdc: float = 10.0  # accumulate partial maker fills until the hedge reaches this
    close_mode: str = "maker_taker"  # maker_taker | taker_taker
    # open_mode taker_taker: open with an IOC on the maker venue too (no resting order); the edge is priced
    # with immediate fills on both books and both taker fees
    open_mode: str = "maker_taker"  # maker_taker | taker_taker
    order_poll_interval_s: float = 1.0  # REST fallback poll period when no push update arrives
    order_final_timeout_s: float = 8.0  # wait for final state after cancel / IOC
    cooldown_s: float = 1.0
    # If the hedge fails: false (default) = keep both legs as they are, cancel all makers and lock the
    # instance for manual intervention; true = first try to flatten the unhedged maker fill with IOC.
    emergency_unwind: bool = False


@dataclass
class RiskConfig:
    max_quote_age_s: float = 3.0
    depth_levels: int = 5
    depth_band_pct: float = 0.10  # taker VWAP must fit inside best +/- band
    stability_window_s: float = 2.0
    stability_max_move_pct: float = 0.10
    funding_gate_enabled: bool = True
    funding_horizon_hours: float = 1.0
    max_adverse_funding_hourly_pct: float = 0.02
    min_available_usdc: float = 100.0
    max_symbol_value_usdc: float = 600.0
    max_venue_notional_usdc: float = 2000.0
    max_price_divergence_pct: float = 15.0  # sanity: same symbol on two venues must be within this
    reconcile_interval_s: float = 30.0
    reconcile_tolerance_mult: float = 1.5  # x max(min_qty) of the two venues
    max_consecutive_failures: int = 3
    failure_pause_s: float = 300.0
    warmup_fresh_s: float = 60.0  # continuous fresh two-venue data needed before quoting (after start/reconnect/gap)
    quick_resume_s: float = 10.0  # a freshness gap shorter than this keeps the warmup (same socket generation)
    max_leverage: float = 2.0  # gross notional on a venue (this instance) / venue equity
    reserve_ratio: float = 0.02  # keep this share of equity unused
    max_unhedged_notional_usdc: float = 500.0  # residual (unhedged) notional above this blocks new opens
    # ---- ported from lighter-rh (open-only blocks: closes keep working)
    volatility_max_pct: Optional[float] = 4.0  # (max-min)/min of the mid within the window >= this blocks opens
    volatility_window_s: float = 300.0
    volatility_block_s: float = 900.0
    max_leg_spread_pct: Optional[float] = None  # own bid-ask spread of either leg above this blocks opens
    max_daily_opens: Optional[int] = None  # open slices per UTC day per symbol
    reduce_only_block_s: float = 1800.0  # venue rejected an opening order with "reduce only": block opens this long
    max_hold_hours: Optional[float] = None  # warn (UI/event) when the oldest grid is older than this
    # ---- v5.2 auto-align (opt-in): a reconcile mismatch on ONE leg only (the other leg matches local state),
    # identical on two consecutive checks and worth at most this much, is adopted as residual (then settled
    # like any residual) instead of halting. 0 = off (always halt, as before). Capped per symbol per hour.
    auto_adopt_max_usdc: float = 0.0
    auto_adopt_max_per_hour: int = 3
    # ---- v5.6 dust clean-up: a grid that cannot be closed (a leg below the exchange minimum, or a close hedge
    # that can never be sent) for dust_grace_s, with every leg worth <= dust_max_usdc, is flattened on both
    # venues with taker orders (top up to the minimum, then reduce-only close). false = keep waiting (v5.5).
    dust_flatten: bool = True
    dust_max_usdc: float = 25.0
    dust_grace_s: float = 120.0
    # v5.6.1: residual-only dust (no grid left, an unhedged leg below the exchange minimum, <= dust_max_usdc) that
    # stayed unchanged this long is flattened on its venue the same way (top up, then reduce-only close)
    dust_residual_grace_s: float = 1800.0
    # ---- v5.6: an open signal that is not traded (blocked / unstable / opposite position / ...) is logged and
    # journaled (signal_skip) at most once per this many seconds per symbol and reason. 0 = off.
    skip_log_s: float = 300.0


@dataclass
class GuardsConfig:
    """Instance-wide pauses and alerts (ported from lighter-rh global risk control)."""
    bell: bool = True  # terminal bell on halt / pause / alert (throttled)
    bell_min_interval_s: float = 60.0
    # slippage pause: realised edge - required edge <= -slippage_pause_pct on a hedged slice
    slippage_pause_pct: Optional[float] = 0.08  # null disables
    slippage_pause_s: float = 900.0
    slippage_burst_count: int = 5  # this many slices with slippage <= -0.01% within the window also pause
    slippage_burst_window_s: float = 180.0
    slippage_pause_scope: str = "all"  # all = every symbol of this instance | symbol = only the affected one
    # hedge latency pause: taker IOC submit -> final state slower than this pauses new slices
    hedge_latency_max_s: Optional[float] = 3.2  # null disables
    hedge_latency_pause_s: float = 1000.0
    # venue error backoff: HTTP 429 / rate limit / invalid nonce -> pause that venue base*2^(n-1) seconds
    backoff_base_s: float = 10.0
    backoff_max_s: float = 300.0
    backoff_reset_s: float = 600.0  # error streak forgotten after this quiet time
    # API failure pause (transport errors / HTTP 5xx / timeouts) within a rolling window
    api_failure_pause: bool = False
    api_failure_count: int = 10
    api_failure_window_s: float = 600.0
    api_failure_pause_s: float = 900.0
    # alerts (never trade by themselves)
    equity_gap_warn_usdc: Optional[float] = 1000.0  # equity difference between the two venues
    equity_gap_interval_s: float = 300.0
    balance_warn_usdc: Optional[float] = None  # equity of a venue below this -> warning
    adverse_funding_warn_hours: Optional[float] = 8.0  # held position with adverse funding this long -> warning
    adverse_funding_warn_hourly_pct: float = 0.01


@dataclass
class AutoSymbolsConfig:
    """v5: add every symbol listed on >= min_venues venues automatically (arb/autosym.py)."""
    enabled: bool = False  # also: `symbols: auto` or CLI --all-symbols
    min_venues: int = 2
    include: list = field(default_factory=list)  # whitelist: auto symbols that may trade like configured ones
    exclude: list = field(default_factory=list)  # never add these
    max_symbols: Optional[int] = None  # cap on auto-added symbols (alphabetical after the include list)
    # which AUTO symbols may open positions (configured + include always may):
    #   none = monitor only | filtered = when the live liquidity filter passes | ranked = top N of `arb rank --save`
    trade: str = "none"
    ranked_top_n: int = 15
    rank_max_age_h: float = 72.0  # older rank file -> warning (still used)
    # live liquidity filter (EMA ~60 s of both books)
    max_book_spread_pct: Optional[float] = 0.15  # widest of the two books' bid/ask spread
    min_depth_usdc: Optional[float] = 300.0  # thinnest side within risk.depth_levels on either book
    filter_configured: bool = False  # also apply the filter to configured symbols
    categories: dict = field(default_factory=dict)  # extra classification {us_equity: [..], crypto: [..], other: [..]}
    templates: dict = field(default_factory=dict)  # per category {strategy: {...}, risk: {...}, ...}


@dataclass
class UiConfig:
    sort: str = "gap"  # gap = closest to the open trigger first (positions first) | config = config order
    max_book_rows: Optional[int] = None  # null = as many as the terminal fits


@dataclass
class SymbolConfig:
    key: str
    venues: dict[str, str]
    enabled: bool = True
    close_only: bool = False
    auto: bool = False  # added by auto_symbols
    category: str = ""  # us_equity | crypto | other (auto symbols)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    basis: BasisConfig = field(default_factory=BasisConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)


@dataclass(frozen=True)
class Pair:
    a: str
    b: str
    maker: str

    @property
    def key(self) -> str:
        return f"{self.a}+{self.b}"

    @property
    def taker(self) -> str:
        return self.b if self.maker == self.a else self.a

    @staticmethod
    def make(x: str, y: str, maker: str) -> "Pair":
        a, b = sorted((x, y), key=VENUE_NAMES.index)
        return Pair(a, b, maker)


def pair_key(x: str, y: str) -> str:
    a, b = sorted((x, y), key=VENUE_NAMES.index)
    return f"{a}+{b}"


# v5.6.1 threshold auto-tune advisor: default T1 bounds (%), by symbol first, then by category. Derived from the
# 10-23/10-24 journal of process B (Lighter RH <-> Lighter): crypto lost money at T1 0.05, ANTHROPIC earned at
# 0.06-0.15, OPENAI was mixed at 0.08-0.24. offhours_*: us_equity symbols with offhours_threshold_pct.
DEFAULT_TUNE_BOUNDS: dict = {
    "crypto": {"min": 0.08, "max": 0.25},
    "us_equity": {"min": 0.08, "max": 0.30, "offhours_min": 0.12, "offhours_max": 0.40},
    "other": {"min": 0.06, "max": 0.30},
    "ANTHROPIC": {"min": 0.06, "max": 0.20},
    "OPENAI": {"min": 0.10, "max": 0.25},
    "MU": {"min": 0.10, "max": 0.25, "offhours_min": 0.20, "offhours_max": 0.40},
}


@dataclass
class AutotuneConfig:
    """v5.6.1: evaluate the realised PnL per symbol and threshold every interval_s and journal advice
    (`tune_advice`). mode advise = never changes a parameter. v5.7 mode apply = the running bot moves T1 one
    step as advised (within the bounds), keeps it in data/<pair>/tune_applied.json across restarts and journals
    `tune_apply`; editing that T1 in the config file discards the applied value. A close_only advice is only
    applied with apply_close_only. Volume farming at break-even: lower_bps 0, raise_bps 0."""
    enabled: bool = True
    mode: str = "advise"
    apply_close_only: bool = False
    interval_s: float = 3600.0
    first_run_s: float = 120.0  # first evaluation this long after start
    lookback_h: float = 168.0  # closes of the last N hours
    min_closes: int = 30  # fewer closes at the current threshold: no advice (hold)
    step_pct: float = 0.01  # one suggested change = this much T1
    lower_bps: float = 1.0  # lower confidence bound of net bps/close > this -> suggest lowering T1
    raise_bps: float = 0.0  # upper confidence bound < this -> suggest raising T1 (at max: close_only)
    z: float = 1.645  # one-sided ~95% confidence
    bounds: dict = field(default_factory=dict)  # merged over DEFAULT_TUNE_BOUNDS, e.g. {LIT: {min: 0.10, max: 0.30}}

    def bounds_for(self, key: str, category: str) -> dict:
        out: dict = {}
        for k in (category, key):
            out.update(DEFAULT_TUNE_BOUNDS.get(k, {}))
        for k in (category, key):
            out.update(self.bounds.get(k, {}) or {})
        return out


@dataclass
class AppConfig:
    venues: dict[str, VenueConfig]
    roles: dict[str, str]  # pair key -> maker venue
    symbols: dict[str, SymbolConfig]
    watch_all_venues: bool = True
    close_only: bool = False
    allow_preexisting_positions: bool = False
    data_dir: str = "data"
    log_dir: str = "logs"
    sampler_interval_s: float = 5.0
    # v5.5: after every maker fill, record both venues' mids at these delays (markout / adverse-selection stats)
    markout_horizons_s: list = field(default_factory=lambda: [1.0, 5.0, 30.0, 60.0])
    max_parallel_slices: int = 1  # concurrent maker slices (different symbols) per instance
    ui_refresh_s: float = 1.0
    guards: GuardsConfig = field(default_factory=GuardsConfig)
    auto_symbols: AutoSymbolsConfig = field(default_factory=AutoSymbolsConfig)
    ui: UiConfig = field(default_factory=UiConfig)
    autotune: AutotuneConfig = field(default_factory=AutotuneConfig)
    source: str = ""

    def enabled_venues(self) -> list[str]:
        return [n for n in VENUE_NAMES if self.venues[n].enabled]

    def make_pair(self, x: str, y: str) -> Pair:
        k = pair_key(x, y)
        return Pair.make(x, y, self.roles[k])

    def active_pair(self) -> Pair:
        en = self.enabled_venues()
        if len(en) != 2:
            raise ConfigError(f"exactly two venues must be enabled for trading, got {en or 'none'}")
        return self.make_pair(*en)

    def watched_venues(self) -> list[str]:
        return list(VENUE_NAMES) if self.watch_all_venues else self.enabled_venues()

    def watched_pairs(self) -> list[Pair]:
        vs = self.watched_venues()
        return [self.make_pair(x, y) for x, y in itertools.combinations(vs, 2)]

    def symbols_for_pair(self, pair: Pair) -> list[SymbolConfig]:
        return [s for s in self.symbols.values() if s.enabled and pair.a in s.venues and pair.b in s.venues]

    def symbols_for_venue(self, venue: str) -> list[str]:
        return sorted({s.venues[venue] for s in self.symbols.values() if s.enabled and venue in s.venues})


DEFAULT_VENUES = {
    "lighter": dict(rest_url="https://mainnet.zklighter.elliot.ai", ws_url="wss://mainnet.zklighter.elliot.ai/stream",
                    chain_id=304, env_prefix="LIGHTER"),
    "lighter_rh": dict(rest_url="https://api.rh.lighter.xyz", ws_url="wss://api.rh.lighter.xyz/stream",
                       chain_id=466324, env_prefix="LIGHTER_RH"),
    "entropy": dict(rest_url="https://api.hyperliquid.xyz", ws_url="wss://api.hyperliquid.xyz/ws",
                    env_prefix="ENTROPY", requests_per_minute=600.0, maker_fee_pct=0.003, taker_fee_pct=0.009),
    "arcus": dict(rest_url="https://api.arcus.xyz", ws_url="wss://api.arcus.xyz/v1/ws",
                  env_prefix="ARCUS", requests_per_minute=120.0, maker_fee_pct=0.015, taker_fee_pct=0.045),
}

DEFAULT_ROLES = {
    "lighter+lighter_rh": "lighter_rh",
    "lighter+entropy": "entropy",
    "lighter_rh+entropy": "entropy",
    "lighter+arcus": "arcus",  # Arcus maker 0 / taker 0.0225%: quote on Arcus, hedge on fee-free Lighter
    "lighter_rh+arcus": "arcus",
    "entropy+arcus": "arcus",  # hedge on Entropy (taker 0.009%) rather than Arcus (0.0225%)
}


def read_raw(path: str | Path, overrides: Optional[dict] = None) -> dict:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if overrides:
        raw = _deep_merge(raw, overrides)
    return raw


def load_config(path: str | Path, overrides: Optional[dict] = None) -> AppConfig:
    cfg = parse_config(read_raw(path, overrides))
    cfg.source = str(path)
    return cfg


def auto_requested(raw: dict) -> bool:
    return raw.get("symbols") == "auto" or bool((raw.get("auto_symbols") or {}).get("enabled"))


def parse_venues(raw: dict) -> dict[str, VenueConfig]:
    venues: dict[str, VenueConfig] = {}
    raw_venues = raw.get("venues") or {}
    if set(raw_venues) - set(VENUE_NAMES):
        raise ConfigError(f"unknown venues: {sorted(set(raw_venues) - set(VENUE_NAMES))}; allowed {VENUE_NAMES}")
    for name in VENUE_NAMES:
        merged = {**DEFAULT_VENUES[name], **(raw_venues.get(name) or {})}
        merged["name"] = name
        venues[name] = _build(VenueConfig, merged, f"venues.{name}")
    return venues


def parse_config(raw: dict) -> AppConfig:
    raw = copy.deepcopy(raw)
    allowed_top = {"venues", "roles", "symbols", "defaults", "market_data", "trading", "paths", "sampler_interval_s", "markout_horizons_s",
                   "ui_refresh_s", "guards", "auto_symbols", "ui", "autotune"}
    if raw.get("symbols") == "auto":  # only auto symbols (expanded by arb/autosym.py before parsing)
        raw["symbols"] = {}
    unknown = {k for k in set(raw) - allowed_top if not str(k).startswith("x-")}  # x-*: YAML anchors
    if unknown:
        raise ConfigError(f"unknown top-level keys: {sorted(unknown)}")

    venues = parse_venues(raw)

    roles = dict(DEFAULT_ROLES)
    for k, v in (raw.get("roles") or {}).items():
        parts = [p.strip() for p in str(k).split("+")]
        if len(parts) != 2 or not all(p in VENUE_NAMES for p in parts) or parts[0] == parts[1]:
            raise ConfigError(f"roles: bad pair key {k!r}, use e.g. 'lighter+entropy'")
        if v not in parts:
            raise ConfigError(f"roles.{k}: maker {v!r} must be one of {parts}")
        roles[pair_key(*parts)] = v

    defaults = raw.get("defaults") or {}
    symbols: dict[str, SymbolConfig] = {}
    for key, sraw in (raw.get("symbols") or {}).items():
        sraw = sraw or {}
        extra = set(sraw) - {"venues", "enabled", "close_only", "strategy", "basis", "execution", "risk", "auto", "category"}
        if extra:
            raise ConfigError(f"symbols.{key}: unknown keys {sorted(extra)}")
        vmap = {str(k): str(v) for k, v in (sraw.get("venues") or {}).items()}
        if set(vmap) - set(VENUE_NAMES):
            raise ConfigError(f"symbols.{key}.venues: unknown venues {sorted(set(vmap) - set(VENUE_NAMES))}")
        if len(vmap) < 2:
            raise ConfigError(f"symbols.{key}: needs a market on at least two venues")
        symbols[key] = SymbolConfig(
            key=key,
            venues=vmap,
            enabled=bool(sraw.get("enabled", True)),
            close_only=bool(sraw.get("close_only", False)),
            auto=bool(sraw.get("auto", False)),
            category=str(sraw.get("category") or ""),
            strategy=_build(StrategyConfig, _deep_merge(defaults.get("strategy") or {}, sraw.get("strategy") or {}), f"symbols.{key}.strategy"),
            basis=_build(BasisConfig, _deep_merge(defaults.get("basis") or {}, sraw.get("basis") or {}), f"symbols.{key}.basis"),
            execution=_build(ExecutionConfig, _deep_merge(defaults.get("execution") or {}, sraw.get("execution") or {}), f"symbols.{key}.execution"),
            risk=_build(RiskConfig, _deep_merge(defaults.get("risk") or {}, sraw.get("risk") or {}), f"symbols.{key}.risk"),
        )
    if not symbols:
        raise ConfigError("no symbols configured (auto_symbols: market list unavailable?)")

    md = raw.get("market_data") or {}
    tr = raw.get("trading") or {}
    paths = raw.get("paths") or {}
    for section, data, keys in (("market_data", md, {"watch_all_venues"}),
                                ("trading", tr, {"close_only", "allow_preexisting_positions", "max_parallel_slices"}),
                                ("paths", paths, {"data_dir", "log_dir"})):
        if set(data) - keys:
            raise ConfigError(f"{section}: unknown keys {sorted(set(data) - keys)}")
    cfg = AppConfig(
        venues=venues,
        roles=roles,
        symbols=symbols,
        watch_all_venues=bool(md.get("watch_all_venues", True)),
        close_only=bool(tr.get("close_only", False)),
        allow_preexisting_positions=bool(tr.get("allow_preexisting_positions", False)),
        max_parallel_slices=int(tr.get("max_parallel_slices", 1)),
        data_dir=str(paths.get("data_dir", "data")),
        log_dir=str(paths.get("log_dir", "logs")),
        sampler_interval_s=float(raw.get("sampler_interval_s", 5.0)),
        markout_horizons_s=sorted(float(x) for x in (raw.get("markout_horizons_s", [1, 5, 30, 60]) or [])),
        ui_refresh_s=float(raw.get("ui_refresh_s", 1.0)),
        guards=_build(GuardsConfig, raw.get("guards") or {}, "guards"),
        auto_symbols=_build(AutoSymbolsConfig, raw.get("auto_symbols") or {}, "auto_symbols"),
        ui=_build(UiConfig, raw.get("ui") or {}, "ui"),
        autotune=_build(AutotuneConfig, raw.get("autotune") or {}, "autotune"),
    )
    errors = validate(cfg)
    if errors:
        raise ConfigError("invalid configuration:\n  - " + "\n  - ".join(errors))
    return cfg


def validate(cfg: AppConfig) -> list[str]:
    errs: list[str] = []
    for v in cfg.venues.values():
        if not v.rest_url.startswith("https://") or not v.ws_url.startswith("wss://"):
            errs.append(f"venues.{v.name}: rest_url must be https:// and ws_url wss://")
        if v.requests_per_minute <= 0:
            errs.append(f"venues.{v.name}.requests_per_minute must be > 0")
        for fee_name in ("maker_fee_pct", "taker_fee_pct"):
            fee = getattr(v, fee_name)
            if fee is not None and not (0 <= fee < 1):
                errs.append(f"venues.{v.name}.{fee_name} must be in [0, 1) percent")
        if v.fee_source not in ("auto", "config"):
            errs.append(f"venues.{v.name}.fee_source must be auto or config")
        if v.fee_refresh_s < 0:
            errs.append(f"venues.{v.name}.fee_refresh_s must be >= 0")
        if v.kind in ("hyperliquid", "arcus") and (v.maker_fee_pct is None or v.taker_fee_pct is None):
            errs.append(f"venues.{v.name}: maker_fee_pct/taker_fee_pct are required")
        if v.kind == "hyperliquid" and not (1 <= v.leverage <= 50):
            errs.append(f"venues.{v.name}.leverage must be 1..50")
    at = cfg.autotune
    if at.mode not in ("advise", "apply"):
        errs.append("autotune.mode must be advise (journal suggestions only) or apply (move T1 as advised)")
    if at.interval_s < 60 or at.lookback_h <= 0 or at.min_closes < 1 or at.step_pct <= 0 or at.z < 0:
        errs.append("autotune: interval_s >= 60, lookback_h > 0, min_closes >= 1, step_pct > 0, z >= 0")
    for k, b in (at.bounds or {}).items():
        if not isinstance(b, dict) or set(b) - {"min", "max", "offhours_min", "offhours_max"}:
            errs.append(f"autotune.bounds.{k}: use {{min, max, offhours_min, offhours_max}}")
            continue
        for lo, hi in (("min", "max"), ("offhours_min", "offhours_max")):
            if lo in b and hi in b and not (0 < float(b[lo]) <= float(b[hi])):
                errs.append(f"autotune.bounds.{k}: need 0 < {lo} <= {hi}")
    if any(h <= 0 or h > 600 for h in cfg.markout_horizons_s):
        errs.append("markout_horizons_s: delays must be in (0, 600] seconds")
    g = cfg.guards
    if g.slippage_pause_scope not in ("all", "symbol"):
        errs.append("guards.slippage_pause_scope must be all or symbol")
    if g.backoff_base_s <= 0 or g.backoff_max_s < g.backoff_base_s:
        errs.append("guards: need 0 < backoff_base_s <= backoff_max_s")
    if g.api_failure_count < 1 or g.slippage_burst_count < 0:
        errs.append("guards: api_failure_count >= 1, slippage_burst_count >= 0")
    a = cfg.auto_symbols
    if a.trade not in ("none", "filtered", "ranked"):
        errs.append("auto_symbols.trade must be none|filtered|ranked")
    if a.min_venues < 2 or a.ranked_top_n < 1 or (a.max_symbols is not None and a.max_symbols < 0):
        errs.append("auto_symbols: min_venues >= 2, ranked_top_n >= 1, max_symbols >= 0")
    if set(a.categories) - {"us_equity", "crypto", "other"} or set(a.templates) - {"us_equity", "crypto", "other"}:
        errs.append("auto_symbols.categories/templates: keys must be us_equity|crypto|other")
    if cfg.ui.sort not in ("gap", "config"):
        errs.append("ui.sort must be gap|config")
    if not (1 <= cfg.max_parallel_slices <= 32):
        errs.append("trading.max_parallel_slices must be 1..32")
    for v in cfg.venues.values():
        if v.liveness_s <= 0 or v.quick_resume_s < 0:
            errs.append(f"venues.{v.name}: liveness_s > 0, quick_resume_s >= 0")
    if len(cfg.enabled_venues()) > 2:
        errs.append(f"at most two venues may be enabled for trading, got {cfg.enabled_venues()}")
    for s in cfg.symbols.values():
        st, b, ex, rk = s.strategy, s.basis, s.execution, s.risk
        p = f"symbols.{s.key}"
        if st.initial_threshold_pct <= 0 or st.grid_step_pct <= 0:
            errs.append(f"{p}: initial_threshold_pct and grid_step_pct must be > 0")
        if not (0 <= st.first_close_ratio < 1):
            errs.append(f"{p}: first_close_ratio must be in [0, 1)")
        if st.max_grids < 1:
            errs.append(f"{p}: max_grids must be >= 1")
        if st.order_value_usdc <= 0 or st.grid_value_usdc <= 0 or st.order_value_usdc > st.grid_value_usdc:
            errs.append(f"{p}: need 0 < order_value_usdc <= grid_value_usdc")
        if st.session not in ("24x7", "us_equity"):
            errs.append(f"{p}: session must be 24x7 or us_equity")
        if st.offhours_threshold_pct is not None and st.offhours_threshold_pct <= 0:
            errs.append(f"{p}: offhours_threshold_pct must be > 0")
        if st.signal_confirm_s < 0:
            errs.append(f"{p}.strategy.signal_confirm_s must be >= 0")
        if st.funding_carry and not (0 < st.carry_exit_hourly_pct <= st.carry_min_hourly_pct and st.carry_credit_hours >= 0
                                     and st.carry_min_open_pct >= 0
                                     and (st.carry_max_hold_hours is None or st.carry_max_hold_hours > 0)):
            errs.append(f"{p}.strategy: funding carry needs 0 < carry_exit_hourly_pct <= carry_min_hourly_pct, "
                        f"carry_credit_hours >= 0, carry_min_open_pct >= 0, carry_max_hold_hours > 0 or null")
        if rk.volatility_max_pct is not None and (rk.volatility_max_pct <= 0 or rk.volatility_window_s <= 0 or rk.volatility_block_s < 0):
            errs.append(f"{p}.risk: volatility_max_pct/volatility_window_s must be > 0")
        if rk.max_leg_spread_pct is not None and rk.max_leg_spread_pct <= 0:
            errs.append(f"{p}.risk.max_leg_spread_pct must be > 0 or null")
        if rk.max_daily_opens is not None and rk.max_daily_opens < 1:
            errs.append(f"{p}.risk.max_daily_opens must be >= 1 or null")
        if st.grid_value_usdc * st.max_grids > rk.max_symbol_value_usdc + 1e-9:
            errs.append(f"{p}: grid_value_usdc*max_grids exceeds risk.max_symbol_value_usdc")
        if b.enabled:
            if min(b.ema_seconds, b.warmup_seconds, b.drift_seconds, b.max_gap_seconds, b.recovery_seconds, b.sample_interval_seconds) <= 0:
                errs.append(f"{p}.basis: all durations must be > 0")
            if not (b.max_gap_seconds < b.recovery_seconds <= b.reset_after_seconds):
                errs.append(f"{p}.basis: need max_gap < recovery <= reset_after")
            if b.warmup_seconds < b.drift_seconds:
                errs.append(f"{p}.basis: warmup_seconds must be >= drift_seconds")
        if ex.maker_price_mode not in ("join", "improve", "threshold"):
            errs.append(f"{p}.execution.maker_price_mode must be join|improve|threshold")
        if ex.requote_threshold_pct < 0 or ex.max_order_age_s <= 0 or ex.max_requotes_per_s <= 0:
            errs.append(f"{p}.execution: requote_threshold_pct>=0, max_order_age_s>0, max_requotes_per_s>0")
        if ex.hedge_max_orders < ex.hedge_max_attempts or ex.hedge_min_notional_usdc < 0:
            errs.append(f"{p}.execution: hedge_max_orders >= hedge_max_attempts, hedge_min_notional_usdc >= 0")
        if st.min_close_profit_pct is not None and st.close_floor_timeout_s < 0:
            errs.append(f"{p}.strategy: close_floor_timeout_s must be >= 0")
        if rk.dust_max_usdc < 0 or rk.dust_grace_s < 0 or rk.skip_log_s < 0 or rk.dust_residual_grace_s < 0:
            errs.append(f"{p}.risk: dust_max_usdc, dust_grace_s, dust_residual_grace_s and skip_log_s must be >= 0")
        if rk.warmup_fresh_s < 0 or rk.quick_resume_s < 0 or rk.max_leverage <= 0 or not (0 <= rk.reserve_ratio < 1):
            errs.append(f"{p}.risk: warmup_fresh_s>=0, quick_resume_s>=0, max_leverage>0, 0<=reserve_ratio<1")
        if ex.close_mode not in ("maker_taker", "taker_taker"):
            errs.append(f"{p}.execution.close_mode must be maker_taker|taker_taker")
        if ex.open_mode not in ("maker_taker", "taker_taker"):
            errs.append(f"{p}.execution.open_mode must be maker_taker|taker_taker")
        if ex.maker_timeout_s <= 0 or ex.hedge_max_attempts < 1 or ex.hedge_slippage_pct <= 0:
            errs.append(f"{p}.execution: maker_timeout_s>0, hedge_max_attempts>=1, hedge_slippage_pct>0")
        if ex.hedge_slippage_pct + ex.hedge_slippage_step_pct * (ex.hedge_max_attempts - 1) > 2.0:
            errs.append(f"{p}.execution: worst-case hedge slippage above 2% is not allowed")
        if rk.depth_levels < 1 or rk.depth_band_pct <= 0 or rk.max_quote_age_s <= 0:
            errs.append(f"{p}.risk: depth_levels>=1, depth_band_pct>0, max_quote_age_s>0")
    return errs


# ---------------------------------------------------------------- sessions
_NY = ZoneInfo("America/New_York")
US_HOLIDAYS = {
    # NYSE full-day closures (extend yearly)
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25", "2026-06-19", "2026-07-03",
    "2026-09-07", "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31", "2027-06-18", "2027-07-05",
    "2027-09-06", "2027-11-25", "2027-12-24",
}
US_EARLY_CLOSES = {"2026-11-27": dtime(13, 0), "2026-12-24": dtime(13, 0), "2027-11-26": dtime(13, 0)}
US_CALENDAR_YEARS = {2026, 2027}  # other years: treated as off-hours (higher threshold) until the calendar is extended


def us_regular_hours(ts: Optional[float] = None) -> bool:
    now = datetime.now(_NY) if ts is None else datetime.fromtimestamp(ts, _NY)
    day = now.strftime("%Y-%m-%d")
    if now.year not in US_CALENDAR_YEARS or now.weekday() >= 5 or day in US_HOLIDAYS:
        return False
    return dtime(9, 30) <= now.time() < US_EARLY_CLOSES.get(day, dtime(16, 0))


def effective_t1(st: StrategyConfig, ts: Optional[float] = None) -> float:
    if st.session == "us_equity" and st.offhours_threshold_pct is not None and not us_regular_hours(ts):
        return st.offhours_threshold_pct
    return st.initial_threshold_pct


def tune_bucket(st: StrategyConfig, ts: Optional[float] = None) -> str:
    """v5.6.1: which threshold is in effect: all (one T1) | rth / off (us_equity with offhours_threshold_pct)."""
    if st.session == "us_equity" and st.offhours_threshold_pct is not None:
        return "rth" if us_regular_hours(ts) else "off"
    return "all"


def as_dict(obj: Any) -> Any:
    from dataclasses import asdict, is_dataclass
    return asdict(obj) if is_dataclass(obj) else obj
