import pytest

from arb.config import ConfigError, Pair, parse_config


def raw(**venues):
    return {"venues": {k: {"enabled": v} for k, v in venues.items()},
            "symbols": {"OPENAI": {"venues": {"lighter": "OPENAI", "lighter_rh": "OPENAI", "entropy": "io:OAI"}},
                        "BTC": {"venues": {"lighter": "BTC", "lighter_rh": "BTC"}}}}


def test_default_roles_and_active_pair():
    cfg = parse_config(raw(lighter=True, entropy=True))
    p = cfg.active_pair()
    assert p == Pair("lighter", "entropy", "entropy") and p.taker == "lighter"
    assert [s.key for s in cfg.symbols_for_pair(p)] == ["OPENAI"]
    cfg = parse_config(raw(lighter=True, lighter_rh=True))
    assert cfg.active_pair().maker == "lighter_rh"
    assert {s.key for s in cfg.symbols_for_pair(cfg.active_pair())} == {"OPENAI", "BTC"}


def test_exactly_two_rule():
    with pytest.raises(ConfigError, match="at most two"):
        parse_config(raw(lighter=True, lighter_rh=True, entropy=True))
    cfg = parse_config(raw(lighter=True))  # monitor-only config is valid
    with pytest.raises(ConfigError, match="exactly two"):
        cfg.active_pair()


def test_watched_pairs_all_three():
    cfg = parse_config(raw(lighter=True, lighter_rh=True))
    assert [p.key for p in cfg.watched_pairs()] == ["lighter+lighter_rh", "lighter+entropy", "lighter_rh+entropy"]


def test_roles_override_and_validation():
    r = raw(lighter=True, entropy=True)
    r["roles"] = {"entropy+lighter": "lighter"}
    assert parse_config(r).active_pair().maker == "lighter"
    r["roles"] = {"lighter+entropy": "lighter_rh"}
    with pytest.raises(ConfigError):
        parse_config(r)


def test_bad_values_rejected():
    r = raw(lighter=True, lighter_rh=True)
    r["defaults"] = {"strategy": {"order_value_usdc": 500, "grid_value_usdc": 100}}
    with pytest.raises(ConfigError, match="order_value_usdc"):
        parse_config(r)
    r = raw(lighter=True)
    r["bogus"] = 1
    with pytest.raises(ConfigError):
        parse_config(r)


def test_entropy_requires_fees():
    r = raw(lighter=True, entropy=True)
    r["venues"]["entropy"]["maker_fee_pct"] = None
    with pytest.raises(ConfigError, match="required"):
        parse_config(r)
