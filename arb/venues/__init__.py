from arb.venues.base import Venue, VenueError


def build_venue(cfg, env=None) -> Venue:
    """Create the adapter for a VenueConfig. Imports lazily so `check` needs no SDKs."""
    if cfg.kind == "lighter":
        from arb.venues.lighter import LighterVenue
        return LighterVenue(cfg, env=env)
    if cfg.kind == "hyperliquid":
        from arb.venues.entropy import EntropyVenue
        return EntropyVenue(cfg, env=env)
    raise VenueError(f"unknown venue kind {cfg.kind}")


__all__ = ["Venue", "VenueError", "build_venue"]
