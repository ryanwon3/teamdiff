from __future__ import annotations

import time

from app.config import Config
from app.riot.client import RiotClient
from app.services.ladder_seeds import ladder_seed_puuids

_CACHE_TTL_SEC = 3600.0
_cache_at: float | None = None
_cached: list[str] | None = None


def _dedupe_puuids(ids: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for p in ids:
        p = p.strip()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def invalidate_ladder_seed_cache() -> None:
    """Clear cached ladder merge (e.g. collector periodic refresh)."""
    global _cache_at, _cached
    _cache_at = None
    _cached = None


def resolve_matchup_seed_puuids(
    client: RiotClient | None, *, force_refresh: bool = False
) -> list[str]:
    """
    File/env seeds plus optional League ladder seeds (cached briefly to avoid
    refetching ladders on every HTTP request).
    """
    global _cache_at, _cached

    if force_refresh:
        invalidate_ladder_seed_cache()

    base = list(Config.MATCHUP_SEED_PUUIDS)
    if client is None or not Config.MATCHUP_LADDER_SEEDS:
        return _dedupe_puuids(base)

    now = time.monotonic()
    if (
        _cached is not None
        and _cache_at is not None
        and now - _cache_at < _CACHE_TTL_SEC
    ):
        return list(_cached)

    merged = _dedupe_puuids(base + ladder_seed_puuids(client))
    _cache_at = now
    _cached = merged
    return list(merged)
