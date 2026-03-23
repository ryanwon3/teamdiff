from __future__ import annotations

import os
from typing import Any

from app.db.store import aggregate_matchup as db_aggregate_matchup
from app.riot.client import RiotClient


def _observations_from_participants(
    participants: list[dict[str, Any]],
    champion_a: int,
    champion_b: int,
) -> tuple[int, int]:
    """Single match: (wins_for_side_playing_A, games_where_A_and_B_on_opposite_teams)."""
    pas = [p for p in participants if p.get("championId") == champion_a]
    pbs = [p for p in participants if p.get("championId") == champion_b]
    wins_a = 0
    games = 0
    for pa in pas:
        for pb in pbs:
            if pa.get("teamId") != pb.get("teamId"):
                games += 1
                if pa.get("win"):
                    wins_a += 1
    return wins_a, games


def _dedupe_preserve_order(ids: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for mid in ids:
        if mid not in seen:
            seen.add(mid)
            out.append(mid)
    return out


def compute_matchup_stats(
    client: RiotClient,
    *,
    seed_puuids: list[str],
    champion_a: int,
    champion_b: int,
    max_match_fetches: int,
    queue_id: int | None,
    matchlist_count_per_seed: int = 20,
) -> dict[str, Any]:
    """
    Pull recent match IDs from seed accounts, fetch match details (capped), and aggregate
    head-to-head when A and B are on opposite teams.
    """
    if champion_a == champion_b:
        return {
            "error": "champion_a and champion_b must differ",
            "wins_a": 0,
            "games": 0,
            "winrate": None,
        }

    collected: list[str] = []
    for puuid in seed_puuids:
        ids = client.match_ids_by_puuid(
            puuid,
            start=0,
            count=matchlist_count_per_seed,
            queue=queue_id,
        )
        collected.extend(ids)

    unique_ids = _dedupe_preserve_order(collected)[:max_match_fetches]

    total_wins_a = 0
    total_games = 0

    for mid in unique_ids:
        match = client.match_by_id(mid)
        info = match.get("info") or {}
        participants = info.get("participants") or []
        if not isinstance(participants, list):
            continue
        if queue_id is not None and info.get("queueId") != queue_id:
            continue
        w, g = _observations_from_participants(
            participants, champion_a, champion_b
        )
        total_wins_a += w
        total_games += g

    winrate: float | None
    if total_games == 0:
        winrate = None
    else:
        winrate = round(total_wins_a / total_games, 4)

    sample_size_warning = total_games < 10

    return {
        "wins_a": total_wins_a,
        "games": total_games,
        "winrate": winrate,
        "match_detail_fetches": len(unique_ids),
        "match_ids_considered": len(unique_ids),
        "sample_size_warning": sample_size_warning,
        "source": "riot",
    }


def compute_matchup_stats_hybrid(
    client: RiotClient | None,
    *,
    db_path: str | None,
    seed_puuids: list[str],
    champion_a: int,
    champion_b: int,
    max_match_fetches: int,
    queue_id: int | None,
    matchlist_count_per_seed: int = 20,
    live_fallback: bool = True,
) -> dict[str, Any]:
    """
    Prefer SQLite aggregates when MATCHUP_DB_PATH points at a file and the DB returns at least
    one head-to-head observation; otherwise optionally fall back to live Riot aggregation.
    """
    path = (db_path or "").strip()
    if path and os.path.isfile(path):
        db_result = db_aggregate_matchup(
            path,
            champion_a=champion_a,
            champion_b=champion_b,
            queue_id=queue_id,
        )
        if db_result.get("error"):
            return db_result
        if db_result.get("games", 0) > 0:
            return db_result

    if not live_fallback:
        if not path or not os.path.isfile(path):
            return {
                "error": (
                    "MATCHUP_DB_PATH must exist when MATCHUP_LIVE_FALLBACK is disabled"
                ),
                "wins_a": 0,
                "games": 0,
                "winrate": None,
            }
        empty = db_aggregate_matchup(
            path,
            champion_a=champion_a,
            champion_b=champion_b,
            queue_id=queue_id,
        )
        if empty.get("error"):
            return empty
        return {
            "wins_a": empty["wins_a"],
            "games": empty["games"],
            "winrate": empty["winrate"],
            "match_detail_fetches": 0,
            "match_ids_considered": 0,
            "sample_size_warning": empty["sample_size_warning"],
            "source": "sqlite",
            "note": "No head-to-head pairs in DB for this matchup (live fallback disabled).",
        }

    if client is None:
        return {
            "error": "RIOT_API_KEY is required when the DB is empty or not configured",
            "wins_a": 0,
            "games": 0,
            "winrate": None,
        }

    return compute_matchup_stats(
        client,
        seed_puuids=seed_puuids,
        champion_a=champion_a,
        champion_b=champion_b,
        max_match_fetches=max_match_fetches,
        queue_id=queue_id,
        matchlist_count_per_seed=matchlist_count_per_seed,
    )
