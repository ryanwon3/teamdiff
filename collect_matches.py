#!/usr/bin/env python3
"""
Long-running worker: pull matchlists from seed PUUIDs, fetch new match details,
and append to MATCHUP_DB_PATH (SQLite).

Run alongside the Flask app (separate terminal). Respects Riot rate limits with
simple sleeps on errors and between rounds.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from dotenv import load_dotenv

# Load repo-root .env before importing app.config so MATCHUP_DB_PATH (etc.) apply
# even when the shell cwd is not the project directory.
_repo_root = Path(__file__).resolve().parent
_env_file = _repo_root / ".env"
if _env_file.is_file():
    load_dotenv(_env_file, override=True)

from app.config import Config
from app.db.store import init_schema, insert_match_if_new, match_exists
from app.riot.client import RiotAPIError, RiotClient
from app.services.seed_puuids import resolve_matchup_seed_puuids


def main() -> None:
    # Re-load after imports: app.config may have run load_dotenv on a different tree
    # (PYTHONPATH) and overridden RIOT_API_KEY with an empty value from that .env.
    if _env_file.is_file():
        load_dotenv(_env_file, override=True)

    key = (os.environ.get("RIOT_API_KEY") or "").strip()
    if not key:
        raise SystemExit(
            "RIOT_API_KEY is required for collect_matches.py. "
            f"Set it in {_env_file} (save the file), or export RIOT_API_KEY in the shell. "
            "If you use PYTHONPATH, point it at this same project so app.config loads the same .env."
        )

    db_path = (os.environ.get("MATCHUP_DB_PATH") or Config.MATCHUP_DB_PATH or "").strip()
    if not db_path:
        raise SystemExit(
            "MATCHUP_DB_PATH must be set in .env at the project root "
            f"(expected {_env_file}) — e.g. MATCHUP_DB_PATH=data/matchups.db"
        )

    if Config.MATCHUP_LADDER_SEEDS and not (Config.RIOT_PLATFORM_ROUTE or "").strip():
        raise SystemExit(
            "MATCHUP_LADDER_SEEDS=1 requires RIOT_PLATFORM_ROUTE "
            "(e.g. na1 for North America)."
        )

    init_schema(db_path)
    plat = (Config.RIOT_PLATFORM_ROUTE or "").strip() or None
    client = RiotClient(key, Config.RIOT_REGIONAL_ROUTE, plat)
    seeds = resolve_matchup_seed_puuids(client)
    if not seeds:
        raise SystemExit(
            "No seed PUUIDs: set MATCHUP_SEED_PUUIDS / puuids.txt and/or enable "
            "MATCHUP_LADDER_SEEDS with RIOT_PLATFORM_ROUTE."
        )

    if Config.MATCHUP_LADDER_SEEDS and Config.MATCHUP_QUEUE_ID is None:
        print(
            "Warning: set MATCHUP_QUEUE_ID=420 to ingest only Ranked Solo games "
            "from these seeds' match histories."
        )

    queue_id = Config.MATCHUP_QUEUE_ID
    list_count = max(1, Config.COLLECTOR_MATCHLIST_COUNT)
    sleep_s = max(1.0, float(Config.COLLECTOR_SLEEP_SECONDS))

    # Match-V5 matchlist is paginated. Always using start=0 only ever sees the same newest
    # `list_count` games per seed; once they are all in the DB, nothing new appears until we
    # advance `start` (older history) or those accounts play new games.
    list_start_by_puuid: dict[str, int] = {}

    print(f"Collector writing to {db_path}; Ctrl+C to stop.")
    while True:
        for puuid in seeds:
            start = list_start_by_puuid.get(puuid, 0)
            try:
                ids = client.match_ids_by_puuid(
                    puuid,
                    start=start,
                    count=list_count,
                    queue=queue_id,
                )
            except RiotAPIError as e:
                print(f"matchlist error {e.status_code}: {e.message[:200]}")
                time.sleep(60 if e.status_code == 429 else sleep_s)
                continue

            had_new = False
            for mid in ids:
                if match_exists(db_path, mid):
                    continue
                try:
                    match = client.match_by_id(mid)
                except RiotAPIError as e:
                    print(f"match {mid} error {e.status_code}")
                    time.sleep(60 if e.status_code == 429 else sleep_s)
                    break

                if insert_match_if_new(db_path, match):
                    print(f"stored {mid}")
                    had_new = True

            if had_new:
                list_start_by_puuid[puuid] = 0
            elif not ids:
                list_start_by_puuid[puuid] = 0
            elif len(ids) < list_count:
                list_start_by_puuid[puuid] = 0
            else:
                list_start_by_puuid[puuid] = start + len(ids)

        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
