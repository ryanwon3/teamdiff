#!/usr/bin/env python3
"""
Long-running worker: pull matchlists from seed PUUIDs, fetch new match details,
timelines, and append to MATCHUP_DB_PATH (SQLite).
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from dotenv import load_dotenv

_repo_root = Path(__file__).resolve().parent
_env_file = _repo_root / ".env"
if _env_file.is_file():
    load_dotenv(_env_file, override=True)

from app.config import Config
from app.db.store import (
    count_timeline_frames,
    init_schema,
    ingest_match_timeline,
    insert_match_if_new,
    match_exists,
    match_needs_participant_meta_backfill,
    merge_participant_meta_from_match,
)
from app.riot.client import RiotAPIError, RiotClient
from app.services.seed_puuids import resolve_matchup_seed_puuids


def main() -> None:
    if _env_file.is_file():
        load_dotenv(_env_file, override=True)

    key = (os.environ.get("RIOT_API_KEY") or "").strip()
    if not key:
        raise SystemExit(
            "RIOT_API_KEY is required for collect_matches.py. "
            f"Set it in {_env_file} (save the file), or export RIOT_API_KEY in the shell."
        )

    db_path = (os.environ.get("MATCHUP_DB_PATH") or Config.MATCHUP_DB_PATH or "").strip()
    if not db_path:
        raise SystemExit(
            "MATCHUP_DB_PATH must be set in .env at the project root "
            f"(expected {_env_file}) — e.g. MATCHUP_DB_PATH=data/matchups.db"
        )

    if Config.MATCHUP_LADDER_SEEDS and not (Config.RIOT_PLATFORM_ROUTE or "").strip():
        raise SystemExit(
            "MATCHUP_LADDER_SEEDS=1 requires RIOT_PLATFORM_ROUTE (e.g. na1)."
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
    api_spacing_s = min(2.0, max(0.5, sleep_s / 15))

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
                    if match_needs_participant_meta_backfill(db_path, mid):
                        try:
                            refill = client.match_by_id(mid)
                        except RiotAPIError as e:
                            print(f"meta backfill {mid} error {e.status_code}")
                            time.sleep(60 if e.status_code == 429 else sleep_s)
                        else:
                            n = merge_participant_meta_from_match(db_path, refill)
                            if n:
                                print(f"  participant lane/id backfill {mid} ({n} rows)")
                            time.sleep(api_spacing_s)
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
                    timeline = None
                    for attempt in range(3):
                        try:
                            timeline = client.match_timeline_by_id(mid)
                            break
                        except RiotAPIError as e:
                            if e.status_code == 429 and attempt < 2:
                                print(f"  timeline {mid} rate limited (429), waiting 60s…")
                                time.sleep(60)
                                continue
                            print(
                                f"  timeline {mid} error {e.status_code}: "
                                f"{(e.message or '')[:160]}"
                            )
                            time.sleep(60 if e.status_code == 429 else sleep_s)
                            break
                    if timeline is not None:
                        n = ingest_match_timeline(db_path, mid, timeline)
                        if n:
                            print(f"  timeline rows {n}")
                        else:
                            fc = count_timeline_frames(timeline)
                            print(
                                f"  timeline warning: stored 0 rows for {mid} "
                                f"(frame_count={fc})"
                            )

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
