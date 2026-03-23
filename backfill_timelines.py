#!/usr/bin/env python3
"""
One-shot worker: fetch Match-V5 timelines for matches already in SQLite that have
no participant_timeline rows (e.g. ingested before the collector wrote timelines).

Uses the same .env as collect_matches (RIOT_API_KEY, RIOT_REGIONAL_ROUTE, MATCHUP_DB_PATH).
"""

from __future__ import annotations

import argparse
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
    match_ids_missing_timeline,
)
from app.riot.client import RiotAPIError, RiotClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill participant_timeline for existing matches.")
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max matches to process (default 200)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.25,
        help="Seconds between successful timeline requests (default 1.25)",
    )
    args = parser.parse_args()

    key = (os.environ.get("RIOT_API_KEY") or "").strip()
    if not key:
        raise SystemExit("RIOT_API_KEY is required.")

    db_path = (os.environ.get("MATCHUP_DB_PATH") or Config.MATCHUP_DB_PATH or "").strip()
    if not db_path or not os.path.isfile(db_path):
        raise SystemExit(f"MATCHUP_DB_PATH must point to an existing SQLite file (got {db_path!r}).")

    init_schema(db_path)
    missing = match_ids_missing_timeline(db_path, limit=args.limit)
    if not missing:
        print("No matches need timeline backfill.")
        return

    client = RiotClient(key, Config.RIOT_REGIONAL_ROUTE, None)
    print(f"Backfilling up to {len(missing)} matches in {db_path}…")

    ok = 0
    failed = 0
    for i, mid in enumerate(missing, start=1):
        timeline = None
        for attempt in range(3):
            try:
                timeline = client.match_timeline_by_id(mid)
                break
            except RiotAPIError as e:
                if e.status_code == 429 and attempt < 2:
                    print(f"  {mid} 429, sleep 60s…")
                    time.sleep(60)
                    continue
                print(f"  {mid} error {e.status_code}: {(e.message or '')[:120]}")
                failed += 1
                time.sleep(60 if e.status_code == 429 else args.sleep)
                break
        if timeline is None:
            continue
        n = ingest_match_timeline(db_path, mid, timeline)
        if n:
            ok += 1
            print(f"[{i}/{len(missing)}] {mid} → {n} timeline rows")
        else:
            failed += 1
            print(
                f"[{i}/{len(missing)}] {mid} → 0 rows "
                f"(frames={count_timeline_frames(timeline)})"
            )
        time.sleep(max(0.1, args.sleep))

    print(f"Done. Stored rows for {ok} matches; {failed} failures or empty parses.")


if __name__ == "__main__":
    main()
