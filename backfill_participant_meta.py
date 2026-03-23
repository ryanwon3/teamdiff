#!/usr/bin/env python3
"""
Fetch Match-V5 details and fill participants.participant_id + team_position for rows
that are missing them (e.g. ingested before those columns were populated).

Timeline rows can exist without this metadata; gold/lane features need both.

Uses .env like collect_matches (RIOT_API_KEY, RIOT_REGIONAL_ROUTE, MATCHUP_DB_PATH).
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
    init_schema,
    match_ids_needing_participant_meta,
    merge_participant_meta_from_match,
)
from app.riot.client import RiotAPIError, RiotClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill Riot participant id + lane on participant rows."
    )
    parser.add_argument("--limit", type=int, default=200, help="Max matches to process")
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.25,
        help="Seconds after each successful match fetch",
    )
    args = parser.parse_args()

    key = (os.environ.get("RIOT_API_KEY") or "").strip()
    if not key:
        raise SystemExit("RIOT_API_KEY is required.")

    db_path = (os.environ.get("MATCHUP_DB_PATH") or Config.MATCHUP_DB_PATH or "").strip()
    if not db_path or not os.path.isfile(db_path):
        raise SystemExit(f"MATCHUP_DB_PATH must point to an existing SQLite file (got {db_path!r}).")

    init_schema(db_path)
    mids = match_ids_needing_participant_meta(db_path, limit=args.limit)
    if not mids:
        print("No matches need participant lane/id backfill.")
        return

    client = RiotClient(key, Config.RIOT_REGIONAL_ROUTE, None)
    print(f"Backfilling up to {len(mids)} matches…")

    ok = 0
    for i, mid in enumerate(mids, start=1):
        try:
            match = client.match_by_id(mid)
        except RiotAPIError as e:
            print(f"  {mid} error {e.status_code}")
            time.sleep(60 if e.status_code == 429 else args.sleep)
            continue
        if not match:
            print(f"  {mid} empty response")
            time.sleep(args.sleep)
            continue
        n = merge_participant_meta_from_match(db_path, match)
        if n:
            ok += 1
            print(f"[{i}/{len(mids)}] {mid} → updated {n} participant row(s)")
        else:
            print(f"[{i}/{len(mids)}] {mid} → no rows updated (unexpected)")
        time.sleep(max(0.1, args.sleep))

    print(f"Done. Touched rows in {ok} matches.")


if __name__ == "__main__":
    main()
