from __future__ import annotations

import sqlite3

from app.db.store import (
    aggregate_matchup,
    fetch_db_summary,
    fetch_gold_curve,
    fetch_gold_leaders_at_15,
    init_schema,
)
from tests.conftest import seed_lane_gold_fixture


def test_aggregate_matchup_counts(tmp_path) -> None:
    db = tmp_path / "m.db"
    init_schema(str(db))
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        """
        INSERT INTO matches (match_id, queue_id, game_version, ingested_at)
        VALUES ('M1', 420, '14.1', '2024-01-01 00:00:00')
        """
    )
    conn.executemany(
        """
        INSERT INTO participants (
            match_id, puuid, champion_id, team_id, win, participant_id, team_position
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("M1", None, 10, 100, 1, 1, "MIDDLE"),
            ("M1", None, 20, 200, 0, 2, "MIDDLE"),
        ],
    )
    conn.commit()
    conn.close()

    r = aggregate_matchup(str(db), champion_a=10, champion_b=20, queue_id=420)
    assert r["games"] == 1
    assert r["wins_a"] == 1
    assert r["winrate"] == 1.0
    assert r["source"] == "sqlite"


def test_fetch_gold_leaders_and_curve(temp_db_path) -> None:
    seed_lane_gold_fixture(temp_db_path)

    leaders = fetch_gold_leaders_at_15(
        temp_db_path,
        champion_anchor=157,
        queue_id=420,
        min_games=1,
        lead_sort="asc",
    )
    assert leaders["lane_games"] >= 1
    assert len(leaders["leaders"]) == 1
    row = leaders["leaders"][0]
    assert row["opponent_id"] == 238
    assert row["games"] == 1
    assert row["avg_gold_lead_at_15"] == -200.0

    curve = fetch_gold_curve(
        temp_db_path,
        champion_a=157,
        champion_b=238,
        queue_id=420,
        mode="time",
    )
    assert not curve.get("error")
    assert curve.get("games_lane", 0) >= 1
    labels = curve.get("labels") or []
    assert "10" in labels and "15" in labels


def test_fetch_db_summary_gold_features_ready(temp_db_path) -> None:
    seed_lane_gold_fixture(temp_db_path)
    s = fetch_db_summary(temp_db_path)
    assert s is not None
    assert s["gold_features_ready"] is True
    assert s["matches_count"] == 1
    assert s["timeline_row_count"] == 4
