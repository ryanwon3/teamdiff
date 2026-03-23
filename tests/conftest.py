from __future__ import annotations

import sqlite3

import pytest

from app import create_app
from app.db.store import init_schema


@pytest.fixture()
def temp_db_path(tmp_path):
    path = tmp_path / "test_matchups.db"
    init_schema(str(path))
    return str(path)


@pytest.fixture()
def app_with_db(temp_db_path):
    app = create_app()
    app.config.update(
        TESTING=True,
        MATCHUP_DB_PATH=temp_db_path,
    )
    return app


@pytest.fixture()
def client(app_with_db):
    return app_with_db.test_client()


def seed_lane_gold_fixture(db_path: str) -> None:
    """One ranked match: two TOP laners with timeline samples for gold APIs."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """
            INSERT INTO matches (match_id, queue_id, game_version, ingested_at)
            VALUES ('NA1_TEST_LANE', 420, '14.1', '2024-06-15 16:00:00')
            """
        )
        conn.executemany(
            """
            INSERT INTO participants (
                match_id, puuid, champion_id, team_id, win,
                participant_id, team_position
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("NA1_TEST_LANE", "p1", 157, 100, 1, 1, "TOP"),
                ("NA1_TEST_LANE", "p2", 238, 200, 0, 2, "TOP"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO participant_timeline (
                match_id, participant_id, minute, total_gold, level
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("NA1_TEST_LANE", 1, 10, 4000, 7),
                ("NA1_TEST_LANE", 2, 10, 4200, 7),
                ("NA1_TEST_LANE", 1, 15, 5200, 9),
                ("NA1_TEST_LANE", 2, 15, 5000, 9),
            ],
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def client_with_lane_data(temp_db_path, app_with_db):
    seed_lane_gold_fixture(temp_db_path)
    return app_with_db.test_client()
