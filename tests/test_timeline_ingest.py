"""Tests for Match-V5 timeline parsing and SQLite ingest."""
from __future__ import annotations

import os
import tempfile
import unittest

from app.db.store import (
    count_timeline_frames,
    ingest_match_timeline,
    init_schema,
    insert_match_if_new,
    match_exists,
    match_ids_missing_timeline,
)
from app.riot.client import RiotAPIError, RiotClient


def _minimal_match_payload(match_id: str) -> dict:
    return {
        "metadata": {"matchId": match_id},
        "info": {
            "queueId": 420,
            "gameVersion": "14.1",
            "participants": [
                {
                    "participantId": 1,
                    "teamId": 100,
                    "championId": 61,
                    "win": True,
                    "puuid": "a",
                    "teamPosition": "MIDDLE",
                },
                {
                    "participantId": 2,
                    "teamId": 200,
                    "championId": 238,
                    "win": False,
                    "puuid": "b",
                    "teamPosition": "MIDDLE",
                },
            ],
        },
    }


def _riot_like_timeline() -> dict:
    """Shape matches Riot Match-V5 timeline (object keyed participantFrames)."""
    return {
        "metadata": {"matchId": "NA1_UNITTEST"},
        "info": {
            "frames": [
                {
                    "timestamp": 0,
                    "participantFrames": {
                        "1": {"participantId": 1, "totalGold": 500, "level": 1},
                        "2": {"participantId": 2, "totalGold": 480, "level": 1},
                    },
                },
                {
                    "timestamp": 60000,
                    "participantFrames": {
                        "1": {"participantId": 1, "totalGold": 650, "level": 2},
                        "2": {"participantId": 2, "totalGold": 620, "level": 2},
                    },
                },
            ]
        },
    }


def _timeline_list_style_frames() -> dict:
    """Defensive: participantFrames as a list with participantId on each entry."""
    return {
        "metadata": {"matchId": "NA1_LISTPF"},
        "info": {
            "frames": [
                {
                    "timestamp": 0,
                    "participantFrames": [
                        {"participantId": 1, "totalGold": 500, "level": 1},
                        {"participantId": 2, "totalGold": 490, "level": 1},
                    ],
                }
            ]
        },
    }


def _timeline_frames_at_root() -> dict:
    return {
        "frames": [
            {
                "timestamp": 120000,
                "participantFrames": {
                    "1": {"totalGold": 700, "level": 3},
                    "2": {"totalGold": 690, "level": 3},
                },
            }
        ]
    }


class TestInsertMatchIdPrefix(unittest.TestCase):
    """Optional shard filter on insert_match_if_new."""

    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._dir.name, "m.db")

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_prefix_rejects_non_matching_match_id(self) -> None:
        init_schema(self.db_path)
        m = _minimal_match_payload("EUW1_not_na")
        self.assertFalse(
            insert_match_if_new(self.db_path, m, match_id_prefix="NA1_")
        )
        self.assertFalse(match_exists(self.db_path, "EUW1_not_na"))


class TestTimelineIngest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self._tmp.close()
        self.db_path = self._tmp.name

    def tearDown(self) -> None:
        try:
            os.unlink(self.db_path)
        except OSError:
            pass

    def test_ingest_dict_participant_frames(self) -> None:
        init_schema(self.db_path)
        insert_match_if_new(self.db_path, _minimal_match_payload("NA1_UNITTEST"))
        self.assertGreater(
            ingest_match_timeline(self.db_path, "NA1_UNITTEST", _riot_like_timeline()),
            0,
        )

    def test_ingest_list_participant_frames(self) -> None:
        init_schema(self.db_path)
        insert_match_if_new(self.db_path, _minimal_match_payload("NA1_LISTPF"))
        self.assertGreater(
            ingest_match_timeline(self.db_path, "NA1_LISTPF", _timeline_list_style_frames()),
            0,
        )

    def test_frames_at_root_fallback(self) -> None:
        init_schema(self.db_path)
        insert_match_if_new(self.db_path, _minimal_match_payload("NA1_ROOT"))
        n = ingest_match_timeline(self.db_path, "NA1_ROOT", _timeline_frames_at_root())
        self.assertGreater(n, 0)

    def test_count_timeline_frames(self) -> None:
        self.assertEqual(count_timeline_frames(_riot_like_timeline()), 2)
        self.assertEqual(count_timeline_frames({}), 0)

    def test_skips_when_total_gold_missing(self) -> None:
        init_schema(self.db_path)
        insert_match_if_new(self.db_path, _minimal_match_payload("NA1_BAD"))
        bad = {
            "info": {
                "frames": [
                    {
                        "timestamp": 0,
                        "participantFrames": {
                            "1": {"level": 1},
                        },
                    }
                ]
            }
        }
        self.assertEqual(ingest_match_timeline(self.db_path, "NA1_BAD", bad), 0)

    def test_match_ids_missing_timeline(self) -> None:
        init_schema(self.db_path)
        insert_match_if_new(self.db_path, _minimal_match_payload("NA1_A"))
        insert_match_if_new(self.db_path, _minimal_match_payload("NA1_B"))
        tl = _riot_like_timeline()
        tl["metadata"] = {"matchId": "NA1_A"}
        ingest_match_timeline(self.db_path, "NA1_A", tl)
        missing = match_ids_missing_timeline(self.db_path, limit=10)
        self.assertIn("NA1_B", missing)
        self.assertNotIn("NA1_A", missing)


class TestRiotClientTimeline(unittest.TestCase):
    def test_empty_body_raises(self) -> None:
        from unittest.mock import patch

        class FakeResp:
            status_code = 200
            text = ""
            content = b""

            def json(self):
                return {}

        with patch("app.riot.client.requests.get", return_value=FakeResp()):
            client = RiotClient("k", "americas", None)
            with self.assertRaises(RiotAPIError) as ctx:
                client.match_timeline_by_id("NA1_123")
            self.assertEqual(ctx.exception.status_code, 0)

    def test_list_json_raises(self) -> None:
        from unittest.mock import patch

        class FakeResp:
            status_code = 200
            text = "[]"
            content = b"[]"

            def json(self):
                return []

        with patch("app.riot.client.requests.get", return_value=FakeResp()):
            client = RiotClient("k", "americas", None)
            with self.assertRaises(RiotAPIError) as ctx:
                client.match_timeline_by_id("NA1_123")
            self.assertIn("expected object", ctx.exception.message)


if __name__ == "__main__":
    unittest.main()
