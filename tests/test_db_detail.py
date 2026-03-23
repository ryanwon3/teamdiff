"""DB match detail and EST ingest formatting."""
from __future__ import annotations

import os
import tempfile
import unittest

from app import create_app
from app.db.store import fetch_match_detail, init_schema, insert_match_if_new
from app.services.ingested_time import ingested_at_est_display


def _sample_match(mid: str) -> dict:
    return {
        "metadata": {"matchId": mid},
        "info": {
            "queueId": 420,
            "gameVersion": "14.1",
            "participants": [
                {
                    "participantId": 1,
                    "teamId": 100,
                    "championId": 61,
                    "win": True,
                    "puuid": "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789AbCd",
                    "teamPosition": "MIDDLE",
                },
                {
                    "participantId": 2,
                    "teamId": 200,
                    "championId": 238,
                    "win": False,
                    "puuid": "ZzYyXxWwVvUuTtSsRrQqPpOoNnMmLlKkJjIiHhGgFf",
                    "teamPosition": "MIDDLE",
                },
            ],
        },
    }


class TestIngestedAtEst(unittest.TestCase):
    def test_winter_est(self) -> None:
        # 18:00 UTC in January → 1 PM EST
        s = ingested_at_est_display("2025-01-15 18:00:00")
        self.assertIsNotNone(s)
        self.assertIn("01:00:00", s)  # 1 PM 12h
        self.assertIn("EST", s)

    def test_summer_edt(self) -> None:
        s = ingested_at_est_display("2025-06-15 18:00:00")
        self.assertIsNotNone(s)
        self.assertIn("EDT", s)

    def test_invalid_returns_none(self) -> None:
        self.assertIsNone(ingested_at_est_display(None))
        self.assertIsNone(ingested_at_est_display("not-a-date"))


class TestFetchMatchDetail(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self._tmp.close()
        self.db_path = self._tmp.name
        init_schema(self.db_path)
        insert_match_if_new(self.db_path, _sample_match("NA1_DETAIL_TEST"))

    def tearDown(self) -> None:
        try:
            os.unlink(self.db_path)
        except OSError:
            pass

    def test_fetch_returns_participants(self) -> None:
        d = fetch_match_detail(self.db_path, "NA1_DETAIL_TEST")
        self.assertIsNotNone(d)
        assert d is not None
        self.assertEqual(d["match"]["queue_id"], 420)
        self.assertEqual(len(d["participants"]), 2)
        ids = {p["champion_id"] for p in d["participants"]}
        self.assertEqual(ids, {61, 238})

    def test_missing_match(self) -> None:
        self.assertIsNone(fetch_match_detail(self.db_path, "NA1_NOPE"))


class TestMatchDetailRoute(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self._tmp.close()
        self.db_path = self._tmp.name
        init_schema(self.db_path)
        insert_match_if_new(self.db_path, _sample_match("NA1_ROUTE_TEST"))
        self.app = create_app()
        self.app.config["MATCHUP_DB_PATH"] = self.db_path
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        try:
            os.unlink(self.db_path)
        except OSError:
            pass

    def test_list_includes_est(self) -> None:
        r = self.client.get("/api/db/matches?limit=5&offset=0")
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        self.assertIn("matches", data)
        self.assertGreaterEqual(len(data["matches"]), 1)
        row = next(m for m in data["matches"] if m["match_id"] == "NA1_ROUTE_TEST")
        self.assertIn("ingested_at_est", row)

    def test_detail_ok(self) -> None:
        r = self.client.get("/api/db/matches/NA1_ROUTE_TEST")
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        self.assertIn("participants", data)
        self.assertEqual(len(data["participants"]), 2)
        for p in data["participants"]:
            self.assertNotIn("puuid", p)
            self.assertIn("puuid_masked", p)

    def test_detail_invalid_id(self) -> None:
        r = self.client.get("/api/db/matches/not!!!valid")
        self.assertEqual(r.status_code, 400)


if __name__ == "__main__":
    unittest.main()
