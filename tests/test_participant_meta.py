"""
Participant lane / Riot id must align with timeline keys for gold features.
Timeline without participant_id on rows is a valid DB state after legacy ingest.
"""
from __future__ import annotations

import os
import tempfile
import unittest

from app.db.store import (
    extract_participant_riot_fields,
    fetch_gold_leaders_at_15,
    init_schema,
    insert_match_if_new,
    ingest_match_timeline,
    match_ids_needing_participant_meta,
    match_needs_participant_meta_backfill,
    merge_participant_meta_from_match,
)


def _make_match(
    match_id: str,
    *,
    include_riot_meta: bool,
    anchor_cid: int,
    opp_cid: int,
) -> dict:
    def part(cid: int, tid: int, win: bool, slot: int) -> dict:
        d: dict = {
            "championId": cid,
            "teamId": tid,
            "win": win,
            "puuid": f"PUUID_{match_id}_{slot}",
        }
        if include_riot_meta:
            d["participantId"] = slot
            d["teamPosition"] = "MIDDLE"
        return d

    return {
        "metadata": {"matchId": match_id},
        "info": {
            "queueId": 420,
            "gameVersion": "test",
            "participants": [
                part(anchor_cid, 100, True, 1),
                part(opp_cid, 200, False, 2),
            ],
        },
    }


def _timeline_for_mid(match_id: str) -> dict:
    return {
        "metadata": {"matchId": match_id},
        "info": {
            "frames": [
                {
                    "timestamp": 600_000,
                    "participantFrames": {
                        "1": {"totalGold": 2500, "level": 6},
                        "2": {"totalGold": 2400, "level": 6},
                    },
                }
            ]
        },
    }


class TestExtractParticipantRiotFields(unittest.TestCase):
    def test_camel_case(self) -> None:
        pid, lane = extract_participant_riot_fields(
            {
                "participantId": 7,
                "teamPosition": "MIDDLE",
                "championId": 1,
            }
        )
        self.assertEqual(pid, 7)
        self.assertEqual(lane, "MIDDLE")

    def test_snake_case_alias(self) -> None:
        pid, lane = extract_participant_riot_fields(
            {
                "participant_id": 3,
                "team_position": "TOP",
            }
        )
        self.assertEqual(pid, 3)
        self.assertEqual(lane, "TOP")

    def test_individual_position_fallback(self) -> None:
        pid, lane = extract_participant_riot_fields(
            {"participantId": 1, "individualPosition": "UTILITY"}
        )
        self.assertEqual(lane, "UTILITY")


class TestMergeParticipantMeta(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self._tmp.close()
        self.db_path = self._tmp.name
        self.mid = "NA1_META_TEST"
        self.anchor = 91001
        self.opp = 92002

    def tearDown(self) -> None:
        try:
            os.unlink(self.db_path)
        except OSError:
            pass

    def test_timeline_without_meta_then_merge_enables_gold_query(self) -> None:
        init_schema(self.db_path)
        legacy = _make_match(self.mid, include_riot_meta=False, anchor_cid=self.anchor, opp_cid=self.opp)
        self.assertTrue(insert_match_if_new(self.db_path, legacy))

        before = fetch_gold_leaders_at_15(
            self.db_path, champion_anchor=self.anchor, queue_id=420, min_games=0
        )
        self.assertEqual(before["anchor_matches_with_lane_meta"], 0)

        ingest_match_timeline(self.db_path, self.mid, _timeline_for_mid(self.mid))
        self.assertTrue(match_needs_participant_meta_backfill(self.db_path, self.mid))
        self.assertIn(self.mid, match_ids_needing_participant_meta(self.db_path, limit=50))

        full = _make_match(self.mid, include_riot_meta=True, anchor_cid=self.anchor, opp_cid=self.opp)
        n = merge_participant_meta_from_match(self.db_path, full)
        self.assertEqual(n, 2)
        self.assertFalse(match_needs_participant_meta_backfill(self.db_path, self.mid))

        after = fetch_gold_leaders_at_15(
            self.db_path, champion_anchor=self.anchor, queue_id=420, min_games=0
        )
        self.assertEqual(after["anchor_matches_with_lane_meta"], 1)
        self.assertEqual(after["lane_games"], 1)
        self.assertEqual(after["games_with_gold_at_15"], 1)
        self.assertEqual(len(after["leaders"]), 1)
        self.assertEqual(after["leaders"][0]["opponent_id"], self.opp)

    def test_insert_with_full_payload_has_lane_meta_immediately(self) -> None:
        init_schema(self.db_path)
        mid2 = "NA1_FRESH"
        m = _make_match(mid2, include_riot_meta=True, anchor_cid=self.anchor, opp_cid=self.opp)
        self.assertTrue(insert_match_if_new(self.db_path, m))
        self.assertFalse(match_needs_participant_meta_backfill(self.db_path, mid2))
        ingest_match_timeline(self.db_path, mid2, _timeline_for_mid(mid2))
        out = fetch_gold_leaders_at_15(
            self.db_path, champion_anchor=self.anchor, queue_id=420, min_games=0
        )
        self.assertGreaterEqual(out["anchor_matches_with_lane_meta"], 1)


if __name__ == "__main__":
    unittest.main()
