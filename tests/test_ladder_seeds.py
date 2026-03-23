"""Tests for League-V4 ladder seed resolution (caps, Master pagination, puuid on entry)."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.services.ladder_seeds import ladder_seed_puuids


def _long_puuid(suffix: str) -> str:
    base = "PUUID_" + suffix
    return base + "x" * max(0, 78 - len(base))


def _entry(summoner_key: int, *, with_puuid: bool = False) -> dict:
    d: dict = {"summonerId": f"enc_{summoner_key}"}
    if with_puuid:
        d["puuid"] = _long_puuid(str(summoner_key))
    return d


class TestLadderSeeds(unittest.TestCase):
    def test_challenger_cap_zero_uses_all_entries(self) -> None:
        entries = [_entry(i) for i in range(5)]
        client = MagicMock()
        client.platform_enabled = True
        client.platform_get.return_value = {"entries": entries}
        client.summoner_by_encrypted_id.side_effect = (
            lambda sid: {"puuid": _long_puuid(sid)}
        )

        with patch("app.services.ladder_seeds.Config") as cfg:
            cfg.MATCHUP_LADDER_SEEDS = True
            cfg.MATCHUP_LADDER_TIERS = ["CHALLENGER"]
            cfg.MATCHUP_LADDER_MAX_PER_TIER = 0
            cfg.MATCHUP_LEAGUE_QUEUE_TYPE = "RANKED_SOLO_5x5"
            cfg.MATCHUP_LADDER_MASTER_MAX_PAGES = 0
            cfg.MATCHUP_LADDER_MASTER_CURSOR_PATH = Path("/dev/null")

            out = ladder_seed_puuids(client)

        self.assertEqual(len(out), 5)
        self.assertEqual(client.summoner_by_encrypted_id.call_count, 5)

    def test_challenger_positive_cap_slices(self) -> None:
        entries = [_entry(i) for i in range(5)]
        client = MagicMock()
        client.platform_enabled = True
        client.platform_get.return_value = {"entries": entries}
        client.summoner_by_encrypted_id.side_effect = (
            lambda sid: {"puuid": _long_puuid(sid)}
        )

        with patch("app.services.ladder_seeds.Config") as cfg:
            cfg.MATCHUP_LADDER_SEEDS = True
            cfg.MATCHUP_LADDER_TIERS = ["CHALLENGER"]
            cfg.MATCHUP_LADDER_MAX_PER_TIER = 2
            cfg.MATCHUP_LEAGUE_QUEUE_TYPE = "RANKED_SOLO_5x5"
            cfg.MATCHUP_LADDER_MASTER_MAX_PAGES = 0
            cfg.MATCHUP_LADDER_MASTER_CURSOR_PATH = Path("/dev/null")

            out = ladder_seed_puuids(client)

        self.assertEqual(len(out), 2)
        self.assertEqual(client.summoner_by_encrypted_id.call_count, 2)

    def test_entry_puuid_skips_summoner_lookup(self) -> None:
        pu = _long_puuid("solo")
        entries = [{"summonerId": "enc_x", "puuid": pu}]
        client = MagicMock()
        client.platform_enabled = True
        client.platform_get.return_value = {"entries": entries}

        with patch("app.services.ladder_seeds.Config") as cfg:
            cfg.MATCHUP_LADDER_SEEDS = True
            cfg.MATCHUP_LADDER_TIERS = ["CHALLENGER"]
            cfg.MATCHUP_LADDER_MAX_PER_TIER = 0
            cfg.MATCHUP_LEAGUE_QUEUE_TYPE = "RANKED_SOLO_5x5"
            cfg.MATCHUP_LADDER_MASTER_MAX_PAGES = 0
            cfg.MATCHUP_LADDER_MASTER_CURSOR_PATH = Path("/dev/null")

            out = ladder_seed_puuids(client)

        self.assertEqual(out, [pu])
        client.summoner_by_encrypted_id.assert_not_called()

    def test_master_pagination_resets_cursor_on_short_page(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cursor = Path(td) / "cursor"
            cursor.write_text("0", encoding="utf-8")

            full = [_entry(i, with_puuid=True) for i in range(200)]
            tail = [_entry(200, with_puuid=True)]

            def platform_get(path: str, params: dict | None = None) -> dict:
                page = (params or {}).get("page", 0)
                if page == 0:
                    return {"entries": full}
                if page == 1:
                    return {"entries": tail}
                return {"entries": []}

            client = MagicMock()
            client.platform_enabled = True
            client.platform_get.side_effect = platform_get

            with patch("app.services.ladder_seeds.Config") as cfg:
                cfg.MATCHUP_LADDER_SEEDS = True
                cfg.MATCHUP_LADDER_TIERS = ["MASTER"]
                cfg.MATCHUP_LADDER_MAX_PER_TIER = 0
                cfg.MATCHUP_LEAGUE_QUEUE_TYPE = "RANKED_SOLO_5x5"
                cfg.MATCHUP_LADDER_MASTER_MAX_PAGES = 2
                cfg.MATCHUP_LADDER_MASTER_CURSOR_PATH = cursor

                out = ladder_seed_puuids(client)

            self.assertEqual(len(out), 201)
            self.assertEqual(int(cursor.read_text(encoding="utf-8")), 0)
            self.assertEqual(client.platform_get.call_count, 2)

    def test_master_cursor_advances_when_pages_full(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cursor = Path(td) / "cursor"
            cursor.write_text("0", encoding="utf-8")

            full0 = [_entry(i, with_puuid=True) for i in range(200)]
            full1 = [_entry(i + 200, with_puuid=True) for i in range(200)]

            def platform_get(path: str, params: dict | None = None) -> dict:
                page = (params or {}).get("page", 0)
                if page == 0:
                    return {"entries": full0}
                if page == 1:
                    return {"entries": full1}
                return {"entries": []}

            client = MagicMock()
            client.platform_enabled = True
            client.platform_get.side_effect = platform_get

            with patch("app.services.ladder_seeds.Config") as cfg:
                cfg.MATCHUP_LADDER_SEEDS = True
                cfg.MATCHUP_LADDER_TIERS = ["MASTER"]
                cfg.MATCHUP_LADDER_MAX_PER_TIER = 0
                cfg.MATCHUP_LEAGUE_QUEUE_TYPE = "RANKED_SOLO_5x5"
                cfg.MATCHUP_LADDER_MASTER_MAX_PAGES = 2
                cfg.MATCHUP_LADDER_MASTER_CURSOR_PATH = cursor

                out = ladder_seed_puuids(client)

            self.assertEqual(len(out), 400)
            self.assertEqual(int(cursor.read_text(encoding="utf-8")), 2)


if __name__ == "__main__":
    unittest.main()
