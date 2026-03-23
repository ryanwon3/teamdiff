from __future__ import annotations

from pathlib import Path

from app.config import Config
from app.riot.client import RiotClient

_TIER_PATH = {
    "CHALLENGER": "/lol/league/v4/challengerleagues/by-queue/{queue}",
    "GRANDMASTER": "/lol/league/v4/grandmasterleagues/by-queue/{queue}",
    "MASTER": "/lol/league/v4/masterleagues/by-queue/{queue}",
}

# Riot League-V4 master league pages are up to this many entries per page.
_MASTER_PAGE_SIZE = 200


def _read_master_cursor(path: Path) -> int:
    try:
        return max(0, int(path.read_text(encoding="utf-8").strip()))
    except (OSError, ValueError):
        return 0


def _write_master_cursor(path: Path, page: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(max(0, page)), encoding="utf-8")


def _puuid_from_league_entry(
    ent: dict,
    client: RiotClient,
    seen: set[str],
    out: list[str],
) -> None:
    raw_pu = ent.get("puuid")
    if isinstance(raw_pu, str) and raw_pu.strip():
        pu = raw_pu.strip()
        if pu not in seen:
            seen.add(pu)
            out.append(pu)
        return

    sid = ent.get("summonerId")
    if not isinstance(sid, str) or not sid:
        return
    summ = client.summoner_by_encrypted_id(sid)
    if not isinstance(summ, dict):
        return
    puuid = summ.get("puuid")
    if isinstance(puuid, str) and puuid and puuid not in seen:
        seen.add(puuid)
        out.append(puuid)


def _entries_for_fixed_tier(
    entries: list,
    cap: int,
) -> list:
    if cap <= 0:
        return list(entries)
    return entries[:cap]


def _collect_master_pages(
    client: RiotClient,
    path: str,
    seen: set[str],
    out: list[str],
) -> None:
    max_pages = Config.MATCHUP_LADDER_MASTER_MAX_PAGES
    if max_pages <= 0:
        return

    cursor_path = Config.MATCHUP_LADDER_MASTER_CURSOR_PATH
    start_page = _read_master_cursor(cursor_path)
    page = start_page
    for _ in range(max_pages):
        data = client.platform_get(path, params={"page": page})
        if not isinstance(data, dict):
            _write_master_cursor(cursor_path, 0)
            return
        entries = data.get("entries")
        if not isinstance(entries, list):
            _write_master_cursor(cursor_path, 0)
            return
        for ent in entries:
            if isinstance(ent, dict):
                _puuid_from_league_entry(ent, client, seen, out)
        if len(entries) < _MASTER_PAGE_SIZE:
            _write_master_cursor(cursor_path, 0)
            return
        page += 1
    _write_master_cursor(cursor_path, page)


def ladder_seed_puuids(client: RiotClient) -> list[str]:
    """
    PUUIDs from configured League-V4 ladders (platform route, e.g. na1).
    Challenger/Grandmaster: all entries when MATCHUP_LADDER_MAX_PER_TIER is 0,
    else first N entries per tier.
    Master: paginated; MATCHUP_LADDER_MASTER_MAX_PAGES per refresh, cursor in
    MATCHUP_LADDER_MASTER_CURSOR_PATH.
    """
    if not Config.MATCHUP_LADDER_SEEDS:
        return []
    if not client.platform_enabled:
        return []

    queue = Config.MATCHUP_LEAGUE_QUEUE_TYPE
    cap = Config.MATCHUP_LADDER_MAX_PER_TIER
    out: list[str] = []
    seen: set[str] = set()

    for tier in Config.MATCHUP_LADDER_TIERS:
        path_tpl = _TIER_PATH.get(tier)
        if not path_tpl:
            continue
        path = path_tpl.format(queue=queue)

        if tier == "MASTER":
            _collect_master_pages(client, path, seen, out)
            continue

        data = client.platform_get(path)
        if not isinstance(data, dict):
            continue
        entries = data.get("entries")
        if not isinstance(entries, list):
            continue
        for ent in _entries_for_fixed_tier(entries, cap):
            if isinstance(ent, dict):
                _puuid_from_league_entry(ent, client, seen, out)
    return out
