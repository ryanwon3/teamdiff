from __future__ import annotations

from app.config import Config
from app.riot.client import RiotClient

_TIER_PATH = {
    "CHALLENGER": "/lol/league/v4/challengerleagues/by-queue/{queue}",
    "GRANDMASTER": "/lol/league/v4/grandmasterleagues/by-queue/{queue}",
    "MASTER": "/lol/league/v4/masterleagues/by-queue/{queue}",
}


def ladder_seed_puuids(client: RiotClient) -> list[str]:
    """
    PUUIDs from configured League-V4 ladders (platform route, e.g. na1).
    """
    if not Config.MATCHUP_LADDER_SEEDS:
        return []
    if not client.platform_enabled:
        return []

    queue = Config.MATCHUP_LEAGUE_QUEUE_TYPE
    cap = max(1, Config.MATCHUP_LADDER_MAX_PER_TIER)
    out: list[str] = []
    seen: set[str] = set()

    for tier in Config.MATCHUP_LADDER_TIERS:
        path_tpl = _TIER_PATH.get(tier)
        if not path_tpl:
            continue
        path = path_tpl.format(queue=queue)
        data = client.platform_get(path)
        if not isinstance(data, dict):
            continue
        entries = data.get("entries")
        if not isinstance(entries, list):
            continue
        for ent in entries[:cap]:
            if not isinstance(ent, dict):
                continue
            sid = ent.get("summonerId")
            if not isinstance(sid, str) or not sid:
                continue
            summ = client.summoner_by_encrypted_id(sid)
            if not isinstance(summ, dict):
                continue
            puuid = summ.get("puuid")
            if isinstance(puuid, str) and puuid and puuid not in seen:
                seen.add(puuid)
                out.append(puuid)
    return out
