from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv

_pkg_root = Path(__file__).resolve().parent.parent
_env_at_repo = _pkg_root / ".env"
if _env_at_repo.is_file():
    load_dotenv(dotenv_path=_env_at_repo, override=True)
else:
    load_dotenv()

# Riot PUUIDs are long alphanumeric + hyphen/underscore (base64url-like).
_PUUID_TOKEN = re.compile(r"^[A-Za-z0-9_-]{20,128}$")
_ENV_ASSIGN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")


def _split_puuids(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def _continuation_seeds_from_dotenv(path: Path) -> str | None:
    """If MATCHUP_SEED_PUUIDS= is empty, accept token(s) on the following non-comment line(s)."""
    if not path.is_file():
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith("MATCHUP_SEED_PUUIDS="):
            continue
        rest = stripped.split("=", 1)[1].strip()
        if rest:
            return rest
        for j in range(i + 1, len(lines)):
            cand = lines[j].strip()
            if not cand or cand.startswith("#"):
                continue
            if _ENV_ASSIGN.match(cand):
                break
            parts = [p.strip() for p in cand.split(",") if p.strip()]
            if parts and all(_PUUID_TOKEN.fullmatch(p) for p in parts):
                return ",".join(parts)
        return None
    return None


def _seeds_from_puuids_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    out: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if _PUUID_TOKEN.fullmatch(s):
            out.append(s)
    if not out:
        return None
    return ",".join(out)


def _hydrate_matchup_seeds() -> None:
    if _split_puuids(os.environ.get("MATCHUP_SEED_PUUIDS")):
        return
    source = _continuation_seeds_from_dotenv(_env_at_repo)
    if not source:
        source = _seeds_from_puuids_file(_pkg_root / "puuids.txt")
    if source:
        os.environ["MATCHUP_SEED_PUUIDS"] = source


_hydrate_matchup_seeds()


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _ladder_tier_list() -> list[str]:
    raw = os.environ.get("MATCHUP_LADDER_TIERS", "CHALLENGER,GRANDMASTER")
    tiers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    return tiers or ["CHALLENGER", "GRANDMASTER"]


def _ladder_max_per_tier() -> int:
    raw = (os.environ.get("MATCHUP_LADDER_MAX_PER_TIER") or "").strip()
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0


def _master_max_pages() -> int:
    raw = (os.environ.get("MATCHUP_LADDER_MASTER_MAX_PAGES") or "3").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 3


def _master_cursor_path() -> Path:
    raw = (os.environ.get("MATCHUP_LADDER_MASTER_CURSOR_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return _pkg_root / "data" / "ladder_master_page.cursor"


def _match_id_prefix() -> str | None:
    raw = (os.environ.get("MATCHUP_MATCH_ID_PREFIX") or "").strip()
    return raw if raw else None


def _collector_seed_refresh_seconds() -> float:
    raw = (os.environ.get("COLLECTOR_SEED_REFRESH_SECONDS") or "3600").strip()
    try:
        return max(60.0, float(raw))
    except ValueError:
        return 3600.0


class Config:
    RIOT_API_KEY = os.environ.get("RIOT_API_KEY", "")
    RIOT_REGIONAL_ROUTE = os.environ.get("RIOT_REGIONAL_ROUTE", "americas").strip()
    RIOT_PLATFORM_ROUTE = (os.environ.get("RIOT_PLATFORM_ROUTE") or "").strip()
    MATCHUP_SEED_PUUIDS = _split_puuids(os.environ.get("MATCHUP_SEED_PUUIDS"))
    MATCHUP_MAX_MATCHES = int(os.environ.get("MATCHUP_MAX_MATCHES", "30"))
    _q = os.environ.get("MATCHUP_QUEUE_ID", "").strip()
    MATCHUP_QUEUE_ID = int(_q) if _q.isdigit() else None
    MATCHUP_DB_PATH = (os.environ.get("MATCHUP_DB_PATH") or "").strip()
    MATCHUP_LIVE_FALLBACK = _env_bool("MATCHUP_LIVE_FALLBACK", True)
    MATCHUP_LADDER_SEEDS = _env_bool("MATCHUP_LADDER_SEEDS", False)
    MATCHUP_LADDER_TIERS = _ladder_tier_list()
    # 0 = use all entries returned for Challenger / Grandmaster (no slice).
    MATCHUP_LADDER_MAX_PER_TIER = _ladder_max_per_tier()
    MATCHUP_LADDER_MASTER_MAX_PAGES = _master_max_pages()
    MATCHUP_LADDER_MASTER_CURSOR_PATH = _master_cursor_path()
    MATCHUP_LEAGUE_QUEUE_TYPE = (
        os.environ.get("MATCHUP_LEAGUE_QUEUE_TYPE", "RANKED_SOLO_5x5").strip()
    )
    # Only persist matches whose match_id starts with this prefix (e.g. NA1_ for NA shard).
    MATCHUP_MATCH_ID_PREFIX = _match_id_prefix()
    COLLECTOR_SLEEP_SECONDS = float(os.environ.get("COLLECTOR_SLEEP_SECONDS", "60"))
    COLLECTOR_MATCHLIST_COUNT = int(os.environ.get("COLLECTOR_MATCHLIST_COUNT", "20"))
    COLLECTOR_SEED_REFRESH_SECONDS = _collector_seed_refresh_seconds()
