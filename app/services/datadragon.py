from __future__ import annotations

import re
import time
from typing import Any

import requests

_VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json"
_CHAMPION_JSON = (
    "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
)
_CACHE_TTL_SECONDS = 6 * 3600
_REQUEST_TIMEOUT = 15

_index_cache: dict[str, Any] | None = None
_cache_expires_at: float = 0.0


def _normalize_query(s: str) -> str:
    t = s.strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t


def _slug_variants(s: str) -> list[str]:
    """Lowercase keys/names; include compact form without spaces for 'Lee Sin' style."""
    raw = s.strip()
    if not raw:
        return []
    low = raw.lower()
    out = [low, _normalize_query(raw)]
    no_space = re.sub(r"[\s'._-]+", "", low)
    if no_space and no_space not in out:
        out.append(no_space)
    return out


def _fetch_json(url: str) -> Any:
    r = requests.get(url, timeout=_REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _build_index(version: str, champion_payload: dict[str, Any]) -> dict[str, Any]:
    data = champion_payload.get("data") or {}
    by_id: dict[int, dict[str, str]] = {}
    by_slug: dict[str, int] = {}

    for dd_key, champ in data.items():
        if not isinstance(champ, dict):
            continue
        key_raw = champ.get("key")
        if key_raw is None:
            continue
        try:
            cid = int(str(key_raw).strip())
        except ValueError:
            continue
        name = str(champ.get("name") or dd_key)
        icon_rel = f"/cdn/{version}/img/champion/{dd_key}.png"
        by_id[cid] = {
            "key": dd_key,
            "name": name,
            "icon_path": icon_rel,
        }
        for slug in _slug_variants(dd_key):
            by_slug.setdefault(slug, cid)
        for slug in _slug_variants(name):
            by_slug.setdefault(slug, cid)
        sid = str(cid)
        by_slug.setdefault(sid, cid)

    return {
        "version": version,
        "by_id": by_id,
        "by_slug": by_slug,
    }


def get_champion_index() -> dict[str, Any]:
    """
    Cached champion index from Data Dragon: version, by_id[int], by_slug[str -> int].
    Slugs are lowercase / normalized name and internal key (e.g. MonkeyKing, Wukong).
    """
    global _index_cache, _cache_expires_at
    now = time.monotonic()
    if _index_cache is not None and now < _cache_expires_at:
        return _index_cache

    try:
        versions = _fetch_json(_VERSIONS_URL)
        if not isinstance(versions, list) or not versions:
            raise ValueError("versions.json empty or invalid")
        version = str(versions[0])
        champ_url = _CHAMPION_JSON.format(version=version)
        payload = _fetch_json(champ_url)
        _index_cache = _build_index(version, payload)
        _cache_expires_at = now + _CACHE_TTL_SECONDS
        return _index_cache
    except Exception:
        if _index_cache is not None:
            return _index_cache
        raise


def icon_url_for(version: str, dd_key: str) -> str:
    return (
        f"https://ddragon.leagueoflegends.com/cdn/{version}/img/champion/{dd_key}.png"
    )


def list_champions_for_api() -> dict[str, Any]:
    """Payload for GET /api/champions."""
    idx = get_champion_index()
    version = idx["version"]
    by_id: dict[int, dict[str, str]] = idx["by_id"]
    champs = []
    for cid in sorted(by_id.keys()):
        meta = by_id[cid]
        dd_key = meta["key"]
        champs.append(
            {
                "id": cid,
                "name": meta["name"],
                "key": dd_key,
                "icon_url": icon_url_for(version, dd_key),
            }
        )
    return {"version": version, "champions": champs}


def resolve_champion_id(raw: str) -> int | None:
    """Resolve positive integer or name/key slug to numeric champion id."""
    s = (raw or "").strip()
    if not s:
        return None
    if s.isdigit():
        n = int(s)
        if n <= 0:
            return None
        idx = get_champion_index()
        if n in idx["by_id"]:
            return n
        return None
    idx = get_champion_index()
    by_slug: dict[str, int] = idx["by_slug"]
    q = _normalize_query(s)
    if q in by_slug:
        return by_slug[q]
    no_space = re.sub(r"[\s'._-]+", "", q)
    if no_space in by_slug:
        return by_slug[no_space]
    return None


def champion_display(champion_id: int) -> dict[str, str] | None:
    """name, key, icon_url for a known id."""
    idx = get_champion_index()
    meta = idx["by_id"].get(champion_id)
    if not meta:
        return None
    version = idx["version"]
    dd_key = meta["key"]
    return {
        "name": meta["name"],
        "key": dd_key,
        "icon_url": icon_url_for(version, dd_key),
    }
