from __future__ import annotations

from typing import Any

import requests


class RiotAPIError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(message)


class RiotClient:
    def __init__(
        self,
        api_key: str,
        regional_route: str,
        platform_route: str | None = None,
    ) -> None:
        self._api_key = api_key
        self._regional_route = regional_route.rstrip("/")
        self._base = f"https://{self._regional_route}.api.riotgames.com"
        pr = (platform_route or "").strip().lower()
        self._platform_route = pr
        self._platform_base = (
            f"https://{pr}.api.riotgames.com" if pr else ""
        )

    def _headers(self) -> dict[str, str]:
        return {"X-Riot-Token": self._api_key}

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._base}{path}"
        r = requests.get(url, headers=self._headers(), params=params or {}, timeout=30)
        if r.status_code != 200:
            detail = r.text[:500] if r.text else ""
            raise RiotAPIError(r.status_code, detail or r.reason)
        if not r.content:
            return None
        return r.json()

    @property
    def platform_enabled(self) -> bool:
        return bool(self._platform_base)

    def platform_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if not self._platform_base:
            raise RiotAPIError(0, "RIOT_PLATFORM_ROUTE is not set (required for ladder seeds)")
        url = f"{self._platform_base}{path}"
        r = requests.get(url, headers=self._headers(), params=params or {}, timeout=30)
        if r.status_code != 200:
            detail = r.text[:500] if r.text else ""
            raise RiotAPIError(r.status_code, detail or r.reason)
        if not r.content:
            return None
        return r.json()

    def summoner_by_encrypted_id(self, encrypted_summoner_id: str) -> dict[str, Any]:
        data = self.platform_get(
            f"/lol/summoner/v4/summoners/{encrypted_summoner_id}",
        )
        if not isinstance(data, dict):
            return {}
        return data

    def match_ids_by_puuid(
        self,
        puuid: str,
        *,
        start: int = 0,
        count: int = 20,
        queue: int | None = None,
    ) -> list[str]:
        params: dict[str, Any] = {"start": start, "count": count}
        if queue is not None:
            params["queue"] = queue
        data = self._get(
            f"/lol/match/v5/matches/by-puuid/{puuid}/ids",
            params=params,
        )
        if not isinstance(data, list):
            return []
        return [str(x) for x in data]

    def match_by_id(self, match_id: str) -> dict[str, Any]:
        data = self._get(f"/lol/match/v5/matches/{match_id}")
        if not isinstance(data, dict):
            return {}
        return data

    def match_timeline_by_id(self, match_id: str) -> dict[str, Any]:
        """Match-V5 timeline (regional route, same host as match_by_id)."""
        data = self._get(f"/lol/match/v5/matches/{match_id}/timeline")
        if data is None:
            raise RiotAPIError(0, "empty timeline response body")
        if not isinstance(data, dict):
            raise RiotAPIError(0, f"timeline JSON was {type(data).__name__}, expected object")
        return data
