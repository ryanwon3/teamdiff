"""Format SQLite ingest timestamps for display (UTC → US Eastern)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

_UTC = ZoneInfo("UTC")
_EASTERN = ZoneInfo("America/New_York")


def ingested_at_est_display(raw: str | None) -> str | None:
    """
    SQLite `ingested_at` from `datetime('now')` is UTC. Convert to America/New_York
    (handles EST/EDT) for a fixed-width display string.
    """
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if len(s) < 19:
        return None
    try:
        dt_naive = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    dt_utc = dt_naive.replace(tzinfo=_UTC)
    dt_et = dt_utc.astimezone(_EASTERN)
    return dt_et.strftime("%Y-%m-%d %I:%M:%S %p %Z")
