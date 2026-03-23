from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

_EASTERN = ZoneInfo("America/New_York")


def utc_sqlite_to_eastern_display(s: str | None) -> str | None:
    """
    Parse SQLite-style UTC timestamps (no offset) and format for US Eastern display.
    On parse failure, returns the original string so callers still show something.
    """
    if s is None:
        return None
    raw = str(s).strip()
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return raw
    return dt.astimezone(_EASTERN).strftime("%Y-%m-%d %I:%M:%S %p %Z")
