from __future__ import annotations

from app.timefmt import utc_sqlite_to_eastern_display


def test_utc_sqlite_to_eastern_display_winter() -> None:
    out = utc_sqlite_to_eastern_display("2024-01-15 17:00:00")
    assert out is not None
    assert "2024-01-15" in out
    assert "EST" in out


def test_utc_sqlite_to_eastern_display_summer() -> None:
    out = utc_sqlite_to_eastern_display("2024-07-15 16:00:00")
    assert out is not None
    assert "EDT" in out


def test_utc_sqlite_to_eastern_display_none() -> None:
    assert utc_sqlite_to_eastern_display(None) is None
    assert utc_sqlite_to_eastern_display("") is None


def test_utc_sqlite_to_eastern_display_unparsed_passthrough() -> None:
    raw = "not-a-date"
    assert utc_sqlite_to_eastern_display(raw) == raw
