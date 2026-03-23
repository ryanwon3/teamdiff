from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def _connect(path: str) -> sqlite3.Connection:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=30.0)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_schema(path: str) -> None:
    with _connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS matches (
                match_id TEXT PRIMARY KEY,
                queue_id INTEGER NOT NULL,
                game_version TEXT,
                ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS participants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
                puuid TEXT,
                champion_id INTEGER NOT NULL,
                team_id INTEGER NOT NULL,
                win INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_participants_match
                ON participants(match_id);
            CREATE INDEX IF NOT EXISTS idx_participants_champion
                ON participants(champion_id);
            """
        )


def match_exists(path: str, match_id: str) -> bool:
    with _connect(path) as conn:
        row = conn.execute(
            "SELECT 1 FROM matches WHERE match_id = ? LIMIT 1",
            (match_id,),
        ).fetchone()
    return row is not None


def insert_match_if_new(path: str, match: dict[str, Any]) -> bool:
    """
    Persist one Match-V5 payload if match_id is new. Returns True if inserted.
    """
    mid = str(match.get("metadata", {}).get("matchId") or "")
    if not mid:
        return False
    info = match.get("info") or {}
    participants = info.get("participants")
    if not isinstance(participants, list) or not participants:
        return False

    queue_id = info.get("queueId")
    if queue_id is None:
        return False
    game_version = info.get("gameVersion")
    if isinstance(game_version, str):
        gv = game_version
    else:
        gv = None

    with _connect(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "INSERT OR IGNORE INTO matches (match_id, queue_id, game_version) VALUES (?, ?, ?)",
                (mid, int(queue_id), gv),
            )
            if cur.rowcount == 0:
                conn.rollback()
                return False
            inserted_rows = 0
            for p in participants:
                if not isinstance(p, dict):
                    continue
                cid = p.get("championId")
                tid = p.get("teamId")
                if cid is None or tid is None:
                    continue
                win = 1 if p.get("win") else 0
                puuid = p.get("puuid")
                if isinstance(puuid, str):
                    pu = puuid
                else:
                    pu = None
                conn.execute(
                    """
                    INSERT INTO participants (match_id, puuid, champion_id, team_id, win)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (mid, pu, int(cid), int(tid), win),
                )
                inserted_rows += 1
            if inserted_rows == 0:
                conn.rollback()
                return False
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return True


def aggregate_matchup(
    path: str,
    *,
    champion_a: int,
    champion_b: int,
    queue_id: int | None,
) -> dict[str, Any]:
    """
    Same pairing semantics as matchup._observations_from_participants:
    count (pa, pb) pairs on opposite teams with those champion ids.
    """
    if champion_a == champion_b:
        return {
            "error": "champion_a and champion_b must differ",
            "wins_a": 0,
            "games": 0,
            "winrate": None,
        }

    init_schema(path)

    if queue_id is None:
        sql = """
            SELECT
                COALESCE(SUM(CASE WHEN pa.win != 0 THEN 1 ELSE 0 END), 0),
                COUNT(*)
            FROM participants pa
            JOIN participants pb
              ON pa.match_id = pb.match_id AND pa.team_id != pb.team_id
            WHERE pa.champion_id = ? AND pb.champion_id = ?
        """
        params: tuple[int, ...] = (champion_a, champion_b)
    else:
        sql = """
            SELECT
                COALESCE(SUM(CASE WHEN pa.win != 0 THEN 1 ELSE 0 END), 0),
                COUNT(*)
            FROM participants pa
            JOIN participants pb
              ON pa.match_id = pb.match_id AND pa.team_id != pb.team_id
            JOIN matches m ON m.match_id = pa.match_id
            WHERE pa.champion_id = ? AND pb.champion_id = ? AND m.queue_id = ?
        """
        params = (champion_a, champion_b, queue_id)

    with _connect(path) as conn:
        row = conn.execute(sql, params).fetchone()
    total_wins_a = int(row[0] or 0)
    total_games = int(row[1] or 0)

    winrate: float | None
    if total_games == 0:
        winrate = None
    else:
        winrate = round(total_wins_a / total_games, 4)

    return {
        "wins_a": total_wins_a,
        "games": total_games,
        "winrate": winrate,
        "match_detail_fetches": 0,
        "match_ids_considered": 0,
        "sample_size_warning": total_games < 10,
        "source": "sqlite",
    }


def _connect_readonly(path: str) -> sqlite3.Connection:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(path)
    uri = f"file:{p.resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    return conn


def fetch_db_summary(path: str) -> dict[str, Any] | None:
    """
    Totals and breakdowns for inspect UI. Returns None if path is not a file.
    """
    p = Path(path)
    if not p.is_file():
        return None
    try:
        with _connect_readonly(str(p)) as conn:
            matches_count = int(
                conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
            )
            participants_count = int(
                conn.execute("SELECT COUNT(*) FROM participants").fetchone()[0]
            )
            queue_rows = conn.execute(
                """
                SELECT queue_id, COUNT(*) AS c
                FROM matches
                GROUP BY queue_id
                ORDER BY c DESC
                """
            ).fetchall()
            mm = conn.execute(
                "SELECT MIN(ingested_at), MAX(ingested_at) FROM matches"
            ).fetchone()
    except sqlite3.Error:
        return None

    ingested_min = mm[0] if mm else None
    ingested_max = mm[1] if mm else None
    queue_breakdown = [
        {"queue_id": int(r[0]), "count": int(r[1])} for r in queue_rows
    ]
    try:
        file_size_bytes = p.stat().st_size
    except OSError:
        file_size_bytes = None

    return {
        "matches_count": matches_count,
        "participants_count": participants_count,
        "queue_breakdown": queue_breakdown,
        "ingested_at_min": ingested_min,
        "ingested_at_max": ingested_max,
        "file_size_bytes": file_size_bytes,
    }


def fetch_matches_page(path: str, *, limit: int, offset: int) -> list[dict[str, Any]]:
    """
    Paginated matches ordered by ingested_at DESC. Parameterized limit/offset.
    Raises FileNotFoundError if path is not a file.
    """
    if limit < 1 or offset < 0:
        return []
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(path)
    with _connect_readonly(str(p)) as conn:
        rows = conn.execute(
            """
            SELECT
                m.match_id,
                m.queue_id,
                m.game_version,
                m.ingested_at,
                (SELECT COUNT(*) FROM participants p WHERE p.match_id = m.match_id)
                    AS participant_count
            FROM matches m
            ORDER BY m.ingested_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "match_id": r[0],
                "queue_id": int(r[1]),
                "game_version": r[2],
                "ingested_at": r[3],
                "participant_count": int(r[4] or 0),
            }
        )
    return out
