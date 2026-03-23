from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

_GOLD_CURVE_MAX_MINUTE = 25
_GOLD_LEADER_MINUTE_CAP = 15
_TIMELINE_MINUTE_CAP = 30


def normalize_team_position(raw: str | None) -> str | None:
    """
    Canonical lane key for pairing (Riot match-v5: TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY).
    Maps SUPPORT -> UTILITY; returns None for empty/invalid.
    """
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if not s or s in ("NONE", "INVALID"):
        return None
    if s == "SUPPORT":
        return "UTILITY"
    if s == "MID":
        return "MIDDLE"
    if s in ("TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"):
        return s
    return None


def extract_participant_riot_fields(p: dict[str, Any]) -> tuple[int | None, str | None]:
    """
    Parse Riot Match-V5 participant object (camelCase; tolerate snake_case).
    Returns (riot_participant_id 1–10, normalized lane or None).
    """
    pid_raw = p.get("participantId")
    if pid_raw is None:
        pid_raw = p.get("participant_id")
    riot_pid: int | None
    if pid_raw is None:
        riot_pid = None
    else:
        try:
            riot_pid = int(pid_raw)
        except (TypeError, ValueError):
            riot_pid = None

    pos_raw = p.get("teamPosition")
    if not isinstance(pos_raw, str) or not pos_raw.strip():
        alt = p.get("individualPosition")
        if isinstance(alt, str) and alt.strip():
            pos_raw = alt
        else:
            tr = p.get("team_position")
            pos_raw = tr if isinstance(tr, str) else None
    lane_pos = (
        normalize_team_position(pos_raw) if isinstance(pos_raw, str) else None
    )
    return riot_pid, lane_pos


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _participant_timeline_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='participant_timeline' LIMIT 1"
    ).fetchone()
    return row is not None


def _apply_schema_migrations(conn: sqlite3.Connection) -> None:
    cols = _table_columns(conn, "participants")
    if "participant_id" not in cols:
        conn.execute("ALTER TABLE participants ADD COLUMN participant_id INTEGER")
    if "team_position" not in cols:
        conn.execute("ALTER TABLE participants ADD COLUMN team_position TEXT")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS participant_timeline (
            match_id TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
            participant_id INTEGER NOT NULL,
            minute INTEGER NOT NULL,
            total_gold INTEGER NOT NULL,
            level INTEGER NOT NULL,
            PRIMARY KEY (match_id, participant_id, minute)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_participant_timeline_match "
        "ON participant_timeline(match_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_participant_timeline_match_participant "
        "ON participant_timeline(match_id, participant_id)"
    )


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
        _apply_schema_migrations(conn)


def match_exists(path: str, match_id: str) -> bool:
    with _connect(path) as conn:
        row = conn.execute(
            "SELECT 1 FROM matches WHERE match_id = ? LIMIT 1",
            (match_id,),
        ).fetchone()
    return row is not None


def insert_match_if_new(
    path: str,
    match: dict[str, Any],
    *,
    match_id_prefix: str | None = None,
) -> bool:
    """
    Persist one Match-V5 payload if match_id is new. Returns True if inserted.
    If match_id_prefix is set, skip (return False) when match_id does not start with it.
    """
    mid = str(match.get("metadata", {}).get("matchId") or "")
    if not mid:
        return False
    if match_id_prefix and not mid.startswith(match_id_prefix):
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

    init_schema(path)

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
                riot_pid, lane_pos = extract_participant_riot_fields(p)
                conn.execute(
                    """
                    INSERT INTO participants (
                        match_id, puuid, champion_id, team_id, win,
                        participant_id, team_position
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (mid, pu, int(cid), int(tid), win, riot_pid, lane_pos),
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


def match_ids_needing_participant_meta(path: str, *, limit: int = 500) -> list[str]:
    """Newest-first match IDs that have at least one participant row missing lane or Riot id."""
    lim = max(1, min(int(limit), 5000))
    p = Path(path)
    if not p.is_file():
        return []
    init_schema(path)
    with _connect_readonly(str(p)) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT p.match_id
            FROM participants p
            WHERE p.participant_id IS NULL
               OR p.team_position IS NULL
               OR TRIM(COALESCE(p.team_position, '')) = ''
            ORDER BY p.match_id DESC
            LIMIT ?
            """,
            (lim,),
        ).fetchall()
    return [str(r[0]) for r in rows]


def match_needs_participant_meta_backfill(path: str, match_id: str) -> bool:
    """
    True if any participant row for this match is missing Riot participant id or lane,
    so gold/lane SQL cannot pair timelines to champions.
    """
    mid = (match_id or "").strip()
    if not mid:
        return False
    p = Path(path)
    if not p.is_file():
        return False
    init_schema(path)
    with _connect_readonly(str(p)) as conn:
        cols = _table_columns(conn, "participants")
        if "participant_id" not in cols or "team_position" not in cols:
            return True
        row = conn.execute(
            """
            SELECT 1 FROM participants
            WHERE match_id = ?
              AND (
                participant_id IS NULL
                OR team_position IS NULL
                OR TRIM(team_position) = ''
              )
            LIMIT 1
            """,
            (mid,),
        ).fetchone()
    return row is not None


def merge_participant_meta_from_match(path: str, match: dict[str, Any]) -> int:
    """
    UPDATE existing participant rows from a Match-V5 detail payload.
    Fills NULL/missing participant_id and team_position (COALESCE: only overwrites when
    API provides a value). Returns number of rows touched (rowcount sum).
    """
    mid = str(match.get("metadata", {}).get("matchId") or "").strip()
    if not mid:
        return 0
    info = match.get("info") or {}
    participants = info.get("participants")
    if not isinstance(participants, list) or not participants:
        return 0

    init_schema(path)
    updated = 0
    with _connect(path) as conn:
        if not conn.execute(
            "SELECT 1 FROM matches WHERE match_id = ? LIMIT 1", (mid,)
        ).fetchone():
            return 0
        for p in participants:
            if not isinstance(p, dict):
                continue
            cid = p.get("championId")
            tid = p.get("teamId")
            if cid is None or tid is None:
                continue
            riot_pid, lane_pos = extract_participant_riot_fields(p)
            cur = conn.execute(
                """
                UPDATE participants
                SET
                    participant_id = COALESCE(?, participant_id),
                    team_position = COALESCE(?, team_position)
                WHERE match_id = ?
                  AND champion_id = ?
                  AND team_id = ?
                """,
                (riot_pid, lane_pos, mid, int(cid), int(tid)),
            )
            updated += cur.rowcount
        conn.commit()
    return updated


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


def _timeline_frames_from_payload(timeline: dict[str, Any]) -> list[Any]:
    """Resolve frames list from Match-V5 timeline (normalizes a few edge layouts)."""
    info = timeline.get("info")
    if isinstance(info, dict) and isinstance(info.get("frames"), list):
        return info["frames"]
    if isinstance(timeline.get("frames"), list):
        return timeline["frames"]
    return []


def count_timeline_frames(timeline: dict[str, Any]) -> int:
    """Number of frame entries in a Match-V5 timeline payload (0 if missing/malformed)."""
    if not isinstance(timeline, dict):
        return 0
    return len(_timeline_frames_from_payload(timeline))


def _participant_frame_participants(pf: Any) -> list[tuple[int, dict[str, Any]]]:
    """
    Riot uses participantFrames as a JSON object keyed by "1".."10".
    Accept a list of frame dicts with participantId as a defensive fallback.
    """
    out: list[tuple[int, dict[str, Any]]] = []
    if isinstance(pf, dict):
        for key, pdata in pf.items():
            if not isinstance(pdata, dict):
                continue
            pid: int | None
            try:
                pid = int(key)
            except (TypeError, ValueError):
                raw = pdata.get("participantId")
                if raw is None:
                    continue
                try:
                    pid = int(raw)
                except (TypeError, ValueError):
                    continue
            out.append((pid, pdata))
    elif isinstance(pf, list):
        for pdata in pf:
            if not isinstance(pdata, dict):
                continue
            raw = pdata.get("participantId")
            if raw is None:
                continue
            try:
                pid = int(raw)
            except (TypeError, ValueError):
                continue
            out.append((pid, pdata))
    return out


def ingest_match_timeline(
    path: str, match_id: str, timeline: dict[str, Any]
) -> int:
    """
    Upsert participant_timeline from Match-V5 timeline JSON.
    minute = round(timestamp_ms / 60000), clamped 0..30.
    Later frames for the same (match_id, participant_id, minute) overwrite earlier.
    """
    init_schema(path)
    mid = (match_id or "").strip()
    if not mid or not isinstance(timeline, dict):
        return 0
    frames = _timeline_frames_from_payload(timeline)
    if not frames:
        return 0

    rows: list[tuple[str, int, int, int, int]] = []
    for fr in frames:
        if not isinstance(fr, dict):
            continue
        ts = fr.get("timestamp")
        try:
            minute = int(round(float(ts) / 60000.0))
        except (TypeError, ValueError):
            continue
        if minute < 0:
            minute = 0
        if minute > _TIMELINE_MINUTE_CAP:
            minute = _TIMELINE_MINUTE_CAP
        pf = fr.get("participantFrames")
        for pid, pdata in _participant_frame_participants(pf):
            tg = pdata.get("totalGold")
            lv = pdata.get("level")
            if tg is None or lv is None:
                continue
            try:
                tg_i = int(tg)
                lv_i = int(lv)
            except (TypeError, ValueError):
                continue
            rows.append((mid, pid, minute, tg_i, lv_i))

    if not rows:
        return 0

    with _connect(path) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO participant_timeline (
                match_id, participant_id, minute, total_gold, level
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def match_ids_missing_timeline(path: str, *, limit: int = 500) -> list[str]:
    """
    Match IDs newest-first that have no rows in participant_timeline.
    If the timeline table is missing, returns the newest matches (all need backfill).
    """
    lim = max(1, min(int(limit), 10_000))
    p = Path(path)
    if not p.is_file():
        return []
    with _connect_readonly(str(p)) as conn:
        if not _participant_timeline_exists(conn):
            rows = conn.execute(
                """
                SELECT match_id FROM matches
                ORDER BY ingested_at DESC
                LIMIT ?
                """,
                (lim,),
            ).fetchall()
            return [str(r[0]) for r in rows]
        rows = conn.execute(
            """
            SELECT m.match_id
            FROM matches m
            WHERE NOT EXISTS (
                SELECT 1 FROM participant_timeline t WHERE t.match_id = m.match_id
            )
            ORDER BY m.ingested_at DESC
            LIMIT ?
            """,
            (lim,),
        ).fetchall()
    return [str(r[0]) for r in rows]


def _lane_pairs_cte(*, queue_id: int | None) -> tuple[str, list[Any]]:
    """Shared lane-opponent filter: anchor champion id bound as first param."""
    base = """
        lane_pairs AS (
            SELECT
                pa.match_id,
                pa.participant_id AS pid_a,
                pb.participant_id AS pid_b,
                pb.champion_id AS opp_champion_id
            FROM participants pa
            INNER JOIN participants pb
                ON pa.match_id = pb.match_id
                AND pa.team_id != pb.team_id
                AND pa.champion_id != pb.champion_id
            INNER JOIN matches m ON m.match_id = pa.match_id
            WHERE pa.champion_id = ?
                AND pa.participant_id IS NOT NULL
                AND pb.participant_id IS NOT NULL
                AND pa.team_position IS NOT NULL
                AND TRIM(pa.team_position) != ''
                AND pa.team_position = pb.team_position
    """
    if queue_id is None:
        return base + "\n        )\n", []
    return (
        base + "\n                AND m.queue_id = ?\n        )\n",
        [queue_id],
    )


def _lane_pairs_pair_cte(*, queue_id: int | None) -> tuple[str, list[Any]]:
    """Lane pairs for fixed champion_a and champion_b (both params first)."""
    base = """
        lane_pairs AS (
            SELECT
                pa.match_id,
                pa.participant_id AS pid_a,
                pb.participant_id AS pid_b
            FROM participants pa
            INNER JOIN participants pb
                ON pa.match_id = pb.match_id
                AND pa.team_id != pb.team_id
            INNER JOIN matches m ON m.match_id = pa.match_id
            WHERE pa.champion_id = ?
                AND pb.champion_id = ?
                AND pa.participant_id IS NOT NULL
                AND pb.participant_id IS NOT NULL
                AND pa.team_position IS NOT NULL
                AND TRIM(pa.team_position) != ''
                AND pa.team_position = pb.team_position
    """
    if queue_id is None:
        return base + "\n        )\n", []
    return (
        base + "\n                AND m.queue_id = ?\n        )\n",
        [queue_id],
    )


def fetch_gold_leaders_at_15(
    path: str,
    *,
    champion_anchor: int,
    queue_id: int | None,
    min_games: int = 0,
    lead_sort: str = "asc",
) -> dict[str, Any]:
    """
    Same-lane opponents for the anchor champion, one row per opponent champion id.
    avg(opponent_gold - anchor_gold) at best minute <= 15 (max minute <= 15 per side).
    lead_sort: "asc" (default) = smallest average gold gap first; "desc" = largest first.
    min_games filters out opponents with fewer qualifying games (0 = all with ≥1 game).
    """
    ls = (lead_sort or "").strip().lower()
    order = "DESC" if ls == "desc" else "ASC"
    init_schema(path)
    cte, extra = _lane_pairs_cte(queue_id=queue_id)
    sql = f"""
        WITH {cte},
        gold_snap AS (
            SELECT
                lp.opp_champion_id,
                lp.match_id,
                ga.total_gold AS gold_a,
                gb.total_gold AS gold_b
            FROM lane_pairs lp
            INNER JOIN (
                SELECT match_id, participant_id, MAX(minute) AS best_m
                FROM participant_timeline
                WHERE minute <= ?
                GROUP BY match_id, participant_id
            ) xa
                ON xa.match_id = lp.match_id AND xa.participant_id = lp.pid_a
            INNER JOIN participant_timeline ga
                ON ga.match_id = xa.match_id
                AND ga.participant_id = xa.participant_id
                AND ga.minute = xa.best_m
            INNER JOIN (
                SELECT match_id, participant_id, MAX(minute) AS best_m
                FROM participant_timeline
                WHERE minute <= ?
                GROUP BY match_id, participant_id
            ) xb
                ON xb.match_id = lp.match_id AND xb.participant_id = lp.pid_b
            INNER JOIN participant_timeline gb
                ON gb.match_id = xb.match_id
                AND gb.participant_id = xb.participant_id
                AND gb.minute = xb.best_m
        )
        SELECT opp_champion_id,
               AVG(CAST(gold_b AS REAL) - gold_a) AS avg_lead,
               COUNT(*) AS n
        FROM gold_snap
        GROUP BY opp_champion_id
        HAVING COUNT(*) >= ?
        ORDER BY avg_lead {order}
    """
    params: list[Any] = (
        [champion_anchor]
        + list(extra)
        + [_GOLD_LEADER_MINUTE_CAP, _GOLD_LEADER_MINUTE_CAP, max(0, min_games)]
    )
    with _connect_readonly(path) as conn:
        rows = conn.execute(sql, params).fetchall()

    leaders: list[dict[str, Any]] = []
    for r in rows:
        leaders.append(
            {
                "opponent_id": int(r[0]),
                "avg_gold_lead_at_15": round(float(r[1]), 2),
                "games": int(r[2]),
            }
        )

    lane_sql = f"WITH {cte} SELECT COUNT(DISTINCT match_id) FROM lane_pairs"
    lane_params: list[Any] = [champion_anchor] + list(extra)
    with _connect_readonly(path) as conn:
        lane_row = conn.execute(lane_sql, lane_params).fetchone()
        gold_sql = f"""
            WITH {cte},
            gold_snap AS (
                SELECT lp.match_id
                FROM lane_pairs lp
                INNER JOIN (
                    SELECT match_id, participant_id, MAX(minute) AS best_m
                    FROM participant_timeline
                    WHERE minute <= ?
                    GROUP BY match_id, participant_id
                ) xa
                    ON xa.match_id = lp.match_id AND xa.participant_id = lp.pid_a
                INNER JOIN participant_timeline ga
                    ON ga.match_id = xa.match_id
                    AND ga.participant_id = xa.participant_id
                    AND ga.minute = xa.best_m
                INNER JOIN (
                    SELECT match_id, participant_id, MAX(minute) AS best_m
                    FROM participant_timeline
                    WHERE minute <= ?
                    GROUP BY match_id, participant_id
                ) xb
                    ON xb.match_id = lp.match_id AND xb.participant_id = lp.pid_b
                INNER JOIN participant_timeline gb
                    ON gb.match_id = xb.match_id
                    AND gb.participant_id = xb.participant_id
                    AND gb.minute = xb.best_m
            )
            SELECT COUNT(DISTINCT match_id) FROM gold_snap
        """
        gold_params = (
            [champion_anchor]
            + list(extra)
            + [_GOLD_LEADER_MINUTE_CAP, _GOLD_LEADER_MINUTE_CAP]
        )
        gold_row = conn.execute(gold_sql, gold_params).fetchone()

        diag_any = """
            SELECT COUNT(DISTINCT pa.match_id)
            FROM participants pa
            INNER JOIN matches m ON m.match_id = pa.match_id
            WHERE pa.champion_id = ?
        """
        diag_lane = """
            SELECT COUNT(DISTINCT pa.match_id)
            FROM participants pa
            INNER JOIN matches m ON m.match_id = pa.match_id
            WHERE pa.champion_id = ?
              AND pa.participant_id IS NOT NULL
              AND pa.team_position IS NOT NULL
              AND TRIM(pa.team_position) != ''
        """
        if queue_id is None:
            dparams = [champion_anchor]
            any_row = conn.execute(diag_any, dparams).fetchone()
            lane_meta_row = conn.execute(diag_lane, dparams).fetchone()
        else:
            dparams_q = [champion_anchor, queue_id]
            any_row = conn.execute(
                diag_any + " AND m.queue_id = ?", dparams_q
            ).fetchone()
            lane_meta_row = conn.execute(
                diag_lane + " AND m.queue_id = ?", dparams_q
            ).fetchone()

    return {
        "champion_anchor_id": champion_anchor,
        "lead_sort": "desc" if order == "DESC" else "asc",
        "lane_games": int(lane_row[0] or 0) if lane_row else 0,
        "games_with_gold_at_15": int(gold_row[0] or 0) if gold_row else 0,
        "anchor_match_count": int(any_row[0] or 0) if any_row else 0,
        "anchor_matches_with_lane_meta": int(lane_meta_row[0] or 0)
        if lane_meta_row
        else 0,
        "leaders": leaders,
    }


def fetch_gold_curve(
    path: str,
    *,
    champion_a: int,
    champion_b: int,
    queue_id: int | None,
    mode: str,
) -> dict[str, Any]:
    """
    Aggregated gold curves for lane-matched games. mode: 'time' or 'level'.
    Time: x = minute 0..25; y = mean total_gold per side (games with both at that minute).
    Level: x = round((level_a+level_b)/2); frame-based samples (uneven spacing).
    """
    init_schema(path)
    if champion_a == champion_b:
        return {"error": "champion_a and champion_b must differ"}
    m = (mode or "").strip().lower()
    if m not in ("time", "level"):
        return {"error": "mode must be time or level"}

    cte, extra = _lane_pairs_pair_cte(queue_id=queue_id)
    params_head: list[Any] = [champion_a, champion_b] + list(extra)

    with _connect_readonly(path) as conn:
        lane_row = conn.execute(
            f"WITH {cte} SELECT COUNT(*) FROM lane_pairs", params_head
        ).fetchone()
        lane_games = int(lane_row[0] or 0) if lane_row else 0

        if m == "time":
            sql = f"""
                WITH {cte}
                SELECT
                    ta.minute AS x_val,
                    AVG(ta.total_gold) AS avg_a,
                    AVG(tb.total_gold) AS avg_b,
                    COUNT(*) AS n
                FROM lane_pairs lp
                INNER JOIN participant_timeline ta
                    ON ta.match_id = lp.match_id AND ta.participant_id = lp.pid_a
                INNER JOIN participant_timeline tb
                    ON tb.match_id = lp.match_id
                    AND tb.participant_id = lp.pid_b
                    AND tb.minute = ta.minute
                WHERE ta.minute >= 0 AND ta.minute <= ?
                GROUP BY ta.minute
                ORDER BY ta.minute
            """
            rows = conn.execute(
                sql, params_head + [_GOLD_CURVE_MAX_MINUTE]
            ).fetchall()
        else:
            sql = f"""
                WITH {cte}
                SELECT
                    CAST(ROUND((ta.level + tb.level) / 2.0) AS INTEGER) AS x_val,
                    AVG(ta.total_gold) AS avg_a,
                    AVG(tb.total_gold) AS avg_b,
                    COUNT(*) AS n
                FROM lane_pairs lp
                INNER JOIN participant_timeline ta
                    ON ta.match_id = lp.match_id AND ta.participant_id = lp.pid_a
                INNER JOIN participant_timeline tb
                    ON tb.match_id = lp.match_id
                    AND tb.participant_id = lp.pid_b
                    AND tb.minute = ta.minute
                GROUP BY x_val
                HAVING x_val BETWEEN 1 AND 18
                ORDER BY x_val
            """
            rows = conn.execute(sql, params_head).fetchall()

        curve_games_row = conn.execute(
            f"""
            WITH {cte}
            SELECT COUNT(DISTINCT lp.match_id)
            FROM lane_pairs lp
            INNER JOIN participant_timeline ta
                ON ta.match_id = lp.match_id AND ta.participant_id = lp.pid_a
            INNER JOIN participant_timeline tb
                ON tb.match_id = lp.match_id
                AND tb.participant_id = lp.pid_b
                AND tb.minute = ta.minute
            """,
            params_head,
        ).fetchone()
    curve_games = int(curve_games_row[0] or 0) if curve_games_row else 0

    if not rows:
        return {
            "error": "Insufficient timeline data for this lane matchup",
            "lane_games": lane_games,
            "games_curve": curve_games,
        }

    labels: list[str] = []
    data_a: list[float] = []
    data_b: list[float] = []
    ns: list[int] = []
    for r in rows:
        labels.append(str(int(r[0])))
        data_a.append(round(float(r[1]), 2))
        data_b.append(round(float(r[2]), 2))
        ns.append(int(r[3]))

    return {
        "mode": m,
        "labels": labels,
        "series": [
            {"key": "anchor", "data": data_a},
            {"key": "opponent", "data": data_b},
        ],
        "games_per_point": ns,
        "games_lane": lane_games,
        "games_curve": curve_games,
        "games_used_min": min(ns) if ns else 0,
        "games_used_max": max(ns) if ns else 0,
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
            tinfo = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                "AND name='participant_timeline'"
            ).fetchone()
            has_tl = bool(tinfo and int(tinfo[0] or 0))
            if has_tl:
                participant_timeline_rows = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM participant_timeline"
                    ).fetchone()[0]
                )
                matches_with_timeline = int(
                    conn.execute(
                        "SELECT COUNT(DISTINCT match_id) FROM participant_timeline"
                    ).fetchone()[0]
                )
                avg_row = conn.execute(
                    """
                    SELECT AVG(cnt) FROM (
                        SELECT COUNT(*) AS cnt
                        FROM participant_timeline
                        GROUP BY match_id
                    )
                    """
                ).fetchone()[0]
                avg_timeline_rows_per_match = (
                    round(float(avg_row), 1) if avg_row is not None else None
                )
            else:
                participant_timeline_rows = 0
                matches_with_timeline = 0
                avg_timeline_rows_per_match = None

            pcols = _table_columns(conn, "participants")
            if (
                has_tl
                and "participant_id" in pcols
                and "team_position" in pcols
            ):
                participants_lane_ready = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM participants
                        WHERE participant_id IS NOT NULL
                          AND team_position IS NOT NULL
                          AND TRIM(team_position) != ''
                        """
                    ).fetchone()[0]
                )
            else:
                participants_lane_ready = 0

            matches_ingested_last_24h = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM matches
                    WHERE datetime(ingested_at) >= datetime('now', '-1 day')
                    """
                ).fetchone()[0]
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
    matches_without_timeline = max(0, matches_count - matches_with_timeline)
    timeline_coverage_pct: float | None
    if matches_count > 0:
        timeline_coverage_pct = round(100.0 * matches_with_timeline / matches_count, 1)
    else:
        timeline_coverage_pct = None

    try:
        file_size_bytes = p.stat().st_size
    except OSError:
        file_size_bytes = None

    return {
        "matches_count": matches_count,
        "participants_count": participants_count,
        "participants_lane_ready": participants_lane_ready,
        "participant_timeline_rows": participant_timeline_rows,
        "matches_with_timeline": matches_with_timeline,
        "matches_without_timeline": matches_without_timeline,
        "timeline_coverage_pct": timeline_coverage_pct,
        "avg_timeline_rows_per_match": avg_timeline_rows_per_match,
        "matches_ingested_last_24h": matches_ingested_last_24h,
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
        has_tl = _participant_timeline_exists(conn)
        if has_tl:
            rows = conn.execute(
                """
                SELECT
                    m.match_id,
                    m.queue_id,
                    m.game_version,
                    m.ingested_at,
                    (SELECT COUNT(*) FROM participants p WHERE p.match_id = m.match_id)
                        AS participant_count,
                    (SELECT COUNT(*) FROM participant_timeline pt
                     WHERE pt.match_id = m.match_id) AS timeline_row_count
                FROM matches m
                ORDER BY m.ingested_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    m.match_id,
                    m.queue_id,
                    m.game_version,
                    m.ingested_at,
                    (SELECT COUNT(*) FROM participants p WHERE p.match_id = m.match_id)
                        AS participant_count,
                    0 AS timeline_row_count
                FROM matches m
                ORDER BY m.ingested_at DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        tl_count = int(r[5] or 0)
        out.append(
            {
                "match_id": r[0],
                "queue_id": int(r[1]),
                "game_version": r[2],
                "ingested_at": r[3],
                "participant_count": int(r[4] or 0),
                "timeline_row_count": tl_count,
                "has_timeline": tl_count > 0,
            }
        )
    return out


def fetch_match_detail(path: str, match_id: str) -> dict[str, Any] | None:
    """
    Match row plus participant rows for one match_id. Returns None if match missing.
    """
    mid = (match_id or "").strip()
    if not mid:
        return None
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(path)
    with _connect_readonly(str(p)) as conn:
        mrow = conn.execute(
            """
            SELECT match_id, queue_id, game_version, ingested_at
            FROM matches
            WHERE match_id = ?
            LIMIT 1
            """,
            (mid,),
        ).fetchone()
        if not mrow:
            return None
        has_tl = _participant_timeline_exists(conn)
        if has_tl:
            tl_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM participant_timeline WHERE match_id = ?",
                    (mid,),
                ).fetchone()[0]
            )
        else:
            tl_count = 0
        prow = conn.execute(
            """
            SELECT
                champion_id,
                team_id,
                win,
                participant_id,
                team_position,
                puuid
            FROM participants
            WHERE match_id = ?
            ORDER BY team_id, (participant_id IS NULL), participant_id
            """,
            (mid,),
        ).fetchall()
    parts: list[dict[str, Any]] = []
    for r in prow:
        pu = r[5]
        parts.append(
            {
                "champion_id": int(r[0]),
                "team_id": int(r[1]),
                "win": bool(r[2]),
                "participant_id": int(r[3]) if r[3] is not None else None,
                "team_position": r[4],
                "puuid": pu if isinstance(pu, str) else None,
            }
        )
    return {
        "match": {
            "match_id": mrow[0],
            "queue_id": int(mrow[1]),
            "game_version": mrow[2],
            "ingested_at": mrow[3],
            "timeline_row_count": tl_count,
            "has_timeline": tl_count > 0,
        },
        "participants": parts,
    }
