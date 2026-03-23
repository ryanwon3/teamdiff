from __future__ import annotations

import os
import re
import sqlite3
from typing import Any

import requests
from flask import Blueprint, current_app, jsonify, render_template, request

from app.db.store import (
    fetch_db_summary,
    fetch_gold_curve,
    fetch_gold_leaders_at_15,
    fetch_match_detail,
    fetch_matches_page,
)
from app.riot.client import RiotAPIError, RiotClient
from app.services import datadragon as dd
from app.services.matchup import compute_matchup_stats_hybrid
from app.services.seed_puuids import resolve_matchup_seed_puuids
from app.timefmt import utc_sqlite_to_eastern_display

bp = Blueprint("main", __name__)

_DB_MATCHES_MAX_LIMIT = 500
_DB_MATCHES_DEFAULT_LIMIT = 25
_SAFE_MATCH_ID = re.compile(r"^[A-Za-z0-9_.-]{4,80}$")


def _format_db_summary_eastern(summary: dict[str, Any]) -> None:
    for key in ("ingested_at_min", "ingested_at_max"):
        if key in summary:
            summary[key] = utc_sqlite_to_eastern_display(summary.get(key))


def _format_match_rows_eastern(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        row["ingested_at"] = utc_sqlite_to_eastern_display(row.get("ingested_at"))


def _resolve_champion_query(param: str) -> tuple[int | None, str]:
    raw = (request.args.get(param) or "").strip()
    if not raw:
        return None, raw
    cid = dd.resolve_champion_id(raw)
    return cid, raw


@bp.get("/")
def index():
    return render_template("index.html")


@bp.get("/matchup")
def matchup_page():
    return render_template("matchup.html")


@bp.get("/database")
def database_page():
    return render_template("database.html")


@bp.get("/api/champions")
def api_champions():
    try:
        payload = dd.list_champions_for_api()
    except (requests.RequestException, ValueError, OSError) as e:
        return (
            jsonify(
                {
                    "error": "Failed to load champion data from Data Dragon",
                    "detail": str(e),
                }
            ),
            502,
        )
    return jsonify(payload)


@bp.get("/api/db/summary")
def api_db_summary():
    db_path = (current_app.config.get("MATCHUP_DB_PATH") or "").strip()
    if not db_path or not os.path.isfile(db_path):
        return jsonify({"error": "Database file not configured or missing"}), 404
    summary = fetch_db_summary(db_path)
    if summary is None:
        return jsonify({"error": "Could not read database summary"}), 500
    summary["db_path_configured"] = True
    _format_db_summary_eastern(summary)
    resp = jsonify(summary)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.get("/api/db/matches")
def api_db_matches():
    db_path = (current_app.config.get("MATCHUP_DB_PATH") or "").strip()
    if not db_path or not os.path.isfile(db_path):
        return jsonify({"error": "Database file not configured or missing"}), 404
    try:
        limit = int(request.args.get("limit") or _DB_MATCHES_DEFAULT_LIMIT)
    except ValueError:
        limit = _DB_MATCHES_DEFAULT_LIMIT
    try:
        offset = int(request.args.get("offset") or 0)
    except ValueError:
        offset = 0
    limit = max(1, min(limit, _DB_MATCHES_MAX_LIMIT))
    offset = max(0, offset)
    try:
        rows = fetch_matches_page(db_path, limit=limit, offset=offset)
    except FileNotFoundError:
        return jsonify({"error": "Database file not configured or missing"}), 404
    except (OSError, sqlite3.Error) as e:
        return jsonify({"error": "Could not read matches", "detail": str(e)}), 500
    _format_match_rows_eastern(rows)
    resp = jsonify({"limit": limit, "offset": offset, "matches": rows})
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.get("/api/db/matches/<match_id>")
def api_db_match_detail(match_id: str):
    if not _SAFE_MATCH_ID.match(match_id):
        return jsonify({"error": "Invalid match id"}), 400
    db_path = (current_app.config.get("MATCHUP_DB_PATH") or "").strip()
    if not db_path or not os.path.isfile(db_path):
        return jsonify({"error": "Database file not configured or missing"}), 404
    try:
        detail = fetch_match_detail(db_path, match_id)
    except FileNotFoundError:
        return jsonify({"error": "Database file not configured or missing"}), 404
    except (OSError, sqlite3.Error) as e:
        return jsonify({"error": "Could not read match", "detail": str(e)}), 500
    if detail is None:
        return jsonify({"error": "Match not found"}), 404

    enriched: list[dict[str, Any]] = []
    for p in detail["participants"]:
        row = {k: v for k, v in p.items() if k != "puuid"}
        pu = p.get("puuid")
        if pu and isinstance(pu, str) and len(pu) > 10:
            row["puuid_masked"] = f"{pu[:4]}…{pu[-4:]}"
        else:
            row["puuid_masked"] = None
        try:
            meta = dd.champion_display(int(p["champion_id"]))
        except (requests.RequestException, ValueError, OSError, TypeError):
            meta = None
        if meta:
            row["champion_name"] = meta["name"]
            row["champion_icon_url"] = meta["icon_url"]
        enriched.append(row)

    match_row = dict(detail["match"])
    match_row["ingested_at"] = utc_sqlite_to_eastern_display(
        match_row.get("ingested_at")
    )
    out = {
        "match": match_row,
        "participants": enriched,
        "timeline_row_count": detail.get("timeline_row_count", 0),
    }
    resp = jsonify(out)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.get("/api/gold-leaders")
def api_gold_leaders():
    try:
        champ_id, raw = _resolve_champion_query("champion")
    except (requests.RequestException, ValueError, OSError) as e:
        return (
            jsonify(
                {
                    "error": "Failed to load champion data from Data Dragon",
                    "detail": str(e),
                }
            ),
            502,
        )

    if not raw:
        return jsonify({"error": "champion query parameter is required"}), 400
    if champ_id is None:
        return jsonify({"error": f"Unknown champion: {raw!r}"}), 400

    try:
        min_games = int(request.args.get("min_games", "0"))
    except ValueError:
        min_games = 0
    min_games = max(0, min_games)

    lead_sort = (request.args.get("lead_sort") or "asc").strip().lower()
    if lead_sort not in ("asc", "desc"):
        return jsonify({"error": "lead_sort must be asc or desc"}), 400

    db_path = (current_app.config.get("MATCHUP_DB_PATH") or "").strip()
    if not db_path or not os.path.isfile(db_path):
        return jsonify({"error": "Database file not configured or missing"}), 404

    queue_id = current_app.config.get("MATCHUP_QUEUE_ID")
    payload = fetch_gold_leaders_at_15(
        db_path,
        champion_anchor=champ_id,
        queue_id=queue_id,
        min_games=min_games,
        lead_sort=lead_sort,
    )
    anchor_meta = dd.champion_display(champ_id)
    if anchor_meta:
        payload["champion_name"] = anchor_meta["name"]
        payload["champion_icon_url"] = anchor_meta["icon_url"]

    leaders = payload.get("leaders") or []
    for row in leaders:
        oid = row.get("opponent_id")
        if isinstance(oid, int):
            om = dd.champion_display(oid)
            if om:
                row["opponent_name"] = om["name"]
                row["opponent_icon_url"] = om["icon_url"]
    payload["leaders"] = leaders
    payload["min_games"] = min_games
    payload["lead_sort"] = lead_sort
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.get("/api/gold-curve")
def api_gold_curve():
    try:
        champ_a, raw_a = _resolve_champion_query("champion_a")
        champ_b, raw_b = _resolve_champion_query("champion_b")
    except (requests.RequestException, ValueError, OSError) as e:
        return (
            jsonify(
                {
                    "error": "Failed to load champion data from Data Dragon",
                    "detail": str(e),
                }
            ),
            502,
        )

    if not raw_a or not raw_b:
        return (
            jsonify(
                {
                    "error": (
                        "champion_a and champion_b are required (name or numeric id)"
                    ),
                }
            ),
            400,
        )
    if champ_a is None:
        return jsonify({"error": f"Unknown champion for champion_a: {raw_a!r}"}), 400
    if champ_b is None:
        return jsonify({"error": f"Unknown champion for champion_b: {raw_b!r}"}), 400

    mode = (request.args.get("mode") or "time").strip().lower()
    if mode not in ("time", "level"):
        return jsonify({"error": "mode must be time or level"}), 400

    db_path = (current_app.config.get("MATCHUP_DB_PATH") or "").strip()
    if not db_path or not os.path.isfile(db_path):
        return jsonify({"error": "Database file not configured or missing"}), 404

    queue_id = current_app.config.get("MATCHUP_QUEUE_ID")
    result = fetch_gold_curve(
        db_path,
        champion_a=champ_a,
        champion_b=champ_b,
        queue_id=queue_id,
        mode=mode,
    )
    if result.get("error"):
        return jsonify(result), 400

    meta_a = dd.champion_display(champ_a)
    meta_b = dd.champion_display(champ_b)
    result["champion_a"] = champ_a
    result["champion_b"] = champ_b
    if meta_a:
        result["champion_a_name"] = meta_a["name"]
        result["champion_a_icon_url"] = meta_a["icon_url"]
    if meta_b:
        result["champion_b_name"] = meta_b["name"]
        result["champion_b_icon_url"] = meta_b["icon_url"]

    for s in result.get("series") or []:
        if not isinstance(s, dict):
            continue
        k = s.get("key")
        if k == "anchor" and meta_a:
            s["name"] = meta_a["name"]
        elif k == "opponent" and meta_b:
            s["name"] = meta_b["name"]

    resp = jsonify(result)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.get("/api/matchup")
def api_matchup():
    try:
        champ_a, raw_a = _resolve_champion_query("champ_a")
        champ_b, raw_b = _resolve_champion_query("champ_b")
    except (requests.RequestException, ValueError, OSError) as e:
        return (
            jsonify(
                {
                    "error": "Failed to load champion data from Data Dragon",
                    "detail": str(e),
                }
            ),
            502,
        )

    if not raw_a or not raw_b:
        return (
            jsonify(
                {
                    "error": (
                        "champ_a and champ_b are required (champion name or "
                        "positive numeric id)"
                    ),
                }
            ),
            400,
        )
    if champ_a is None:
        return (
            jsonify({"error": f"Unknown champion for champ_a: {raw_a!r}"}),
            400,
        )
    if champ_b is None:
        return (
            jsonify({"error": f"Unknown champion for champ_b: {raw_b!r}"}),
            400,
        )

    db_path = (current_app.config.get("MATCHUP_DB_PATH") or "").strip()
    path_ok = bool(db_path) and os.path.isfile(db_path)
    live_fallback = bool(current_app.config.get("MATCHUP_LIVE_FALLBACK", True))

    key = (current_app.config.get("RIOT_API_KEY") or "").strip()

    if live_fallback:
        if not key and not path_ok:
            return jsonify({"error": "RIOT_API_KEY is not set"}), 503
    else:
        if not path_ok:
            return jsonify(
                {
                    "error": (
                        "MATCHUP_DB_PATH must point to an existing SQLite file when "
                        "MATCHUP_LIVE_FALLBACK is disabled (run collect_matches.py first)."
                    )
                }
            ), 400

    regional = (current_app.config.get("RIOT_REGIONAL_ROUTE") or "americas").strip()
    max_matches = int(current_app.config.get("MATCHUP_MAX_MATCHES") or 30)
    queue_id = current_app.config.get("MATCHUP_QUEUE_ID")

    plat = (current_app.config.get("RIOT_PLATFORM_ROUTE") or "").strip() or None

    try:
        client = RiotClient(key, regional, plat) if key else None
        seeds = resolve_matchup_seed_puuids(client)

        if live_fallback and not seeds:
            return jsonify(
                {
                    "error": (
                        "No seed PUUIDs for live fallback: set MATCHUP_SEED_PUUIDS or puuids.txt, "
                        "and/or MATCHUP_LADDER_SEEDS=1 with RIOT_PLATFORM_ROUTE, then restart."
                    )
                }
            ), 400

        result = compute_matchup_stats_hybrid(
            client,
            db_path=db_path or None,
            seed_puuids=seeds,
            champion_a=champ_a,
            champion_b=champ_b,
            max_match_fetches=max_matches,
            queue_id=queue_id,
            live_fallback=live_fallback,
        )

        if result.get("error"):
            err = result["error"]
            if isinstance(err, str) and (
                "RIOT_API_KEY" in err or "not configured" in err
            ):
                return jsonify(result), 503
            return jsonify(result), 400

        result["champion_a"] = champ_a
        result["champion_b"] = champ_b
        meta_a = dd.champion_display(champ_a)
        meta_b = dd.champion_display(champ_b)
        if meta_a:
            result["champion_a_name"] = meta_a["name"]
            result["champion_a_icon"] = meta_a["icon_url"]
        if meta_b:
            result["champion_b_name"] = meta_b["name"]
            result["champion_b_icon"] = meta_b["icon_url"]

        return jsonify(result)
    except RiotAPIError as e:
        return (
            jsonify(
                {
                    "error": "Riot API request failed",
                    "status_code": e.status_code,
                    "detail": e.message,
                }
            ),
            502,
        )
    except (requests.RequestException, ValueError, OSError) as e:
        return (
            jsonify(
                {
                    "error": "Network or data source error",
                    "detail": str(e),
                }
            ),
            502,
        )
    except Exception as e:
        current_app.logger.exception("api_matchup")
        return (
            jsonify(
                {
                    "error": "Unexpected server error",
                    "detail": str(e),
                }
            ),
            500,
        )
