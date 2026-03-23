from __future__ import annotations

import os
import sqlite3

import requests
from flask import Blueprint, current_app, jsonify, render_template, request

from app.db.store import fetch_db_summary, fetch_matches_page
from app.riot.client import RiotAPIError, RiotClient
from app.services import datadragon as dd
from app.services.matchup import compute_matchup_stats_hybrid
from app.services.seed_puuids import resolve_matchup_seed_puuids

bp = Blueprint("main", __name__)

_DB_MATCHES_MAX_LIMIT = 100
_DB_MATCHES_DEFAULT_LIMIT = 25


def _resolve_champion_query(param: str) -> tuple[int | None, str]:
    raw = (request.args.get(param) or "").strip()
    if not raw:
        return None, raw
    cid = dd.resolve_champion_id(raw)
    return cid, raw


@bp.get("/")
def index():
    return render_template("index.html")


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
    return jsonify(summary)


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
    return jsonify({"limit": limit, "offset": offset, "matches": rows})


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

    try:
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
