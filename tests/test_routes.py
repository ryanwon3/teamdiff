from __future__ import annotations

from unittest.mock import patch

from app import create_app


def test_index_ok(client) -> None:
    res = client.get("/")
    assert res.status_code == 200


def test_database_page_ok(client) -> None:
    res = client.get("/database")
    assert res.status_code == 200


def test_api_db_summary_missing_db(tmp_path) -> None:
    app = create_app()
    app.config.update(
        TESTING=True,
        MATCHUP_DB_PATH=str(tmp_path / "does_not_exist.db"),
    )
    res = app.test_client().get("/api/db/summary")
    assert res.status_code == 404
    data = res.get_json()
    assert data and "error" in data


def test_api_db_summary_eastern_and_ready_flag(client_with_lane_data) -> None:
    res = client_with_lane_data.get("/api/db/summary")
    assert res.status_code == 200
    data = res.get_json()
    assert data["gold_features_ready"] is True
    assert data["db_path_configured"] is True
    mini = data.get("ingested_at_min") or ""
    assert "EST" in mini or "EDT" in mini


def test_api_db_matches_formats_ingested(client_with_lane_data) -> None:
    res = client_with_lane_data.get("/api/db/matches?limit=5")
    assert res.status_code == 200
    data = res.get_json()
    row = data["matches"][0]
    ing = row.get("ingested_at") or ""
    assert "EST" in ing or "EDT" in ing


def test_api_db_match_detail_formats_ingested(app_with_db, temp_db_path) -> None:
    from tests.conftest import seed_lane_gold_fixture

    seed_lane_gold_fixture(temp_db_path)
    cli = app_with_db.test_client()
    res = cli.get("/api/db/matches/NA1_TEST_LANE")
    assert res.status_code == 200
    data = res.get_json()
    ing = (data.get("match") or {}).get("ingested_at") or ""
    assert "EST" in ing or "EDT" in ing


def test_api_champions_mocked(client) -> None:
    fake = {"version": "1", "champions": [{"id": 1, "name": "A", "icon_url": ""}]}
    with patch("app.routes.dd.list_champions_for_api", return_value=fake):
        res = client.get("/api/champions")
    assert res.status_code == 200
    assert res.get_json() == fake
