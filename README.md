# teamdiff

Find lane matchup edges from sampled match history (local prototype).

## What it does

This is a small **Flask** app with a plain **HTML/CSS/JS** front end. The browser never sees your Riot API key. The server pulls recent **Match-V5** data for one or more **seed PUUIDs**, then estimates **how often champion A wins when A and B are on opposite teams** in those games.

**Limitations:** Riot does not offer “random match” search. You must supply PUUIDs whose games overlap the matchup you care about, so the estimate is only as good as that sample. Development keys have **tight rate limits**; keep `MATCHUP_MAX_MATCHES` modest.

## Setup

1. Create a virtual environment and install dependencies:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Copy [.env.example](.env.example) to `.env` and fill in:

   - `RIOT_API_KEY` — from the [Riot Developer Portal](https://developer.riotgames.com/).
   - `RIOT_REGIONAL_ROUTE` — routing cluster for Match-V5 (`americas`, `europe`, `asia`, `sea`).
   - `MATCHUP_SEED_PUUIDS` — comma-separated PUUIDs (no spaces between entries is simplest). **Put each value on the same line as the key** (`MATCHUP_SEED_PUUIDS=yourIdHere`) with no blank line after `=`; otherwise the app sees an empty list. Save `.env` and restart the server after edits.
   - **Or** use a root-level `puuids.txt` (see [puuids.txt.example](puuids.txt.example)): one PUUID per line, file is gitignored. If `MATCHUP_SEED_PUUIDS` is empty after loading `.env`, the app reads `puuids.txt` automatically when that file exists.

3. Run locally:

   ```bash
   python run.py
   ```

   Open [http://127.0.0.1:5000](http://127.0.0.1:5000). Use champion **names or numeric IDs** on the Matchup and Gold lanes pages.

## Operations (run, collect, save)

**Save configuration:** Edit the project root `.env` (start from [.env.example](.env.example)), save the file, then restart any running processes (`python run.py` or `python collect_matches.py`) so they reload environment variables. The `.env` file is gitignored; keep your own backup if you rely on it.

**Run the web app:**

```bash
source .venv/bin/activate
python run.py
```

Then open [http://127.0.0.1:5000](http://127.0.0.1:5000).

**Run the match collector** (long-running worker; needs `RIOT_API_KEY` and `MATCHUP_DB_PATH` in `.env`):

```bash
source .venv/bin/activate
python collect_matches.py
```

Use the **same** `.env` for both the server and the collector so `MATCHUP_QUEUE_ID`, seed PUUIDs, and DB path stay aligned.

**Save match data:** All ingested games live in the SQLite file pointed to by `MATCHUP_DB_PATH` (for example `data/matchups.db`). Copy or back up that file to preserve your dataset.

**Run tests:**

```bash
source .venv/bin/activate
pytest -q
```

## Collector vs app features

[`collect_matches.py`](collect_matches.py) fetches match lists for seed PUUIDs, stores each new match in SQLite (`matches`, `participants` with Riot participant id and lane), then ingests timelines into `participant_timeline`.

| UI / API | Needs in the database |
| --- | --- |
| Gold lanes & gold curves (`/`, `/api/gold-leaders`, `/api/gold-curve`) | Timeline rows, `participant_id`, and `team_position` on participants (same-lane pairs). |
| Matchup (`/matchup`, `/api/matchup`) | `matches` + `participants` only; optional `MATCHUP_QUEUE_ID` filter. |
| Database inspector (`/database`, `/api/db/*`) | Same SQLite file; ingest times are shown in **US Eastern** time in the API responses used by the UI. |

The `/api/db/summary` payload includes `gold_features_ready` when the file has timeline data and lane metadata filled in—use the Database page for a quick health check.

## API (JSON)

`GET /api/matchup?champ_a=<id>&champ_b=<id>`

Returns counts, optional `winrate`, `sample_size_warning`, and metadata. Errors return JSON with an `error` field.

## Optional env

- `MATCHUP_MAX_MATCHES` — cap on match-detail requests per call (default `30`).
- `MATCHUP_QUEUE_ID` — e.g. `420` for ranked solo; omit for all queues from the matchlist.
- `MATCHUP_DB_PATH` — path to the SQLite file used by Gold lanes, Database, and DB-backed matchup stats.

## Project layout

- [app/](app/) — Flask app, Riot client, matchup aggregation
- [templates/](templates/) — Jinja HTML
- [static/](static/) — CSS and JS
- [tests/](tests/) — pytest suite
