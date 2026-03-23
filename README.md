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

   Open [http://127.0.0.1:5000](http://127.0.0.1:5000). Enter **numeric champion IDs** (e.g. from Data Dragon) for A and B, then submit.

## API (JSON)

`GET /api/matchup?champ_a=<id>&champ_b=<id>`

Returns counts, optional `winrate`, `sample_size_warning`, and metadata. Errors return JSON with an `error` field.

## Optional env

- `MATCHUP_MAX_MATCHES` — cap on match-detail requests per call (default `30`).
- `MATCHUP_QUEUE_ID` — e.g. `420` for ranked solo; omit for all queues from the matchlist.

## Project layout

- [app/](app/) — Flask app, Riot client, matchup aggregation
- [templates/](templates/) — Jinja HTML
- [static/](static/) — CSS and JS
