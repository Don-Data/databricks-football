# databricks-football

A football data pipeline built entirely in Databricks, on the free Databricks Free Edition: ingestion, a medallion lakehouse, orchestration and analysis, all defined as code and deployed with a Declarative Automation Bundle.

**Status:** work in progress. It's built step by step, and the commit history is the build log.

## What exists so far

| Path | What it is |
|---|---|
| `databricks.yml` | The bundle: variables, the `setup` script, and the `dev` and `prod` targets |
| `setup/catalog.sql` | The catalog. It's the one object a bundle resource can't create on Free Edition |
| `setup/run_sql.py` | Runs a SQL file on the SQL warehouse; called by `databricks bundle run setup` |
| `resources/landing.schema.yml` | The `landing` schema (bronze, silver and gold get their own later) |
| `resources/api_responses.volume.yml` | The landing volume: raw API responses, one JSON envelope per request, protected against `bundle destroy` |
| `resources/ingest.job.yml` | The Lakeflow Job: `extract_competitions` → `extract_matches` → `run_pipeline` on serverless, daily at 06:00 Europe/Berlin, with an email on failure |
| `resources/bronze.schema.yml` | The `bronze` schema |
| `resources/football.pipeline.yml` | The Spark Declarative Pipeline (serverless, triggered by the job) |
| `src/pipeline/bronze/*.sql` | Bronze streaming tables: Auto Loader (`read_files`) reads every landed file as `VARIANT`, one row per match or competition per fetch |
| `resources/silver.schema.yml` | The `silver` schema |
| `src/pipeline/silver/*.sql` | Silver: typed columns, and AUTO CDC keeps one row per match, competition and team, the version from the latest fetch (SCD Type 1). Teams are derived from the teams named in matches |
| `resources/gold.schema.yml` | The `gold` schema: what consumers read |
| `src/pipeline/gold/standings.sql` | The league table per competition and season (materialized view), only for seasons that are complete in the data |
| `src/pipeline/gold/matches.sql`, `teams.sql` | Every match, and the team dimension (views: 1:1 projections of silver, no storage, always current) |
| `src/football/ingest.py` | The extract logic: API calls via dlt's retrying `requests`, which days to fetch (derived from the landing), 10-day date windows, the competition drift check, landing |
| `src/football/tracked_competitions.yml` | What gets ingested: `history_start` and which competitions are tracked or deliberately ignored |
| `tests/test_ingest.py` | Unit tests for the fetch-range rules, date windows, the drift check and landing (no API, no Databricks) |

**Two targets in the same workspace.** `dev` is for building: names get a `[dev <user>]` or `dev_<user>_` prefix, and the schedule is paused. `prod` is the real daily run, with unprefixed schemas `landing`, `bronze`, `silver` and `gold`. Each target has its own landing volume, so the first run of each is a full load.

## Setup

1. A Databricks CLI profile named `free-edition` that points to your workspace:
   `databricks auth login --host <your-workspace-url> --profile free-edition`
2. A football-data.org API token (the free tier is enough), stored as a secret:
   ```
   databricks secrets create-scope football --profile free-edition
   databricks secrets put-secret football fd_token --profile free-edition
   ```
3. The catalog, created once (it's safe to run again):
   ```
   databricks bundle run setup -t dev
   ```
   Free Edition keeps every catalog on Default Storage, and the Unity Catalog API refuses to create those. Bundle resources and Terraform both go through that API, so `setup/catalog.sql` creates the catalog with SQL on the Serverless Starter Warehouse instead. Everything inside the catalog is a normal bundle resource. To use another catalog name, add `--var catalog=<name>` to every bundle command.

## Deploy and run

```
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run ingest -t dev                                    # fetches whatever isn't settled yet
databricks bundle run ingest -t dev --params date_from=2026-08-01,date_to=2026-08-31   # re-fetch a period on purpose

databricks bundle deploy -t prod                                       # the scheduled daily run
```

**Which days a run fetches.** There's no separate watermark. Every matches file name carries the window it requested, for example `20261110T143558_2026-11-07_2026-11-17_df315d6c.json`, so a run sees from the file names alone which days are settled. A day is settled once a request covered it after it had ended. A run fetches from the first unsettled day since `history_start`, always at least 3 days back for late corrections, up to 6 days ahead. So the first run is the full load, a normal day is a single request, a run after an outage catches up by itself, and a deleted file is simply fetched again.

## Tests

```
uv run pytest tests
```

## Data

Football data provided by the Football-Data.org API.
