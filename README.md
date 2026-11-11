# databricks-football

A football data pipeline built entirely in Databricks, on the free Databricks Free Edition: ingestion, a medallion lakehouse, orchestration and analysis, all defined as code and deployed with a Declarative Automation Bundle.

**Status:** work in progress. It's built step by step, and the commit history is the build log.

## What exists so far

| Path | What it is |
|---|---|
| `databricks.yml` | The bundle: variables, the `setup` script, and the `dev` and `prod` targets |
| `setup/catalog.sql` | The catalog. It's the one object a bundle resource can't create on Free Edition |
| `setup/run_sql.py` | Runs a SQL file on the SQL warehouse; called by `databricks bundle run setup` |
| `setup/grant_prod_access.py` | The one-time access grants and the ownership transfer for the prod service principal; called by `databricks bundle run grant_prod_access -t prod` |
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
| `.github/workflows/ci.yml` | CI/CD: the tests and a lint on every push and pull request, without secrets; after a merge to `main`, the prod deploy |

**Two targets in the same workspace.** `dev` is for building: names get a `[dev <user>]` or `dev_<user>_` prefix, and the schedule is paused. `prod` is the real daily run, with unprefixed schemas `landing`, `bronze`, `silver` and `gold`. Each target has its own landing volume, so the first run of each is a full load. `prod` deploys only from `main`: changes go in through a pull request, and the prod target names `main` as its branch, so `bundle deploy -t prod` from any other branch stops with "not on the right Git branch". Prod is deployed by the service principal `github-actions-football`, and the job and pipeline run as it.

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
```

Prod is deployed by GitHub Actions after a merge to `main` (see [CI/CD](#cicd)). Don't deploy it from a laptop. The bundle makes the deployer the owner of the job and the pipeline, and the service principal can only deploy what it owns.

**Which days a run fetches.** There's no separate watermark. Every matches file name carries the window it requested, for example `20261110T143558_2026-11-07_2026-11-17_df315d6c.json`, so a run sees from the file names alone which days are settled. A day is settled once a request covered it after it had ended. A run fetches from the first unsettled day since `history_start`, always at least 3 days back for late corrections, up to 6 days ahead. So the first run is the full load, a normal day is a single request, a run after an outage catches up by itself, and a deleted file is simply fetched again.

## Tests

```
uv run pytest
```

GitHub Actions runs the same tests and a lint on every push and pull request (`.github/workflows/ci.yml`). `bundle deploy` doesn't run them, so run them before deploying dev.

## CI/CD

After a merge to `main`, the `deploy` job in `.github/workflows/ci.yml` runs `databricks bundle deploy -t prod`, once the tests have passed. It logs in as the service principal `github-actions-football` with an OAuth secret (OAuth M2M). Free Edition has no account console, so workload identity federation (OIDC, no stored secret) isn't available there.

The secrets live in the GitHub environment `prod`, and only `main` may use it. Pull requests, and forks above all, never see them.

**One-time setup** (as a workspace admin). This is how this repo moved its existing prod deployment, which a person had deployed by hand. Starting without one isn't tested here.

1. Create the service principal:
   ```
   databricks service-principals create --display-name github-actions-football
   ```
2. In GitHub, create the environment `prod`, with `main` as its only deployment branch. Then add these secrets to it:
   - `DATABRICKS_HOST`: the workspace URL
   - `DATABRICKS_CLIENT_ID`: the service principal's application ID
   - `DATABRICKS_CLIENT_SECRET`: from `databricks service-principal-secrets-proxy create <service-principal-id> --lifetime 31536000s` (one year)
   - `OPERATOR_EMAIL`: who gets failure emails and CAN_MANAGE in prod.
     - It must be exactly a Databricks login, as `databricks current-user me` shows it under `userName`.
     - Any other address fails the deploy with "Principal: UserName(\*\*\*) does not exist".
3. Move the job, the schemas and the volume into the service principal's state with `databricks bundle deployment bind <key> <id> -t prod`.
   - Without this, the first deploy from CI starts with an empty state and creates everything a second time.
   - Check it with `databricks bundle plan -t prod`. Delete `.databricks/bundle/prod` locally first, or the plan reads your local cache and shows updates even when the remote state is empty.
4. Run `databricks bundle run grant_prod_access -t prod`. It grants the service principal its access, gives you read access to the prod schemas, and transfers the job to the service principal.
5. Replace the pipeline, because it can't be handed over. "Only metastore admins can change pipeline owner", and on Free Edition the metastore belongs to Databricks. So the service principal creates the pipeline anew:
   - Delete the old pipeline with `databricks pipelines delete <pipeline-id>`.
   - Drop its tables. They're kept after the delete, but "cannot be updated until moved into an active pipeline". Moving them needs one identity that owns both pipelines.
     - List them all before dropping, so none is forgotten: `SELECT table_schema, table_name, table_type FROM football.information_schema.tables WHERE table_schema IN ('bronze', 'silver', 'gold') AND table_name NOT LIKE '\_\_%' AND table_name NOT LIKE 'event\_log%'`.
     - A forgotten one fails the new pipeline with "is already managed by pipeline …".
     - The filter hides the pipeline's internal tables (`__materialization_…`, `event_log_…`). Leave those alone.
6. Deploy straight away, by merging or with *Run workflow*, then start the job once. Bronze rebuilds itself from the landing volume, and silver and gold follow.
   - From step 4 until this deploy, prod is down: the job runs as the service principal but still points to the old files. Do steps 4 to 6 in one go.

**Keep in mind:** Databricks deletes an OAuth secret after 90 days without use. If nothing has been merged for that long, create a new secret, replace `DATABRICKS_CLIENT_SECRET`, and redeploy with *Run workflow* (`workflow_dispatch`).

## License and data

Code: MIT (see `LICENSE`). The football data comes from football-data.org and is subject to its terms; none of it is in this repo.

Football data provided by the Football-Data.org API.
