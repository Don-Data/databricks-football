# databricks-football

A football data pipeline built entirely in Databricks, on the free Databricks Free Edition: ingestion, a medallion lakehouse, orchestration, data tests and an analysis dashboard. Everything is defined as code, deployed with a Declarative Automation Bundle, and prod is deployed by GitHub Actions.

```
football-data.org API -> landing volume (raw JSON) -> bronze (VARIANT) -> silver (typed, latest version) -> gold (standings, matches, teams, home advantage) -> dashboard
```

**Status:** complete. It was built step by step, and the commit history is the build log.

## What's in the repo

| Path | What it is |
|---|---|
| `databricks.yml` | The bundle: variables, the `setup` and `grant_prod_access` scripts, and the `dev` and `prod` targets |
| `setup/catalog.sql`, `setup/run_sql.py` | The catalog, the one object a bundle resource can't create on Free Edition, created with SQL on the SQL warehouse by `databricks bundle run setup` |
| `setup/grant_prod_access.py` | The one-time grants the prod service principal needs before its first deploy (catalog, secret scope); called by `databricks bundle run grant_prod_access -t prod` |
| `resources/ingest.job.yml` | The Lakeflow Job: `extract_competitions` → `extract_matches` → `extract_standings` → `run_pipeline` on serverless, daily at 06:00 Europe/Berlin, with an email on failure |
| `src/football/ingest.py` | The extract logic: API calls via dlt's retrying `requests`, which days to fetch (derived from the landing), 10-day date windows, the competition drift check, the published league tables, landing |
| `src/football/tracked_competitions.yml` | What gets ingested: `history_start` and which competitions are tracked or deliberately ignored |
| `resources/landing.schema.yml`, `resources/api_responses.volume.yml` | The `landing` schema and its volume: raw API responses, one JSON envelope per request. Protected in prod (see [Starting over](#starting-over)); in dev, `bundle destroy` deletes it with every landed file |
| `resources/football.pipeline.yml` | The Spark Declarative Pipeline (serverless, triggered by the job) |
| `resources/bronze.schema.yml`, `silver.schema.yml`, `gold.schema.yml` | The `bronze`, `silver` and `gold` schemas. Gold is what consumers read |
| `src/pipeline/bronze/*.sql` | Bronze streaming tables: Auto Loader (`read_files`) reads every landed file as `VARIANT`: one row per match or competition per fetch, and one per published league table |
| `src/pipeline/silver/*.sql` | Silver: typed columns, and AUTO CDC keeps one row per match, competition, team and team in a published table, the version from the latest fetch (SCD Type 1). Teams are derived from the teams named in matches |
| `src/pipeline/gold/standings.sql` | The league table per competition and season (materialized view), only for seasons that are complete in the data |
| `src/pipeline/gold/matches.sql`, `teams.sql` | Every match, with the game's score and winner (a penalty shootout's goals subtracted), and the team dimension. Views: no storage, always current |
| `src/pipeline/gold/home_advantage.sql` | Every team's home and away record per competition and season (a view, counts only), from finished matches with a real home side |
| `src/pipeline/gold/standings_check.sql` | Our league table next to the one football-data.org publishes, per team, with three warning expectations (see [Checking gold against the API](#checking-gold-against-the-api)) |
| `src/pipeline/gold/checks.sql` | `gold_keys`: a private materialized view that fails the update when a key appears twice in gold |
| `resources/home_advantage.dashboard.yml`, `src/dashboards/home_advantage.lvdash.json` | The AI/BI dashboard on `gold.home_advantage`: how much more teams win at home, by competition and by team |
| `tests/test_ingest.py` | Unit tests for the fetch-range rules, date windows, the drift check, which competitions get a published table, and landing (no API, no Databricks) |
| `pyproject.toml`, `uv.lock` | Python dependencies (uv) and the pytest configuration |
| `.github/workflows/ci.yml` | CI/CD: the tests and a lint on every push and pull request, without secrets; after a merge to `main`, the prod deploy |
| `.gitattributes` | LF line endings everywhere, whatever a contributor's Git settings are |

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
```

Prod is deployed by GitHub Actions after a merge to `main` (see [CI/CD](#cicd)). Don't deploy it from a laptop. The bundle makes the deployer the owner of the job and the pipeline, and the service principal can only deploy what it owns.

**Which days a run fetches.** There's no separate watermark. Every matches file name carries the window it requested, for example `20261110T143558_2026-11-07_2026-11-17_df315d6c.json`, so a run sees from the file names alone which days are settled. A day is settled once a request covered it after it had ended. A run fetches from the first unsettled day since `history_start`, always at least 3 days back for late corrections, up to 6 days ahead. So the first run is the full load, a normal day is a single request, a run after an outage catches up by itself, and a deleted file is simply fetched again.

## Tests

```
uv run pytest
```

GitHub Actions runs the same tests and a lint on every push and pull request (`.github/workflows/ci.yml`). `bundle deploy` doesn't run them, so run them before deploying dev.

**Data tests run inside the pipeline**, as expectations with `ON VIOLATION FAIL UPDATE`. A violation stops the update, and with it the daily job, which sends the failure email:
- bronze: every match and competition has an id, and every published table names its competition and season, because silver keys on them;
- `gold.standings`: every game counts as exactly one of won, drawn or lost;
- `gold_keys` (`src/pipeline/gold/checks.sql`, private): one row per key in `gold.matches`, `gold.teams`, `gold.standings`, `gold.home_advantage` and `gold.standings_check`. Views can't carry expectations, so this materialized view counts the rows per key.

Silver needs no uniqueness check, because AUTO CDC keeps one row per key by construction. Expectations check rows as they're written, not the whole table on every run: each bronze row is checked once, on its way in. A `FAIL UPDATE` expectation that passes reports no metrics in the pipeline's *Data quality* tab. The only sign is the update completing.

## Home advantage

`gold.home_advantage` holds every team's home and away record per competition and season. It covers league matches of the regular season, and cup matches except finals, which are played at a neutral venue. A match decided on penalties counts as the draw it was: football-data.org's full-time score includes the penalties, so the view subtracts them. The view holds counts only, so it adds up across teams and seasons. Compute ratios as ratios of sums.

The dashboard `Home advantage` sits on top of it. The deploy publishes it, and viewers query with their own permissions:

```
databricks bundle open home_advantage -t dev
```

As of 2026-11-11, over 4,803 matches:
- The home team took 57.7% of the points, where 50% would mean no home advantage.
- Brazil's Série A is highest (64%), and Italy's Serie A is lowest (53%).
- Not one of the 40 Brazilian team-seasons with at least 10 home and 10 away games did better away. In Serie A 2025/26, 6 of 20 teams did.

## Checking gold against the API

`gold.standings` computes every table itself, from the matches. football-data.org also publishes its tables, and the ingest job lands them on every run, for the current season of every tracked league (`extract_standings`). `gold.standings_check` puts the two side by side, team by team, with three warning expectations:

| Expectation | A failure means |
|---|---|
| `same_games` | Played, won, drawn, lost or goals differ: our matches and the API's disagree, or the API's table is wrong |
| `same_points` | The same games but other points: a point deduction, which isn't in the match data |
| `same_position` | The same points but another position: a tie-break. `standings.sql` ranks ties by goal difference and goals only, and tied teams share a position |

They're warnings, not failures: the API's table can be wrong, and that mustn't stop the pipeline. Their pass rates are in the pipeline's *Data quality* tab after every update, and the rows behind them are in `gold.standings_check`, for example:

```sql
SELECT * FROM football.gold.standings_check WHERE NOT (ours_position <=> published_position)
```

The first version of this check was a script run by hand on 2026-11-11, and it also compared the complete 2025/26 seasons. 16 of 17 tables were identical down to every position. In Ligue 1 2025/26, football-data.org's own table leaves out Nantes vs Toulouse. That match was abandoned at 0-0, and the LFP confirmed the 0-0, so gold is right.

The first run of this version, in dev on 2026-11-11, compared 332 teams in 17 tables. Dev still has the 2025/26 tables the script landed; prod only gets the current seasons.

| Expectation | Passed | The failures |
|---|---|---|
| `same_games` | 330 of 332 | Toulouse and Nantes, Ligue 1 2025/26: the abandoned match |
| `same_points` | 329 of 332 | The same two, and Southampton, Championship 2026/27: the same games, 4 points fewer. That's the deduction an independent disciplinary commission imposed in May 2026 for unauthorised filming of other clubs' training |
| `same_position` | 324 of 332 | The knock-on effects of both, and a tie in the Brasileirão 2026. Vitória and Internacional both have 38 points; the API ranks Vitória ahead on wins (10 to 9), Brazil's first tie-break, while `standings.sql` ranks Internacional ahead on goal difference |

## CI/CD

After a merge to `main`, the `deploy` job in `.github/workflows/ci.yml` runs `databricks bundle deploy -t prod`, once the tests have passed. It logs in as the service principal `github-actions-football` with an OAuth secret (OAuth M2M). Free Edition has no account console, so workload identity federation (OIDC, no stored secret) isn't available there.

The secrets live in the GitHub environment `prod`, and only `main` may use it. Pull requests, and forks above all, never see them.

**One-time setup** (as a workspace admin, after [Setup](#setup)). Prod is never deployed by a person. The service principal creates every prod object on its first deploy, so it owns all of them.

1. Create the service principal:
   ```
   databricks service-principals create --display-name github-actions-football
   ```
2. Give it what exists before its first deploy: `USE CATALOG` and `CREATE SCHEMA` on the catalog, and `READ` on the `football` secret scope:
   ```
   databricks bundle run grant_prod_access -t prod
   ```
3. In GitHub, create the environment `prod`, with `main` as its only deployment branch. Then add these secrets to it:
   - `DATABRICKS_HOST`: the workspace URL
   - `DATABRICKS_CLIENT_ID`: the service principal's application ID
   - `DATABRICKS_CLIENT_SECRET`: from `databricks service-principal-secrets-proxy create <service-principal-id> --lifetime 31536000s` (one year)
   - `OPERATOR_EMAIL`: who gets failure emails, CAN_MANAGE and read access in prod.
     - It must be exactly a Databricks login, as `databricks current-user me` shows it under `userName`.
     - Any other address fails the deploy with "Principal: UserName(\*\*\*) does not exist".
4. Deploy with *Run workflow* (`workflow_dispatch`) on `main`, then start the job once. That first run is the full load.

**Taking over a prod that a person deployed by hand is harder.** The job can be handed over, but "Only metastore admins can change pipeline owner", and on Free Edition the metastore belongs to Databricks. This repo did that once: [PR #5](https://github.com/Don-Data/databricks-football-trial/pull/5) and the README at that commit describe the steps.

**Keep in mind:** Databricks deletes an OAuth secret after 90 days without use. If nothing has been merged for that long, create a new secret, replace `DATABRICKS_CLIENT_SECRET`, and redeploy with *Run workflow* (`workflow_dispatch`).

## Reloads

Neither reload deletes a landed file. Both work in prod too, with `-t prod`: the run happens as the service principal, and the operator may start it because the prod target gives them CAN_MANAGE.

- **Rebuild the tables**, for example after a fix in `src/pipeline/`. A full refresh reads every landed file again, without an API call:
  ```
  databricks bundle run football -t dev --full-refresh-all
  ```
- **Fetch a period again**, for example when the API corrected results. The new files go next to the old ones, and silver takes the latest fetch:
  ```
  databricks bundle run ingest -t dev --params date_from=2026-08-01,date_to=2026-08-31
  ```

## Starting over

`databricks bundle destroy -t dev` deletes the dev job, the pipeline, the schemas with all their tables, and the landing volume with every landed file. The catalog, the secret scope and the service principal stay, because they aren't bundle resources. `databricks bundle deploy -t dev` and `databricks bundle run ingest -t dev` then build everything again, starting with a full load.

Prod's landing volume is protected. The landed files are the one thing in prod a new fetch can't bring back, because it only gets what the API says today. So the prod target sets `lifecycle.prevent_destroy` on the volume, and any `bundle destroy` or deploy that would delete or recreate it stops with "has lifecycle.prevent_destroy set". Renaming the volume is an in-place update and keeps the files. Moving it to another schema or catalog is a recreate.

To start prod over on purpose, from `main`, as an admin:

1. In `databricks.yml`, set `prevent_destroy` to `false` under the prod target. Don't commit it: the check reads the local configuration.
2. `databricks bundle destroy -t prod`
3. `git restore databricks.yml`, then step 4 of the one-time setup in [CI/CD](#cicd).

## License and data

Code: MIT (see `LICENSE`). The football data comes from football-data.org and is subject to its terms; none of it is in this repo.

Football data provided by the Football-Data.org API.
