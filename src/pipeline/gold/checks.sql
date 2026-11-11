-- Gold checks that need more than one row: one row per key in what consumers read.
--
-- Expectations check one row at a time, and views (gold.matches, gold.teams) can't carry them at
-- all. So this private materialized view counts the rows per key, and its expectation fails the
-- update, and with it the job, as soon as a key appears twice. That's what a join fan-out looks
-- like: a duplicate competition in silver would duplicate every one of its matches here.
--
-- Silver needs no such check: AUTO CDC keeps exactly one row per key by construction.
-- Private: it lives inside the pipeline and isn't published to the catalog.

CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_keys (
  CONSTRAINT one_row_per_key EXPECT (rows_per_key = 1) ON VIOLATION FAIL UPDATE
)
COMMENT 'Rows per key in the gold views and tables. Fails the update when a key appears twice.'
AS
SELECT 'matches' AS dataset, cast(match_id AS STRING) AS key, count(*) AS rows_per_key
FROM ${gold_schema}.matches
GROUP BY match_id
UNION ALL
SELECT 'teams', cast(team_id AS STRING), count(*)
FROM ${gold_schema}.teams
GROUP BY team_id
UNION ALL
SELECT 'standings', concat_ws('/', competition_code, season_id, team_id), count(*)
FROM ${gold_schema}.standings
GROUP BY competition_code, season_id, team_id
UNION ALL
SELECT 'home_advantage', concat_ws('/', competition_code, season, team_id), count(*)
FROM ${gold_schema}.home_advantage
GROUP BY competition_code, season, team_id;
