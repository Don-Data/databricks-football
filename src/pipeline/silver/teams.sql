-- Silver: one row per team, in its latest known state.
--
-- The API has no team feed in what we land, but every match names both teams, so the team
-- list is derived from bronze.matches: home and away team of every fetched match, one stream.
-- A team appears in many matches of the same fetch with identical data, so the match id breaks
-- the tie and keeps the result deterministic.

CREATE TEMPORARY VIEW teams_typed AS
SELECT
  team:id::bigint          AS team_id,
  team:name::string        AS team_name,
  team:shortName::string   AS team_short_name,
  team:tla::string         AS team_tla,          -- three-letter code, e.g. FCB
  team:crest::string       AS crest_url,
  match_id,
  fetched_at
FROM (
  SELECT explode(array(match:homeTeam, match:awayTeam)) AS team, match:id::bigint AS match_id, fetched_at
  FROM STREAM(matches)
)
WHERE team:id IS NOT NULL;                       -- a knockout fixture can name an undecided opponent

CREATE OR REFRESH STREAMING TABLE ${silver_schema}.teams
COMMENT 'One row per team in its latest known state, derived from the teams named in bronze.matches.';

CREATE FLOW teams_latest AS AUTO CDC INTO ${silver_schema}.teams
FROM STREAM(teams_typed)
KEYS (team_id)
SEQUENCE BY STRUCT(fetched_at, match_id)
COLUMNS * EXCEPT (match_id)
STORED AS SCD TYPE 1;
