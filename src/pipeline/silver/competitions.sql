-- Silver: one row per competition, in its latest known state, with typed columns.
-- Every run lands the full list again, so AUTO CDC keeps the version from the latest fetch.

CREATE TEMPORARY VIEW competitions_typed AS
SELECT
  competition:id::bigint                         AS competition_id,
  competition:code::string                       AS competition_code,
  competition:name::string                       AS competition_name,
  competition:type::string                       AS competition_type,  -- LEAGUE, CUP
  competition:area.name::string                  AS area_name,
  competition:area.code::string                  AS area_code,
  competition:plan::string                       AS plan,
  competition:currentSeason.id::bigint           AS current_season_id,
  competition:currentSeason.startDate::date      AS current_season_start_date,
  competition:currentSeason.endDate::date        AS current_season_end_date,
  competition:currentSeason.currentMatchday::int AS current_matchday,
  competition:lastUpdated::timestamp             AS last_updated,
  fetched_at
FROM STREAM(competitions);

CREATE OR REFRESH STREAMING TABLE ${silver_schema}.competitions
COMMENT 'One row per competition in its latest known state, typed. Kept current by AUTO CDC from bronze.competitions.';

CREATE FLOW competitions_latest AS AUTO CDC INTO ${silver_schema}.competitions
FROM STREAM(competitions_typed)
KEYS (competition_id)
SEQUENCE BY fetched_at
STORED AS SCD TYPE 1;
