-- Silver: one row per match, in its latest known state, with typed columns.
--
-- Bronze holds every fetch of a match: daily runs re-fetch recent days, and a kick-off at
-- 00:00 UTC arrives in two windows. AUTO CDC keeps one row per match_id, and the version
-- from the latest fetch wins (SCD Type 1). A fixture turns into a result in place.

CREATE TEMPORARY VIEW matches_typed AS
SELECT
  match:id::bigint                AS match_id,
  match:competition.id::bigint    AS competition_id,
  match:competition.code::string  AS competition_code,
  match:season.id::bigint         AS season_id,
  match:season.startDate::date    AS season_start_date,
  match:season.endDate::date      AS season_end_date,
  match:matchday::int             AS matchday,        -- empty for knockout rounds
  match:stage::string             AS stage,
  match:['group']::string         AS match_group,     -- group stages only
  match:utcDate::timestamp        AS kickoff_utc,
  match:status::string            AS status,
  match:homeTeam.id::bigint       AS home_team_id,
  match:homeTeam.name::string     AS home_team_name,
  match:homeTeam.shortName::string AS home_team_short_name,
  match:awayTeam.id::bigint       AS away_team_id,
  match:awayTeam.name::string     AS away_team_name,
  match:awayTeam.shortName::string AS away_team_short_name,
  match:score.winner::string      AS winner,          -- HOME_TEAM, AWAY_TEAM, DRAW
  match:score.duration::string    AS duration,        -- REGULAR, EXTRA_TIME, PENALTY_SHOOTOUT
  match:score.fullTime.home::int  AS full_time_home,
  match:score.fullTime.away::int  AS full_time_away,
  match:score.halfTime.home::int  AS half_time_home,
  match:score.halfTime.away::int  AS half_time_away,
  match:score.penalties.home::int AS penalties_home,
  match:score.penalties.away::int AS penalties_away,
  match:lastUpdated::timestamp    AS last_updated,    -- when the API last changed the match
  fetched_at                                          -- when we fetched this version
FROM STREAM(matches);

CREATE OR REFRESH STREAMING TABLE ${silver_schema}.matches
COMMENT 'One row per match in its latest known state, typed. Kept current by AUTO CDC from bronze.matches.';

CREATE FLOW matches_latest AS AUTO CDC INTO ${silver_schema}.matches
FROM STREAM(matches_typed)
KEYS (match_id)
SEQUENCE BY fetched_at
STORED AS SCD TYPE 1;
