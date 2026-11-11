-- Silver: the league tables as football-data.org publishes them, one row per team and table, in
-- the latest version, typed. Only the total table of the regular season: the response also
-- carries home and away tables. AUTO CDC keeps the row from the latest fetch per team.

CREATE TEMPORARY VIEW published_standings_typed AS
SELECT
  response:competition.id::bigint      AS competition_id,
  response:competition.code::string    AS competition_code,
  response:season.id::bigint           AS season_id,
  CASE WHEN year(response:season.startDate::date) = year(response:season.endDate::date)
       THEN cast(year(response:season.startDate::date) AS STRING)
       ELSE concat(year(response:season.startDate::date), '/',
                   right(cast(year(response:season.endDate::date) AS STRING), 2))
  END                                  AS season,          -- the same label as silver.matches
  entry:team.id::bigint                AS team_id,
  entry:team.name::string              AS team_name,
  entry:position::int                  AS position,
  entry:playedGames::int               AS played,
  entry:won::int                       AS won,
  entry:draw::int                      AS drawn,
  entry:lost::int                      AS lost,
  entry:goalsFor::int                  AS goals_for,
  entry:goalsAgainst::int              AS goals_against,
  entry:points::int                    AS points,
  fetched_at
FROM (
  SELECT response, fetched_at, explode(cast(standing:table AS ARRAY<VARIANT>)) AS entry
  FROM (
    SELECT response, fetched_at, explode(cast(response:standings AS ARRAY<VARIANT>)) AS standing
    FROM STREAM(published_standings)
  )
  WHERE standing:type::string = 'TOTAL' AND standing:stage::string = 'REGULAR_SEASON'
);

CREATE OR REFRESH STREAMING TABLE ${silver_schema}.published_standings
COMMENT 'The league tables football-data.org publishes, one row per team and table, latest fetch. Kept current by AUTO CDC from bronze.published_standings.';

CREATE FLOW published_standings_latest AS AUTO CDC INTO ${silver_schema}.published_standings
FROM STREAM(published_standings_typed)
KEYS (competition_id, season_id, team_id)
SEQUENCE BY fetched_at
STORED AS SCD TYPE 1;
