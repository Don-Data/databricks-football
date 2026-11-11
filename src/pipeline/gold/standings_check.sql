-- Gold: our league table next to football-data.org's, team by team. A check, not for analysis.
--
-- gold.standings computes every table itself, from the matches. The API also publishes its
-- tables, and the ingest job lands them for the current season of every tracked league. Three
-- warning expectations compare the two. Their pass rates show in the pipeline's Data quality
-- tab after every update; the rows behind them are in this table.
--   same_games     played, won, drawn, lost and goals. A difference means our matches and the
--                  API's disagree, or the API's table is wrong (it once left out an abandoned
--                  match the league had confirmed: Nantes - Toulouse, Ligue 1 2025/26).
--   same_points    differs with the same games only through a point deduction, which isn't in
--                  the match data.
--   same_position  differs with the same points through a tie-break: standings.sql ranks ties
--                  by goal difference and goals only and lets tied teams share a position.
-- Warnings, not failures: the API can be wrong, and that mustn't stop our pipeline.
--
-- Only tables the API published are compared, and a team counts once either side has played.
-- That leaves out a season whose first games haven't been played yet.

CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.standings_check (
  CONSTRAINT same_games EXPECT (
    ours_played <=> published_played
    AND ours_won <=> published_won
    AND ours_drawn <=> published_drawn
    AND ours_lost <=> published_lost
    AND ours_goals_for <=> published_goals_for
    AND ours_goals_against <=> published_goals_against
  ),
  CONSTRAINT same_points EXPECT (ours_points <=> published_points),
  CONSTRAINT same_position EXPECT (ours_position <=> published_position)
)
COMMENT 'Our league table (gold.standings) next to the one football-data.org publishes, per team. A quality check: see the expectations same_games, same_points and same_position.'
AS
WITH published AS (
  SELECT * FROM ${silver_schema}.published_standings
),

published_tables AS (
  SELECT DISTINCT competition_code, season_id FROM published
),

ours AS (
  SELECT s.*
  FROM ${gold_schema}.standings AS s
  JOIN published_tables USING (competition_code, season_id)
)

SELECT
  coalesce(o.competition_code, p.competition_code) AS competition_code,
  coalesce(o.season, p.season)                     AS season,
  coalesce(o.season_id, p.season_id)               AS season_id,
  coalesce(o.team_id, p.team_id)                   AS team_id,
  coalesce(o.team_name, p.team_name)               AS team_name,
  o.position      AS ours_position,      p.position      AS published_position,
  o.played        AS ours_played,        p.played        AS published_played,
  o.won           AS ours_won,           p.won           AS published_won,
  o.drawn         AS ours_drawn,         p.drawn         AS published_drawn,
  o.lost          AS ours_lost,          p.lost          AS published_lost,
  o.goals_for     AS ours_goals_for,     p.goals_for     AS published_goals_for,
  o.goals_against AS ours_goals_against, p.goals_against AS published_goals_against,
  o.points        AS ours_points,        p.points        AS published_points,
  p.fetched_at    AS published_fetched_at
FROM ours AS o
FULL OUTER JOIN published AS p
  ON o.competition_code = p.competition_code AND o.season_id = p.season_id AND o.team_id = p.team_id
WHERE o.team_id IS NOT NULL OR p.played > 0;
