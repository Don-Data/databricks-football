-- Gold: the league table of every competition and season, computed from finished matches.
--
-- Only leagues, and only their regular season: play-offs and relegation play-offs don't count
-- towards the table, and cups have no single table. AWARDED matches count with their awarded score,
-- as in the official table: Nantes - Toulouse 2025/26 was abandoned at 0-0, and the LFP confirmed
-- the 0-0. football-data.org's own table leaves that match out (see src/football/check_standings.py).
--
-- Grouped by season from the start: the data holds several seasons per competition (2025/26 and
-- 2026/27 in Europe, calendar years in Brazil), and a table across seasons would be wrong.
--
-- Only seasons whose first matchday is in the data. History starts on one date for all
-- competitions, so a season that began earlier (Brazil 2025 began in March, history in August)
-- would produce a table with half the matches missing. The ingestion guarantees gap-free
-- history from its start date, so "matchday 1 is there" means "the whole season is there".
--
-- Known simplifications against official tables: ties are ranked by points, goal difference and
-- goals scored only (some leagues use head-to-head first), and point deductions aren't in the
-- match data. Tied teams share a position.

CREATE OR REFRESH MATERIALIZED VIEW ${gold_schema}.standings
COMMENT 'League table per competition and season: one row per team, from finished regular-season matches.'
AS
WITH league_matches AS (
  SELECT m.*
  FROM ${silver_schema}.matches AS m
  JOIN ${silver_schema}.competitions AS c USING (competition_id)
  WHERE c.competition_type = 'LEAGUE'
    AND m.stage = 'REGULAR_SEASON'
    AND m.status IN ('FINISHED', 'AWARDED')
),

complete_seasons AS (
  SELECT competition_id, season_id
  FROM league_matches
  GROUP BY competition_id, season_id
  HAVING min(matchday) = 1
),

-- Every match twice: once from the home team's view, once from the away team's.
team_matches AS (
  SELECT competition_id, season_id, season,
         home_team_id AS team_id, home_team_name AS team_name,
         full_time_home AS goals_for, full_time_away AS goals_against
  FROM league_matches
  UNION ALL
  SELECT competition_id, season_id, season,
         away_team_id, away_team_name,
         full_time_away, full_time_home
  FROM league_matches
),

totals AS (
  SELECT
    competition_id,
    season_id,
    any_value(season)                          AS season,
    team_id,
    any_value(team_name)                       AS team_name,
    count(*)                                   AS played,
    count_if(goals_for > goals_against)        AS won,
    count_if(goals_for = goals_against)        AS drawn,
    count_if(goals_for < goals_against)        AS lost,
    sum(goals_for)                             AS goals_for,
    sum(goals_against)                         AS goals_against
  FROM team_matches
  JOIN complete_seasons USING (competition_id, season_id)
  GROUP BY competition_id, season_id, team_id
)

SELECT
  c.competition_code,
  c.competition_name,
  c.area_name,
  t.season_id,
  t.season,                                                            -- '2025/26' or '2026', from silver
  rank() OVER (
    PARTITION BY t.competition_id, t.season_id
    ORDER BY 3 * t.won + t.drawn DESC, t.goals_for - t.goals_against DESC, t.goals_for DESC
  )                                            AS position,
  t.team_id,
  t.team_name,
  t.played,
  t.won,
  t.drawn,
  t.lost,
  t.goals_for,
  t.goals_against,
  t.goals_for - t.goals_against                AS goal_difference,
  3 * t.won + t.drawn                          AS points
FROM totals AS t
JOIN ${silver_schema}.competitions AS c USING (competition_id);
