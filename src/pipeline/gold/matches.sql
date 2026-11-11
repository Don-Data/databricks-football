-- Gold: every match, shaped for the people and tools that consume it (dashboards, Genie,
-- ad hoc queries). Consumers read gold only, so silver can change without breaking them.
--
-- A view, not a materialized copy: it's a 1:1 projection of silver.matches with names instead
-- of codes, so it needs no storage and no refresh, and it's always current. Materialize it only
-- if queries get slow. Technical columns (fetched_at, last_updated) stay in silver.
--
-- Goals and winner are the game's, not the tie's. When a tie goes to penalties,
-- football-data.org's full-time score includes the penalties, and its `winner` is unreliable:
-- usually the shootout's winner, sometimes DRAW. In a two-legged tie the shootout can follow a
-- game one side won: Tolima - Tachira (Libertadores 2026) comes as 3-1 with penalties 3-0, and
-- the game was 0-1. The API's `duration` doesn't flag every shootout either. So the penalties
-- are always subtracted, and the winner always follows the goals. The shootout stays in
-- penalties_home and penalties_away.

CREATE VIEW ${gold_schema}.matches
COMMENT 'Every match with competition, season, teams and score. One row per match. Goals and winner are the game''s, without a penalty shootout.'
AS WITH games AS (
  SELECT
    *,
    full_time_home - coalesce(penalties_home, 0) AS home_goals,
    full_time_away - coalesce(penalties_away, 0) AS away_goals
  FROM ${silver_schema}.matches
)
SELECT
  m.match_id,
  c.competition_code,
  c.competition_name,
  c.area_name,
  m.season,
  m.stage,
  m.matchday,
  m.match_group,
  m.kickoff_utc,
  m.status,
  m.home_team_id,
  m.home_team_name,
  m.home_team_short_name,
  m.away_team_id,
  m.away_team_name,
  m.away_team_short_name,
  m.home_goals,
  m.away_goals,
  m.half_time_home  AS home_goals_half_time,
  m.half_time_away  AS away_goals_half_time,
  CASE -- NULL until there's a score
    WHEN m.home_goals > m.away_goals THEN 'HOME_TEAM'
    WHEN m.home_goals < m.away_goals THEN 'AWAY_TEAM'
    WHEN m.home_goals = m.away_goals THEN 'DRAW'
  END AS winner,
  m.duration,
  m.penalties_home,
  m.penalties_away
FROM games AS m
JOIN ${silver_schema}.competitions AS c USING (competition_id);
