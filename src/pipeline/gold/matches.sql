-- Gold: every match, shaped for the people and tools that consume it (dashboards, Genie,
-- ad hoc queries). Consumers read gold only, so silver can change without breaking them.
--
-- A view, not a materialized copy: it's a 1:1 projection of silver.matches with names instead
-- of codes, so it needs no storage and no refresh, and it's always current. Materialize it only
-- if queries get slow. Technical columns (fetched_at, last_updated) stay in silver.

CREATE VIEW ${gold_schema}.matches
COMMENT 'Every match with competition, season, teams and score. One row per match.'
AS SELECT
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
  m.full_time_home  AS home_goals,
  m.full_time_away  AS away_goals,
  m.half_time_home  AS home_goals_half_time,
  m.half_time_away  AS away_goals_half_time,
  m.winner,
  m.duration,
  m.penalties_home,
  m.penalties_away
FROM ${silver_schema}.matches AS m
JOIN ${silver_schema}.competitions AS c USING (competition_id);
