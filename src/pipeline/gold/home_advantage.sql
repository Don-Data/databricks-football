-- Gold: every team's home and away record, per competition and season. The base for the home
-- advantage analysis (resources/home_advantage.dashboard.yml).
--
-- Only matches with a real home side: league matches of the regular season, and cup matches
-- except the final, which is played at a neutral venue. League play-offs are left out for the
-- same reason (the Championship play-off final is at Wembley).
--
-- Only FINISHED matches: an AWARDED result wasn't played out (Nantes - Toulouse 2025/26 was
-- abandoned after 22 minutes), so it says nothing about playing at home.
--
-- The match score, not the tie: after a penalty shootout, football-data.org's full-time score
-- includes the penalties, and `winner` names who won the tie. A two-legged tie can go to
-- penalties after a 0-1 home defeat, so the penalties are subtracted and the result is
-- derived from the goals.
--
-- Counts only, no ratios, so everything adds up across teams, seasons and competitions. The
-- home team's share of the points, for example, is
-- sum(home_points) / (sum(home_points) + sum(away_points)), and 50% means no home advantage.

CREATE VIEW ${gold_schema}.home_advantage
COMMENT 'Home and away record per competition, season and team, from finished matches with a home side. Counts only: compute ratios as ratios of sums.'
AS
WITH home_side_matches AS (
  SELECT
    c.competition_code,
    c.competition_name,
    c.competition_type,
    c.area_name,
    m.season,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.full_time_home - coalesce(m.penalties_home, 0) AS home_goals,
    m.full_time_away - coalesce(m.penalties_away, 0) AS away_goals
  FROM ${silver_schema}.matches AS m
  JOIN ${silver_schema}.competitions AS c USING (competition_id)
  WHERE m.status = 'FINISHED'
    AND (
      (c.competition_type = 'LEAGUE' AND m.stage = 'REGULAR_SEASON')
      OR (c.competition_type = 'CUP' AND m.stage <> 'FINAL')
    )
),

-- Every match twice: once for the home team, once for the away team.
team_matches AS (
  SELECT competition_code, competition_name, competition_type, area_name, season,
         home_team_id AS team_id, home_team_name AS team_name, true AS at_home,
         home_goals AS goals_for, away_goals AS goals_against
  FROM home_side_matches
  UNION ALL
  SELECT competition_code, competition_name, competition_type, area_name, season,
         away_team_id, away_team_name, false,
         away_goals, home_goals
  FROM home_side_matches
),

records AS (
  SELECT
    competition_code,
    any_value(competition_name)                                  AS competition_name,
    any_value(competition_type)                                  AS competition_type,
    any_value(area_name)                                         AS area_name,
    season,
    team_id,
    any_value(team_name)                                         AS team_name,
    count_if(at_home)                                            AS home_played,
    count_if(at_home AND goals_for > goals_against)              AS home_won,
    count_if(at_home AND goals_for = goals_against)              AS home_drawn,
    count_if(at_home AND goals_for < goals_against)              AS home_lost,
    coalesce(sum(CASE WHEN at_home THEN goals_for END), 0)       AS home_goals_for,
    coalesce(sum(CASE WHEN at_home THEN goals_against END), 0)   AS home_goals_against,
    count_if(NOT at_home)                                        AS away_played,
    count_if(NOT at_home AND goals_for > goals_against)          AS away_won,
    count_if(NOT at_home AND goals_for = goals_against)          AS away_drawn,
    count_if(NOT at_home AND goals_for < goals_against)          AS away_lost,
    coalesce(sum(CASE WHEN NOT at_home THEN goals_for END), 0)     AS away_goals_for,
    coalesce(sum(CASE WHEN NOT at_home THEN goals_against END), 0) AS away_goals_against
  FROM team_matches
  GROUP BY competition_code, season, team_id
)

SELECT
  *,
  3 * home_won + home_drawn AS home_points,
  3 * away_won + away_drawn AS away_points
FROM records;
