-- Gold: the team dimension. Join it to gold.matches (home_team_id, away_team_id) or
-- gold.standings (team_id) for names, codes and crests. A view: a 1:1 projection of silver.teams.

CREATE VIEW ${gold_schema}.teams
COMMENT 'One row per team: name, short name, three-letter code and crest.'
AS SELECT
  team_id,
  team_name,
  team_short_name,
  team_tla,
  crest_url
FROM ${silver_schema}.teams;
