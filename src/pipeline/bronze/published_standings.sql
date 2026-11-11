-- Bronze: every landed /standings response, one row per response, unchanged as VARIANT.
-- A response is one league table as football-data.org publishes it: one competition and season,
-- with a total, a home and an away table. Silver keeps the latest version of every team's row;
-- gold compares it with the table we compute ourselves (gold/standings_check.sql).
--
-- Every response must name its competition and season, because silver keys on them.
CREATE OR REFRESH STREAMING TABLE published_standings (
  CONSTRAINT table_has_competition_and_season
    EXPECT (response:competition.id IS NOT NULL AND response:season.id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'One row per landed /standings response: a published league table, unchanged, as VARIANT.'
AS SELECT
  envelope:response AS response,
  envelope:fetched_at::timestamp AS fetched_at,
  envelope:request.path::string AS request_path,
  _metadata.file_path AS source_file
FROM STREAM read_files(
  '${landing_root}/standings/',
  format => 'json',
  singleVariantColumn => 'envelope'
);
