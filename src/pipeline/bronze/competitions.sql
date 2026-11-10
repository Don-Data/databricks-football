-- Bronze: every competition of every landed /competitions response, one row per competition per fetch.
-- Unchanged as VARIANT, like bronze.matches. Each run lands the full list, so silver keeps the latest.
CREATE OR REFRESH STREAMING TABLE competitions
COMMENT 'One row per competition per landed API response. The competition is unchanged, as VARIANT.'
AS SELECT
  explode(cast(envelope:response.competitions AS ARRAY<VARIANT>)) AS competition,
  envelope:fetched_at::timestamp AS fetched_at,
  _metadata.file_path AS source_file
FROM STREAM read_files(
  '${landing_root}/competitions/',
  format => 'json',
  singleVariantColumn => 'envelope'
);
