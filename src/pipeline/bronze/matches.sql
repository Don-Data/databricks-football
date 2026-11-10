-- Bronze: every match of every landed /matches response, one row per match per fetch.
--
-- The match stays exactly as the API returned it, as VARIANT: no schema to infer, nothing to
-- break when the API adds a field. The only change is unpacking the response's matches array.
--
-- The same match appears many times: once per fetch (daily runs re-fetch recent days), and a
-- kick-off at exactly 00:00 UTC lands in two neighbouring windows, because the API's dateTo
-- includes midnight. Silver keeps the latest version per match id.
CREATE OR REFRESH STREAMING TABLE matches
COMMENT 'One row per match per landed API response. The match is unchanged, as VARIANT.'
AS SELECT
  explode(cast(envelope:response.matches AS ARRAY<VARIANT>)) AS match,
  envelope:fetched_at::timestamp AS fetched_at,
  envelope:request.params.dateFrom::date AS request_date_from,
  envelope:request.params.dateTo::date AS request_date_to,
  _metadata.file_path AS source_file
FROM STREAM read_files(
  '${landing_root}/matches/',
  format => 'json',
  singleVariantColumn => 'envelope'
);
