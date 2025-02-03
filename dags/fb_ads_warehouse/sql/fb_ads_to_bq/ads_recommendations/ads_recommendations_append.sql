merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_recommendations` as target
using
(
SELECT
  _airbyte_extracted_at,
  id,
  created_time,

  -- JSON_EXTRACT_SCALAR(excluded_audience, '$.id') AS excluded_audience_id,
  -- JSON_EXTRACT_SCALAR(excluded_audience, '$.name') AS excluded_audience_name,
  -- JSON_EXTRACT(targeting, '$.genders') AS genders,
 
  -- JSON_EXTRACT(targeting, '$.geo_locations') AS geo_locations,
  -- JSON_EXTRACT(targeting, '$.geo_locations.countries') AS geo_location_countries,
  -- JSON_EXTRACT(targeting, '$.geo_locations.location_types') AS geo_location_types,
  -- JSON_EXTRACT_SCALAR(targeting, '$.targeting_automation.advantage_audience') AS advantage_audience,
  -- JSON_EXTRACT_SCALAR(targeting, '$.targeting_relaxation_types.custom_audience') AS targeting_relaxation_custom_audience,
  -- JSON_EXTRACT_SCALAR(targeting, '$.targeting_relaxation_types.lookalike') AS targeting_relaxation_lookalike,
 
    -- Extract individual fields from the JSON objects
  JSON_EXTRACT_SCALAR(value, '$.blame_field') AS blame_field,
  CAST(JSON_EXTRACT_SCALAR(value, '$.code') AS INT64) AS code,
  JSON_EXTRACT_SCALAR(value, '$.confidence') AS confidence,
  JSON_EXTRACT_SCALAR(value, '$.importance') AS importance,
  JSON_EXTRACT_SCALAR(value, '$.message') AS message,
  JSON_EXTRACT_SCALAR(value, '$.title') AS title,

FROM
(
  select
  *,
  row_number() over(partition by id,created_time order by created_time desc) as rn,
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads,
  -- UNNEST(JSON_EXTRACT_ARRAY(targeting, '$.excluded_custom_audiences')) AS excluded_audience,
  UNNEST(JSON_EXTRACT_ARRAY(recommendations)) AS value
)
where rn = 1 and date(created_time) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.id = source.id
when matched and target.created_time < source.created_time
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.id = source.id,
target.created_time = source.created_time,
target.blame_field = source.blame_field,
target.code = source.code,target.confidence = source.confidence,
target.importance = source.importance,
target.message = source.message,
target.title = source.title
when not matched
then insert
(
_airbyte_extracted_at,
id,
created_time,
blame_field,
code,
confidence,
importance,
message,
title
)
values
(
source._airbyte_extracted_at,
source.id,
source.created_time,
source.blame_field,
source.code,
source.confidence,
source.importance,
source.message,
source.title
)


