create or replace table `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_recommendations`
partition by date_trunc(_airbyte_extracted_at, day)
as
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
where rn = 1
); 