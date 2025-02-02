CREATE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_non_json`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  id,
  name,
  status,
  adset_id,
  bid_type,
  account_id,
  bid_amount,
  campaign_id,
  created_time,
  source_ad_id,
  updated_time,
  effective_status,
  last_updated_by_app_id,
    -- TARGETING,
  JSON_EXTRACT_SCALAR(targeting, '$.age_max') AS age_max,
  JSON_EXTRACT_SCALAR(targeting, '$.age_min') AS age_min,
  JSON_EXTRACT(targeting, '$.brand_safety_content_filter_levels') AS brand_safety_content_filter_levels,
 
  -- JSON_EXTRACT(targeting, '$.excluded_custom_audiences') AS excluded_custom_audiences,
  -- JSON_EXTRACT_SCALAR(excluded_audience, '$.id') AS excluded_audience_id,
  -- JSON_EXTRACT_SCALAR(excluded_audience, '$.name') AS excluded_audience_name,
  JSON_EXTRACT(targeting, '$.genders') AS genders,
 
  -- JSON_EXTRACT(targeting, '$.geo_locations') AS geo_locations,
  JSON_EXTRACT(targeting, '$.geo_locations.countries') AS geo_location_countries,
  JSON_EXTRACT(targeting, '$.geo_locations.location_types') AS geo_location_types,
  JSON_EXTRACT_SCALAR(targeting, '$.targeting_automation.advantage_audience') AS advantage_audience,
  JSON_EXTRACT_SCALAR(targeting, '$.targeting_relaxation_types.custom_audience') AS targeting_relaxation_custom_audience,
  JSON_EXTRACT_SCALAR(targeting, '$.targeting_relaxation_types.lookalike') AS targeting_relaxation_lookalike,
 
    -- Extract individual fields from the JSON objects
  -- JSON_EXTRACT_SCALAR(value, '$.blame_field') AS blame_field,
  -- CAST(JSON_EXTRACT_SCALAR(value, '$.code') AS INT64) AS code,
  -- JSON_EXTRACT_SCALAR(value, '$.confidence') AS confidence,
  -- JSON_EXTRACT_SCALAR(value, '$.importance') AS importance,
  -- JSON_EXTRACT_SCALAR(value, '$.message') AS message,
  -- JSON_EXTRACT_SCALAR(value, '$.title') AS title,


  JSON_EXTRACT_scalar(creative, '$.id') as creative_id
FROM
(
  select
  *,
  row_number() over(partition by id,created_time order by created_time desc) as rn,
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads
  -- UNNEST(JSON_EXTRACT_ARRAY(targeting, '$.excluded_custom_audiences')) AS excluded_audience,
  -- UNNEST(JSON_EXTRACT_ARRAY(recommendations)) AS value
)
where rn = 1


