merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_non_json` as target
using
(
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
where rn = 1 and date(created_time) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.id = source.id
when matched and target.created_time > source.created_time
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.id = source.id,
target.name = source.name,
target.status = source.status,
target.adset_id = source.adset_id,
target.bid_type = source.bid_type,
target.account_id = source.account_id,
target.bid_amount = source.bid_amount,
target.campaign_id = source.campaign_id,
target.created_time = source.created_time,
target.source_ad_id = source.source_ad_id,
target.updated_time = source.updated_time,
target.effective_status = source.effective_status,
target.last_updated_by_app_id = source.last_updated_by_app_id,
target.age_max = source.age_max,
target.age_min = source.age_min,
target.brand_safety_content_filter_levels = source.brand_safety_content_filter_levels,
target.genders = source.genders,
target.geo_location_countries = source.geo_location_countries,
target.geo_location_types = source.geo_location_types,
target.advantage_audience = source.advantage_audience,
target.targeting_relaxation_custom_audience = source.targeting_relaxation_custom_audience,
target.targeting_relaxation_lookalike = source.targeting_relaxation_lookalike
when not matched
then insert
(
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
age_max,
age_min,
brand_safety_content_filter_levels,
genders,
geo_location_countries,
geo_location_types,
advantage_audience,
targeting_relaxation_custom_audience,
targeting_relaxation_lookalike
)
values
(
source._airbyte_extracted_at,
source.id,
source.name,
source.status,
source.adset_id,
source.bid_type,
source.account_id,
source.bid_amount,
source.campaign_id,
source.created_time,
source.source_ad_id,
source.updated_time,
source.effective_status,
source.last_updated_by_app_id,
source.age_max,
source.age_min,
source.brand_safety_content_filter_levels,
source.genders,
source.geo_location_countries,
source.geo_location_types,
source.advantage_audience,
source.targeting_relaxation_custom_audience,
source.targeting_relaxation_lookalike
)