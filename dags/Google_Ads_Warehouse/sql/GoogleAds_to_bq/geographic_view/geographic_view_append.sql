MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.geographic_view` as TARGET
USING
(
SELECT
  _airbyte_extracted_at,
  ad_group_id,
  customer_descriptive_name,
  customer_id,
  geographic_view_country_criterion_id,
  geographic_view_location_type,
  segments_date,
FROM
(
select *,
row_number() over(partition by ad_group_id,segments_date,geographic_view_location_type,geographic_view_country_criterion_id order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.geographic_view`
)
where rn = 1 and DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS SOURCE
ON TARGET.ad_group_id = SOURCE.ad_group_id
and TARGET.segments_date = SOURCE.segments_date
and TARGET.geographic_view_location_type = SOURCE.geographic_view_location_type
and TARGET.geographic_view_country_criterion_id = SOURCE.geographic_view_country_criterion_id
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.ad_group_id = SOURCE.ad_group_id,
  TARGET.customer_descriptive_name = SOURCE.customer_descriptive_name,
  TARGET.customer_id = SOURCE.customer_id,
  TARGET.geographic_view_country_criterion_id = SOURCE.geographic_view_country_criterion_id,
  TARGET.geographic_view_location_type = SOURCE.geographic_view_location_type,
  TARGET.segments_date = SOURCE.segments_date
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_group_id,
  customer_descriptive_name,
  customer_id,
  geographic_view_country_criterion_id,
  geographic_view_location_type,
  segments_date
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_group_id,
  SOURCE.customer_descriptive_name,
  SOURCE.customer_id,
  SOURCE.geographic_view_country_criterion_id,
  SOURCE.geographic_view_location_type,
  SOURCE.segments_date
)