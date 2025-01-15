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
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY ad_group_id ORDER BY ad_group_id DESC) AS ROW_NUM
    FROM
     `shopify-pubsub-project.pilgrim_bi_google_ads.geographic_view`
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE
    ROW_NUM = 1
) AS SOURCE
ON TARGET.ad_group_id = SOURCE.ad_group_id
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