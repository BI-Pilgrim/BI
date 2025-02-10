MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.audience` as TARGET
USING
(
SELECT
  _airbyte_extracted_at,
  audience_description,
  audience_exclusion_dimension,
  audience_id,
  audience_name,
  audience_resource_name,
  audience_status,
  customer_id,
FROM
(
select *,
row_number() over(partition by audience_id order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.audience`
)
where rn = 1 and DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS SOURCE
ON TARGET.audience_id = SOURCE.audience_id
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.audience_description = SOURCE.audience_description,
  TARGET.audience_exclusion_dimension = SOURCE.audience_exclusion_dimension,
  TARGET.audience_id = SOURCE.audience_id,
  TARGET.audience_name = SOURCE.audience_name,
  TARGET.audience_resource_name = SOURCE.audience_resource_name,
  TARGET.audience_status = SOURCE.audience_status,
  TARGET.customer_id = SOURCE.customer_id
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  audience_description,
  audience_exclusion_dimension,
  audience_id,
  audience_name,
  audience_resource_name,
  audience_status,
  customer_id
)
VALUES
(
  _airbyte_extracted_at,
  audience_description,
  audience_exclusion_dimension,
  audience_id,
  audience_name,
  audience_resource_name,
  audience_status,
  customer_id
)