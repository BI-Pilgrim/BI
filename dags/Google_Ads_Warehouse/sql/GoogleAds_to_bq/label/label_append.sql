MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.label` as TARGET
USING
(
SELECT
  _airbyte_extracted_at,
  customer_id,
  label_id,
  label_name,
  label_resource_name,
  label_status,
  label_text_label_background_color,
  label_text_label_description,
FROM
(
select *,
row_number() over(partition by label_id order by _airbyte_extracted_at) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.label`
)
where rn = 1 and DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS SOURCE
ON TARGET.label_id = SOURCE.label_id
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.customer_id = SOURCE.customer_id,
  TARGET.label_id = SOURCE.label_id,
  TARGET.label_name = SOURCE.label_name,
  TARGET.label_resource_name = SOURCE.label_resource_name,
  TARGET.label_status = SOURCE.label_status,
  TARGET.label_text_label_background_color = SOURCE.label_text_label_background_color,
  TARGET.label_text_label_description = SOURCE.label_text_label_description
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  customer_id,
  label_id,
  label_name,
  label_resource_name,
  label_status,
  label_text_label_background_color,
  label_text_label_description
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.customer_id,
  SOURCE.label_id,
  SOURCE.label_name,
  SOURCE.label_resource_name,
  SOURCE.label_status,
  SOURCE.label_text_label_background_color,
  SOURCE.label_text_label_description
)