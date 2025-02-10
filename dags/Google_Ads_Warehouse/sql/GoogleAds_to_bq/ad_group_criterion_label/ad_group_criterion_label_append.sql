MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_criterion_label` as TARGET
USING
(
SELECT
  _airbyte_extracted_at,
  ad_group_criterion_criterion_id,
  ad_group_criterion_label_ad_group_criterion,
  ad_group_criterion_label_label,
  ad_group_criterion_label_resource_name,
  ad_group_id,
  label_id,
FROM
(
select *,
row_number() over(partition by ad_group_id,ad_group_criterion_criterion_id order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_criterion_label`
)
where rn = 1 and DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS SOURCE
ON TARGET.ad_group_id = SOURCE.ad_group_id
and TARGET.ad_group_criterion_criterion_id = SOURCE.ad_group_criterion_criterion_id
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.ad_group_criterion_criterion_id = SOURCE.ad_group_criterion_criterion_id,
  TARGET.ad_group_criterion_label_ad_group_criterion = SOURCE.ad_group_criterion_label_ad_group_criterion,
  TARGET.ad_group_criterion_label_label = SOURCE.ad_group_criterion_label_label,
  TARGET.ad_group_criterion_label_resource_name = SOURCE.ad_group_criterion_label_resource_name,
  TARGET.ad_group_id = SOURCE.ad_group_id,
  TARGET.label_id = SOURCE.label_id
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_group_criterion_criterion_id,
  ad_group_criterion_label_ad_group_criterion,
  ad_group_criterion_label_label,
  ad_group_criterion_label_resource_name,
  ad_group_id,
  label_id
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_group_criterion_criterion_id,
  SOURCE.ad_group_criterion_label_ad_group_criterion,
  SOURCE.ad_group_criterion_label_label,
  SOURCE.ad_group_criterion_label_resource_name,
  SOURCE.ad_group_id,
  SOURCE.label_id
)