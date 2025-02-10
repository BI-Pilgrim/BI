CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_criterion_label`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
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
where rn = 1
