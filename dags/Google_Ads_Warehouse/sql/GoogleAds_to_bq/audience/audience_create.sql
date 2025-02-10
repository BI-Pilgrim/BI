CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.audience`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
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
where rn = 1