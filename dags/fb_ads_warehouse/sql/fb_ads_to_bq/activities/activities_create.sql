CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.activities`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  actor_id,
  object_id,
  account_id,
  actor_name,
  event_time,
  event_type,
  extra_data,
  object_name,
  object_type,
  application_id,
  application_name,
  date_time_in_timezone,
  translated_event_type,
from
(
select
*,
row_number() over(partition by object_id, event_time, account_id order by event_time desc) as rn
FROM `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.activities`
)
where rn = 1