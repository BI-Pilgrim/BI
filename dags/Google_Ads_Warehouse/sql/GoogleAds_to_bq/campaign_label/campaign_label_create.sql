create or replace table `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_label`
partition by date_trunc(_airbyte_extracted_at, day)
as
select
_airbyte_extracted_at,
label_id,
label_name,
campaign_id,
label_resource_name,
campaign_resource_name,
Campaign_label_resource_name
from
(
select *,
row_number() over(partition by campaign_id,label_id order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_google_ads.campaign_label
)
where rn = 1