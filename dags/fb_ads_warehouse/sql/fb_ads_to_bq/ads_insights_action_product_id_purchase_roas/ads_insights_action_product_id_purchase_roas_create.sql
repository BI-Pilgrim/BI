CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_purchase_roas`
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  date_start,
  product_id,
  adset_id,
  account_id,
  campaign_id,


  -- purchase_roas,
  JSON_EXTRACT_SCALAR(purch_roas, '$.1d_click') AS purchase_roas_1d_click,
  JSON_EXTRACT_SCALAR(purch_roas, '$.1d_view') AS purchase_roas_1d_view,
  JSON_EXTRACT_SCALAR(purch_roas, '$.28d_click') AS purchase_roas_28d_click,
  JSON_EXTRACT_SCALAR(purch_roas, '$.28d_views') AS purchase_roas_28d_views,
  JSON_EXTRACT_SCALAR(purch_roas, '$.7d_click') AS purchase_roas_7d_click,
  JSON_EXTRACT_SCALAR(purch_roas, '$.7d_view') AS purchase_roas_7d_view,
  JSON_EXTRACT_SCALAR(purch_roas, '$.action_type') AS purchase_roas_action_type,
  JSON_EXTRACT_SCALAR(purch_roas, '$.value') AS purchase_roas_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,product_id,JSON_EXTRACT_SCALAR(purch_roas, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id,
UNNEST(JSON_EXTRACT_ARRAY(purchase_roas)) AS purch_roas
)
where rn = 1