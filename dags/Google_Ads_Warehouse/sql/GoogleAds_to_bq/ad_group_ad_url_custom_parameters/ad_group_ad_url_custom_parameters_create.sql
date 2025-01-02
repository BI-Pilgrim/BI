CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_url_custom_parameters`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_group_ad_ad_id,
  ad_group_id,

  -- ad_group_ad_ad_url_custom_parameters,
  MAX(CASE WHEN REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'key: \"([^\"]+)\"') = 'campaignname' THEN 
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'value: \"([^\"]+)\"') END) AS campaignname,
  MAX(CASE WHEN REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'key: \"([^\"]+)\"') = 'medium' THEN 
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'value: \"([^\"]+)\"') END) AS medium,
  MAX(CASE WHEN REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'key: \"([^\"]+)\"') = 'source' THEN 
      REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'value: \"([^\"]+)\"') END) AS param_source
FROM
  `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
  UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_url_custom_parameters)) AS param
GROUP BY
  ad_group_id, ad_group_ad_ad_id, _airbyte_extracted_at;