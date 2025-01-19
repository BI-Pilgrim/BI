MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_url_custom_parameters` as TARGET
USING
(
  SELECT
    ad_group_ad_ad_id,
    ad_group_id,
    _airbyte_extracted_at,

     -- ad_group_ad_ad_url_custom_parameters,
     MAX(CASE WHEN REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'key: \"([^\"]+)\"') = 'campaignname' THEN 
         REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'value: \"([^\"]+)\"') END) AS campaignname,
     MAX(CASE WHEN REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'key: \"([^\"]+)\"') = 'medium' THEN 
         REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'value: \"([^\"]+)\"') END) AS medium,
     MAX(CASE WHEN REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'key: \"([^\"]+)\"') = 'source' THEN 
         REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(param, '$'), r'value: \"([^\"]+)\"') END) AS param_source

  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY ad_group_id ORDER BY ad_group_id DESC) AS ROW_NUM
    FROM
       `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
       UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_url_custom_parameters)) AS param
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE
    ROW_NUM = 1
  GROUP BY
    ad_group_id, ad_group_ad_ad_id, _airbyte_extracted_at
) AS SOURCE
ON TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id,
TARGET.ad_group_id = SOURCE.ad_group_id,
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.campaignname = SOURCE.campaignname,
TARGET.medium = SOURCE.medium,
TARGET.param_source = SOURCE.param_source
WHEN NOT MATCHED
THEN INSERT
(
  ad_group_ad_ad_id,
  ad_group_id,
  _airbyte_extracted_at,
  campaignname,
  medium,
  param_source
)
VALUES
(
  SOURCE.ad_group_ad_ad_id,
  SOURCE.ad_group_id,
  SOURCE._airbyte_extracted_at,
  SOURCE.campaignname,
  SOURCE.medium,
  SOURCE.param_source
)