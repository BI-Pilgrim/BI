MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_search_ad_desc` as TARGET
USING
(
  SELECT
    ad_group_ad_ad_id,
    ad_group_id,
    _airbyte_extracted_at,

     -- ad_group_ad_ad_responsive_search_ad_descriptions,
     REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(ABC, '$'), r'text: \"([^\"]+)\"') AS search_ad_descriptions

  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY ad_group_id ORDER BY ad_group_id DESC) AS ROW_NUM
    FROM
     `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
     UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_responsive_search_ad_descriptions)) AS ABC
  )
  WHERE
    ROW_NUM = 1
) AS SOURCE
ON TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
TARGET.ad_group_ad_ad_id = SOURCE.ad_group_ad_ad_id,
TARGET.ad_group_id = SOURCE.ad_group_id,
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.search_ad_descriptions = SOURCE.search_ad_descriptions
WHEN NOT MATCHED
THEN INSERT
(
  ad_group_ad_ad_id,
  ad_group_id,
  _airbyte_extracted_at,
  search_ad_descriptions
)
VALUES
(
  SOURCE.ad_group_ad_ad_id,
  SOURCE.ad_group_id,
  SOURCE._airbyte_extracted_at,
  SOURCE.search_ad_descriptions
)