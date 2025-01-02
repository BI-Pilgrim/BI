MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.user_interest` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    user_interest_launched_to_all,
    user_interest_name,
    user_interest_resource_name,
    user_interest_taxonomy_type,
    user_interest_user_interest_id,
    user_interest_user_interest_parent,
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY user_interest_user_interest_id ORDER BY _airbyte_extracted_at DESC) AS row_num
    FROM
      `shopify-pubsub-project.pilgrim_bi_google_ads.user_interest`
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1 -- Keep only the most recent row per label_id
) AS SOURCE
ON TARGET.user_interest_user_interest_id = SOURCE.user_interest_user_interest_id
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN
  UPDATE SET
    TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
    TARGET.user_interest_launched_to_all = SOURCE.user_interest_launched_to_all,
    TARGET.user_interest_name = SOURCE.user_interest_name,
    TARGET.user_interest_resource_name = SOURCE.user_interest_resource_name,
    TARGET.user_interest_taxonomy_type = SOURCE.user_interest_taxonomy_type,
    TARGET.user_interest_user_interest_id = SOURCE.user_interest_user_interest_id,
    TARGET.user_interest_user_interest_parent = SOURCE.user_interest_user_interest_parent
WHEN NOT MATCHED
THEN
INSERT
(
  _airbyte_extracted_at,
  user_interest_launched_to_all,
  user_interest_name,
  user_interest_resource_name,
  user_interest_taxonomy_type,
  user_interest_user_interest_id,
  user_interest_user_interest_parent
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.user_interest_launched_to_all,
  SOURCE.user_interest_name,
  SOURCE.user_interest_resource_name,
  SOURCE.user_interest_taxonomy_type,
  SOURCE.user_interest_user_interest_id,
  SOURCE.user_interest_user_interest_parent
)
