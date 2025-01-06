MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.activities` AS TARGET
USING
(
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
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY _airbyte_extracted_at ORDER BY _airbyte_extracted_at DESC) AS row_num
      FROM
        `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.activities`
      WHERE
        DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1 -- Keep only the most recent row per label_id
) AS SOURCE
ON
  TARGET.object_id = SOURCE.object_id
WHEN MATCHED AND TARGET._airbyte_extracted_at > SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.actor_id = SOURCE.actor_id,
  TARGET.object_id = SOURCE.object_id,
  TARGET.account_id = SOURCE.account_id,
  TARGET.actor_name = SOURCE.actor_name,
  TARGET.event_time = SOURCE.event_time,
  TARGET.event_type = SOURCE.event_type,
  TARGET.extra_data = SOURCE.extra_data,
  TARGET.object_name = SOURCE.object_name,
  TARGET.object_type = SOURCE.object_type,
  TARGET.application_id = SOURCE.application_id,
  TARGET.application_name = SOURCE.application_name,
  TARGET.date_time_in_timezone = SOURCE.date_time_in_timezone,
  TARGET.translated_event_type = SOURCE.translated_event_type
WHEN NOT MATCHED
THEN INSERT
(
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
  translated_event_type
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.actor_id,
  SOURCE.object_id,
  SOURCE.account_id,
  SOURCE.actor_name,
  SOURCE.event_time,
  SOURCE.event_type,
  SOURCE.extra_data,
  SOURCE.object_name,
  SOURCE.object_type,
  SOURCE.application_id,
  SOURCE.application_name,
  SOURCE.date_time_in_timezone,
  SOURCE.translated_event_type
)
