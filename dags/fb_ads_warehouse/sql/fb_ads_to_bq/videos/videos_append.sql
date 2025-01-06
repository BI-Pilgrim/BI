MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.videos` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    id,
    icon,
    title,
    views,
    `length`,
    `source`,
    published,
    account_id,
    embed_html,
    embeddable,
    is_episode,
    post_views,
    created_time,
    updated_time,
    permalink_url,
    content_category,
    is_crosspost_video,
    is_instagram_eligible,
    is_crossposting_eligible,


  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.videos
      WHERE
        DATE(_airbyte_extracted_at) > DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
    )
  WHERE row_num = 1  
) AS SOURCE


ON TARGET.id = SOURCE.id
WHEN MATCHED AND TARGET._airbyte_extracted_at > SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.id = SOURCE.id,
  TARGET.icon = SOURCE.icon,
  TARGET.title = SOURCE.title,
  TARGET.views = SOURCE.views,
  TARGET.`length` = SOURCE.`length`,
  TARGET.`source` = SOURCE.`source`,
  TARGET.published = SOURCE.published,
  TARGET.account_id = SOURCE.account_id,
  TARGET.embed_html = SOURCE.embed_html,
  TARGET.embeddable = SOURCE.embeddable,
  TARGET.is_episode = SOURCE.is_episode,
  TARGET.post_views = SOURCE.post_views,
  TARGET.created_time = SOURCE.created_time,
  TARGET.updated_time = SOURCE.updated_time,
  TARGET.permalink_url = SOURCE.permalink_url,
  TARGET.content_category = SOURCE.content_category,
  TARGET.is_crosspost_video = SOURCE.is_crosspost_video,
  TARGET.is_instagram_eligible = SOURCE.is_instagram_eligible,
  TARGET.is_crossposting_eligible = SOURCE.is_crossposting_eligible


WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  id,
  icon,
  title,
  views,
  `length`,
  `source`,
  published,
  account_id,
  embed_html,
  embeddable,
  is_episode,
  post_views,
  created_time,
  updated_time,
  permalink_url,
  content_category,
  is_crosspost_video,
  is_instagram_eligible,
  is_crossposting_eligible
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.id,
  SOURCE.icon,
  SOURCE.title,
  SOURCE.views,
  SOURCE.`length`,
  SOURCE.`source`,
  SOURCE.published,
  SOURCE.account_id,
  SOURCE.embed_html,
  SOURCE.embeddable,
  SOURCE.is_episode,
  SOURCE.post_views,
  SOURCE.created_time,
  SOURCE.updated_time,
  SOURCE.permalink_url,
  SOURCE.content_category,
  SOURCE.is_crosspost_video,
  SOURCE.is_instagram_eligible,
  SOURCE.is_crossposting_eligible
)
