CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_normal`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  ad_group_ad_ad_call_ad_disable_call_conversion,
  ad_group_ad_ad_legacy_responsive_display_ad_allow_flexible_color,
  ad_group_ad_ad_responsive_display_ad_allow_flexible_color,
  ad_group_ad_ad_responsive_display_ad_control_spec_enable_asset_enhancements,
  ad_group_ad_ad_responsive_display_ad_control_spec_enable_autogen_video,
  ad_group_ad_ad_id,
  ad_group_ad_ad_image_ad_pixel_height,
  ad_group_ad_ad_image_ad_pixel_width,
  ad_group_ad_ad_image_ad_preview_pixel_height,
  ad_group_ad_ad_image_ad_preview_pixel_width,
  ad_group_id,
  segments_date,
  ad_group_ad_ad_app_ad_mandatory_ad_text,
  ad_group_ad_ad_call_ad_conversion_reporting_state,
  ad_group_ad_ad_display_upload_ad_display_upload_product_type,
  ad_group_ad_ad_expanded_dynamic_search_ad_description,
  ad_group_ad_ad_expanded_dynamic_search_ad_description2,
  ad_group_ad_ad_group,
  ad_group_ad_ad_image_ad_image_url,
  ad_group_ad_ad_image_ad_name,
  ad_group_ad_ad_image_ad_preview_image_url,
  ad_group_ad_ad_legacy_responsive_display_ad_format_setting,
  ad_group_ad_ad_resource_name,
  ad_group_ad_ad_responsive_display_ad_format_setting,
  ad_group_ad_ad_responsive_search_ad_path1,
  ad_group_ad_ad_responsive_search_ad_path2,
  ad_group_ad_ad_strength,
  ad_group_ad_ad_system_managed_resource_source,
  ad_group_ad_ad_text_ad_description1,
  ad_group_ad_ad_text_ad_description2,
  ad_group_ad_ad_tracking_url_template,
  ad_group_ad_ad_type,
  ad_group_ad_ad_video_ad_in_stream_action_button_label,
  ad_group_ad_ad_video_ad_in_stream_action_headline,
  ad_group_ad_policy_summary_approval_status,
  ad_group_ad_policy_summary_review_status,
  ad_group_ad_resource_name,
  ad_group_ad_status,
  _airbyte_extracted_at,
FROM
(
select *,
row_number() over(partition by ad_group_ad_ad_id,segments_date order by _airbyte_extracted_at desc) rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`
)
where rn = 1
