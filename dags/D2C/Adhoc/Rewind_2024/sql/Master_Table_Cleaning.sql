CREATE TABLE `shopify-pubsub-project.adhoc_data.Master_All_in_one_Rewind2024_cleaned` AS
SELECT
    COALESCE(customer_id, '') AS customer_id,
    COALESCE(state, '') AS state,
    max(COALESCE(Skin_Care_top_10, '')) AS Skin_Care_top_10,
    max(COALESCE(Hair_Care_top_10, '')) AS Hair_Care_top_10,
    max(COALESCE(Makeup_top_10, '')) AS Makeup_top_10,
    max(COALESCE(top_1_percentile, '')) AS top_1_percentile,
    max(COALESCE(top_10_percentile, '')) AS top_10_percentile,
    max(COALESCE(top_20_percentile, '')) AS top_20_percentile,
    max(COALESCE(top_50_percentile, '')) AS top_50_percentile,
    max(COALESCE(JFM24, '')) AS JFM24,
    max(COALESCE(AMJ24, '')) AS AMJ24,
    max(COALESCE(JAS24, '')) AS JAS24,
    max(COALESCE(OND24, '')) AS OND24,
    max(COALESCE(Overall_Top_Product, '')) AS Overall_Top_Product,
    max(COALESCE(Hair_Care_Product, '')) AS Hair_Care_Product,
    max(COALESCE(Skin_care_Product, '')) AS Skin_care_Product,
    max(COALESCE(Makeup_Product, '')) AS Makeup_Product,
    max(COALESCE(CAST(CASE WHEN total_qty < 0 THEN 0 ELSE total_qty END AS INT64), 0)) AS total_qty,
    max(COALESCE(CAST(CASE WHEN GMV_value < 0 THEN 0 ELSE GMV_value END AS INT64), 0)) AS GMV_value,
    max(COALESCE(CAST(CASE WHEN Purchased_vales < 0 THEN 0 ELSE Purchased_vales END AS INT64), 0)) AS Purchased_vales,
    max(COALESCE(CAST(CASE WHEN Amount_Saved < 0 THEN 0 ELSE Amount_Saved END AS INT64), 0)) AS Amount_Saved
FROM `shopify-pubsub-project.adhoc_data.Master_All_in_one_Rewind2024`
group by 1,2;
