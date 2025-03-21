select 
distinct
A.customer_id,
A.state,
A.Skin_Care_top_10,
A.Hair_Care_top_10,
A.Makeup_top_10,
C.top_1_percentile,
C.top_10_percentile,
C.top_20_percentile,
C.top_50_percentile,
B.JFM24,
B.AMJ24,
B.JAS24,
B.OND24,
D.Overall_Top_Product,
D.Hair_Care_Product,
D.Skin_care_Product,
D.Makeup_Product,
D.total_qty,
D.GMV_value,
D.Purchased_vales,
D.Amount_Saved,
from `shopify-pubsub-project.adhoc_data.Customer_state_category_top10_Rewind2024` as A
left join `shopify-pubsub-project.adhoc_data.Customer_Quarter_Top10_Rewind2024` as B
using(customer_id)
left join `shopify-pubsub-project.adhoc_data.Customer__State_Top_1_10_20_50_Rewind2024` as C
using(customer_id)
left join `shopify-pubsub-project.adhoc_data.Customer_Top_Producst_and_Discount_Rewind2024` as D
using(customer_id)
