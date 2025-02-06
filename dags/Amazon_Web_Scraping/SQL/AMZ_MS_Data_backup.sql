-- 1) Drop the current month table so thaat new table can embed inot it 
DROP TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Current_month_MS`;

-- 2) Creating a current month table by deleting the duplicate rows from raw market_sizing table 
create or replace table `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Current_month_MS`
AS
select 
 distinct
Product_Title,
Parent_ASIN,
ASIN,
Product_URL,
MRP_Price,
Per_100ml_price,
Selling_Price,
Unit_Sold,
AZ_URL,
Size_of_SKU,
Brand_Name,
Brand_value,
Best_Seller_in_Beauty,
Best_Seller_in_Category,
Scent_type,
Skin_type,
Benefits,
Item_form,
Item_weight,
Active_ingredient,
Net_volume,
Skin_tone,
Item_volume,
Special_feature,
Pack_size,
SPF_factor,
Hair_type,
Material_type_free,
Recomended_for,
Reviews_Summary,
Max_Unit_Sold,
Revenue,
Max_Revenue,
Category,
Benefits_type,
Active_ingredient_type,
Material_type_free_type,
Item_weight_in_gm,
Net_volume_in_ml,
Item_volume_in_ml,
SPF_factor_type,
date(Date_MS) as Date_MS,
Inbuilt_category,

from (SELECT
    *,
    row_number() over(partition by ASIN order by Revenue desc) as ranking
 FROM `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing`
)
where ranking = 1;

-- 3.0) prequisit : Execute the below CTE to get the new asins which are not tagged historically 
-- get the result manually and add it in the https://docs.google.com/spreadsheets/d/14ljF-Q7fQk84PpI_Dr6fD_U0fKRa3LhJSyAHR_Lct1w and fill it up manually and then run step3

select * from 
(select
distinct
CM.Category,
CM.asin,
CM.Inbuilt_category,
case when CM.Inbuilt_category='0' then CM.Category else CTM.Final_Category end as Final_Category
from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Current_month_MS` as CM
left join `shopify-pubsub-project.Amazon_Market_Sizing.Inbuilt_Final_category_mapping` as CTM
on CM.ASIN = CTM.Child_ASIN
)
where Final_Category is null;

-- 3) Same current month data is Appended to AMZ_SKU_level_Historical_MS as well after joining it with category mappping sheet to get the final cateory
INSERT INTO `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_SKU_level_Historical_MS`
select 
cast(CM.Date_MS as date) as Date_MS,
CM.Category,
CM.Brand_value,
CM.Product_Title,
CM.Parent_ASIN,
CM.ASIN as Child_ASIN,
CM.Product_URL,
CM.Selling_Price,
CM.Unit_Sold,
CM.Revenue,
CM.MRP_Price,
CM.Per_100ml_price,
CM.Max_Unit_Sold,
CM.Max_Revenue,
CM.Size_of_SKU,
CM.Best_Seller_in_Beauty,
CM.Scent_type,
CM.Skin_type,
CM.Benefits,
CM.Item_form,
CM.Item_weight,
CM.Active_ingredient,
CM.Net_volume,
CM.Skin_tone,
CM.Item_volume,
CM.Pack_size,
CM.SPF_factor,
CM.Hair_type,
CM.Material_type_free,
CM.Recomended_for,
CM.Reviews_Summary,
CM.Benefits_type,
CM.Active_ingredient_type,
CM.Material_type_free_type,
CM.Scent_type as Scent_type_type,
CM.Hair_type as Hair_type_type,
CM.Recomended_for as Recomended_for_type,
CM.Item_weight_in_gm,
CM.Net_volume_in_ml,
CM.Item_volume_in_ml,
CM.SPF_factor_type,
CM.Best_Seller_in_Category,
CM.Inbuilt_category,
case when CM.Inbuilt_category = '0' then CM.Category else CTM.Final_Category end as Final_Category
from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Current_month_MS` as CM
left join `shopify-pubsub-project.Amazon_Market_Sizing.Inbuilt_Final_category_mapping` as CTM
on CM.ASIN = CTM.Child_ASIN;


-- 4) Appending the latest current month data into aggregated table by aggregating 
-- INSERT INTO
create or replace table `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Aggregated_MS` as
select 
Date_MS,
Brand_value,
Category,
Inbuilt_category,
Final_Category,
sum(Revenue) as Min_Revenue,
sum(Max_Revenue) as Max_Revenue,
sum(Unit_Sold) as Min_Unit_sold,
sum(Max_Unit_Sold) as Max_Unit_sold,
count(distinct Child_ASIN) as Child_ASIN,
from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_SKU_level_Historical_MS`
-- where Date_MS = '2025-01-01'
group by ALL;

-- Once everything is done drop the table in which the data is coming from python code
DROP TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing`;
