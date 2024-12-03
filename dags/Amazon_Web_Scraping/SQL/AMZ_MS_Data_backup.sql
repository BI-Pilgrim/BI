-- 1) To Drop the Current Month table So that new Scraped data data can be cleaned by removing the duplicates and mapping of final category is done and new table as current_month_MS can be created
DROP TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Current_month_MS`;


-- 2) Process to remove duplicates and mapped the inbuilt_category to final_Category and creating the table current_month_MS with the new month's cleaned data
create table `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Current_month_MS`
AS
select 
* from 
(  select 
  CM.*,
  MP.Final_Category,
  row_number() over(partition by ASIN) as numbering
  from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing` as CM
  left join `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_MS_Inbuilt_Category_Mapping` as MP
  on CM.Inbuilt_category = MP.Rich_Inbuilt_category and 
  CM.Category = MP.Category
)
where numbering=1;

-- 3) Same current month data is Appended to AMZ_SKU_level_Historical_MS as well
INSERT INTO `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_SKU_level_Historical_MS`
select
Date(Date_MS) as Date_MS,
Category,
Brand_value,
Product_Title,
Parent_ASIN,
ASIN as Child_ASIN,
Product_URL,
Selling_Price,
Unit_Sold,
Revenue,
MRP_Price,
Per_100ml_price,
Max_Unit_Sold,
Max_Revenue,
Size_of_SKU,
Best_Seller_in_Beauty,
Scent_type,
Skin_type,
Benefits,
Item_form,
Item_weight,
Active_ingredient,
Net_volume,
Skin_tone,
Item_volume,
Pack_size,
SPF_factor,
Hair_type,
Material_type_free,
Recomended_for,
Reviews_Summary,
Benefits_type,
Active_ingredient_type,
Material_type_free_type,
Scent_type as Scent_type_type,
Hair_type as Hair_type_type,
Recomended_for as Recomended_for_type,
Item_weight_in_gm,
Net_volume_in_ml,
Item_volume_in_ml,
SPF_factor_type,
Best_Seller_in_Category,
Inbuilt_category,
Final_Category,
from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Current_month_MS`;


-- 4) Appending the latest current month data into aggregated table by aggregating 
INSERT INTO `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Aggregated_MS`
select 
Date_MS,
Brand_value,
Category,
Final_Category,
sum(Revenue) as Min_Revenue,
sum(Max_Revenue) as Max_Revenue,
sum(Unit_Sold) as Min_Unit_sold,
sum(Max_Unit_Sold) as Max_Unit_sold,
count(distinct ASIN) as Child_ASIN,

from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Current_month_MS`
group by ALL;

-- Once everything is done drop the table in which the data is coming from python code
DROP TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing`;

