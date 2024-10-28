-- Before starting with the new data append first add the missing SKU from the Amazon coming data to the same table w.r.t previous month data 

INSERT INTO `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing`
select 
 CM.* 
from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_current_month_MS` as CM
left join `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing` as ND 
on CM.Child_ASIN = ND.Child_ASIN
where ND.Child_ASIN is null;


-- drop the current table
DROP TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_current_month_MS`;


-- Create the current table with recent 1 month data from amazon_market_sizing where data is coming from python code
CREATE TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_current_month_MS`
AS
SELECT 
Date_MS,
Category,
Brand_value,
Product_Title,
Parent_ASIN,
ASIN as Child_ASIN,
Product_URL,
Selling_Price,
Unit_Sold,
Revenue,
No_Of_Ratings as Ratings,
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
Scent_type_type,
Hair_type_type,
Recomended_for_type,
Item_weight_in_gm,
Net_volume_in_ml,
Item_volume_in_ml,
SPF_factor_type,
Best_Seller_in_Category,

FROM (
select 
B.*,
C.Catgeory_Revenue,
ROW_NUMBER() OVER (PARTITION BY Parent_ASIN,Date_MS ORDER BY Catgeory_Revenue DESC) AS row_num
from  `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing` as B
left join (SELECT
    Category,
    sum(Revenue) as Catgeory_Revenue
    from   `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing`
    group by 1) as C
on B.Category = C.Category
)
where row_num=1;



-- Inserting the same clean data of current month into the backup table SKU level
INSERT INTO `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_SKU_level_MS`

SELECT 
Date_MS,
Category,
Brand_value,
Product_Title,
Parent_ASIN,
ASIN as Child_ASIN,
Product_URL,
Selling_Price,
Unit_Sold,
Revenue,
No_Of_Ratings as Ratings,
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
Scent_type_type,
Hair_type_type,
Recomended_for_type,
Item_weight_in_gm,
Net_volume_in_ml,
Item_volume_in_ml,
SPF_factor_type,
Best_Seller_in_Category,


FROM (
select 
B.*,
C.Catgeory_Revenue,
ROW_NUMBER() OVER (PARTITION BY Parent_ASIN,Date_MS ORDER BY Catgeory_Revenue DESC) AS row_num
from  `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing` as B
left join (
  SELECT
    Category,
    sum(Revenue) as Catgeory_Revenue
    from   `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing`
    group by 1) as C
on B.Category = C.Category
)
where row_num=1;


-- appending the latest current month data into aggregated table
INSERT INTO `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Aggregated_MS`
select 
Date_MS,
Brand_value,
Category,

sum(Revenue) as Min_Revenue,
sum(Max_Revenue) as Max_Revenue,
sum(Unit_Sold) as Min_Unit_sold,
sum(Max_Unit_Sold) as Max_Unit_sold,
count(distinct Parent_ASIN) as Parent_SKU,

from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_current_month_MS`
group by ALL;


-- Once everything is done drop the table in which the data is coming from python code
DROP TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Market_Sizing`;
