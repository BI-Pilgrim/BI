
-- #2 This will combine the ratings from Flipkart and Amazon Rating into one 
create or Replace Table `shopify-pubsub-project.Project_Goonj_asia.Marketplace_Combined_Ratings`
as

with combine as 
(SELECT 
distinct 
'Flipkart' as Channel,
SKU_ID,
Title,
date_trunc(Scraped_date,month) as Scraped_date,
avg(Avg_rating) as Avg_rating
 FROM `shopify-pubsub-project.Project_Goonj_asia.Flipkart_Ratings_Top_Products`
group by ALL

union all

select
distinct
'Amazon' as Channel,
SKU,
Product_Title,
date(date_trunc(Scraped_date,month)) as Scraped_date,
avg(Avg_rating) as Avg_rating
from `shopify-pubsub-project.Project_Goonj_asia.AMZ_Ratings_Top_products`
group by ALL
)
select 
  C.Channel ,
  SKU_ID,
  coalesce(Product_Title,Title,SKU_ID,'') as Product_Title,
  Scraped_date,
  Avg_rating
 
 from combine as C
 left join 
 (select
distinct
Parent_SKU,
Product_Title,
from `shopify-pubsub-project.adhoc_data_asia.Product_SKU_mapping_D2C`) as M
on C.SKU_ID = M.Parent_SKU;
