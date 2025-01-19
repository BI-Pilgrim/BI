CREATE OR REPLACE Table `shopify-pubsub-project.Amazon_Market_Sizing.Top_10_Brands` AS

with agg_brand as (select
Final_Category,
Brand_value,
sum(Max_Revenue) as max_Revenue
from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Aggregated_MS`
where Date_MS='2024-12-01'
group by ALL)

select
Final_Category,
Brand_value
from (select 
*,
row_number() over(partition by Final_Category order by max_Revenue desc) as ranking
from agg_brand)
where ranking<=10;


CREATE OR REPLACE Table `shopify-pubsub-project.Amazon_Market_Sizing.New_Competitor_Brand_History` AS

with Agg_cte as 
( 
  select 
    Date_MS,
    final_category,
    lower(Brand_Value) as Brand_Value,
    sum(Min_Revenue) as Min_Revenue,
    sum(Max_Revenue) as Max_Revenue,
    sum(Min_Unit_sold) as Min_Unit_sold,
    sum(Max_Unit_sold) as Max_Unit_sold

 from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Aggregated_MS` 
 group by ALL
),

static_brand_history as 
( select 
  distinct
  A.Final_Category,
  lower(A.Brand_value) as Brand_value,
  B.Date_MS,
  B.Min_Revenue,
  B.Max_Revenue,
  B.Min_Unit_sold,
  B.Max_Unit_sold,
  1 as flag
  from `shopify-pubsub-project.Amazon_Market_Sizing.Top_10_Brands` as A
  left join Agg_cte as B
  on A.Final_Category = B.Final_Category
  and lower(A.Brand_value) = lower(B.Brand_value)
  where date_ms is not null
),

New_Brand_history as 
( select 
  distinct
  
  B.Final_Category,
  lower(B.Brand_value) as Brand_value,
  B.Date_MS,
  B.Min_Revenue,
  B.Max_Revenue,
  B.Min_Unit_sold,
  B.Max_Unit_sold,
  0 as flag
  from `shopify-pubsub-project.Amazon_Market_Sizing.Top_10_Brands` as A
  right join Agg_cte as B
  on A.Final_Category = B.Final_Category
  and lower(A.Brand_value) = lower(B.Brand_value)
  where A.Final_Category is null
)

select 
* from static_brand_history
union all
select 
* from New_Brand_history
