DROP TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_New_Brand_History`;


DROP TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Static_Brand_History`;


CREATE TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_New_Brand_History`
as 
with brand_ranking as 
(
   select 
 *,
 row_number() over(partition by Category order by Max_Revenue desc) as top_brand
 from (
  SELECT
    Category,
    Brand_value,
    sum(Max_Revenue) as Max_revenue,
  FROM `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_current_month_MS`
  where brand_value not in ('0','')
  group by ALL)
),

top_10_this_month as
(
  select 
    *
  from brand_ranking
  where top_brand<=10
),

top_10_static as
(
  select 
    *
  from `shopify-pubsub-project.Amazon_Market_Sizing.Category_Wise_Top_Brands`
),

new_brands_coming as 
(
  select 
    D.Category,
    D.Brand_value
  from top_10_this_month as D
  left join top_10_static as S
  on D.Category = S.Category
  and D.Brand_value = S.Brand_value
  where S.Brand_value is null
)

select
  AG.* 
from new_brands_coming  as NB
left join (
  select 
  *
  from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Aggregated_MS`
) as AG
on NB.Category = AG.Category
and NB.Brand_value = AG.Brand_value
where AG.Category is not null;



CREATE TABLE `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Static_Brand_History`

as 

  select 
    AG.*
  from `shopify-pubsub-project.Amazon_Market_Sizing.Category_Wise_Top_Brands` as SB
  left join 
  (
    select 
      *
    from `shopify-pubsub-project.Amazon_Market_Sizing.AMZ_Aggregated_MS`

  ) as AG

on SB.Category = AG.Category
and SB.Brand_value = AG.Brand_value
where AG.Brand_value is not null;
