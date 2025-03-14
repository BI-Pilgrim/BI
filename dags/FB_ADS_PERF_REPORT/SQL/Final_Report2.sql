create or replace table `shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2`
partition by date_start
as


with agg_spend as
(
  select
  *
  from
    (
    select
    ad_id,
    date_start,
    -- gender,
    -- spend,
    sum(spend) as spend
    from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_gender
    -- where ad_id = '120214394066630308'

    group by ad_id,date_start
    order by ad_id, date_start desc
    )
  where spend > 0
),

revenue_data as
(
  select
  ad_id,
  date_start,
  -- gender,
  sum(cast(json_extract_scalar(act_vals, '$.1d_click')as float64)) as revenue,
  -- json_extract_scalar(act_vals, '$.action_type') as action_type,
  -- sum(spend) as spend
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_gender,
  unnest(json_extract_array(action_values)) as act_vals
  where json_extract_scalar(act_vals, '$.action_type') = 'purchase'
  -- and ad_id = '120200086228210111'
  group by ad_id,date_start
  order by ad_id, date_start desc
),

last7day_data as
(
  select
  ad_id,
  sum(spend) as spend_L7D,
  sum(revenue) as revenue_L7D
  from
  (
    select
    agg.ad_id,
    agg.date_start,
    agg.spend,
    rd.revenue,
    row_number() over(partition by agg.ad_id order by agg.date_start desc) as rn
    from agg_spend agg
    left join revenue_data rd
    on agg.ad_id = rd.ad_id
    and agg.date_start = rd.date_start
    order by ad_id,agg.date_start desc
  )
  where rn <= 7
group by ad_id
)
,

tiered_data as
(
select
ad_id,
date_start,
cumulative_spend,
cumulative_revenue,
cumulative_revenue/cumulative_spend as roas,
case
  WHEN cumulative_revenue/cumulative_spend >= 1.4 AND cumulative_spend >= 100000 THEN 'TITANIUM'
  WHEN cumulative_revenue/cumulative_spend >= 1.4 AND (cumulative_spend BETWEEN 50000 AND 100001) THEN 'POT. TITANIUM'
  WHEN (cumulative_revenue/cumulative_spend BETWEEN 1.0 AND 1.4) AND cumulative_spend >= 100000 THEN 'PLATINUM'
  WHEN (cumulative_revenue/cumulative_spend BETWEEN 1.0 AND 1.4) AND (cumulative_spend BETWEEN 50000 AND 100001)THEN 'POT. PLATINUM'
  WHEN (cumulative_revenue/cumulative_spend BETWEEN 0.8 AND 1.0) AND cumulative_spend >= 50000 THEN 'GOLD'
  WHEN (cumulative_revenue/cumulative_spend BETWEEN 0.6 AND 0.8) AND (cumulative_spend BETWEEN 50000 AND 100001) THEN 'SILVER'
  ELSE "OTHERS"
end as tier
from
(
  select
  ad_id,
  date_start,
  -- tgt_gender,
  sum(spend) over(partition by ad_id order by date_start) as cumulative_spend,
  sum(revenue_1dc) over(partition by ad_id order by date_start) as cumulative_revenue,
  row_number() over(partition by ad_id,date_start order by date_start) as rn
  from
  (
    select
    ad_id,
    ad_name,
    gender as tgt_gender,
    date_start,
    spend,

    cast(JSON_EXTRACT_SCALAR(act,'$.1d_click') as float64) AS revenue_1dc,
    JSON_EXTRACT_SCALAR(act,'$.action_type') AS action_values_action_type,

    from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_gender,
    unnest(json_extract_array(action_values)) as act

    where JSON_EXTRACT_SCALAR(act,'$.action_type') = 'purchase' --and ad_id = '120200086927160111'--'120200231086040111'
    AND REGEXP_CONTAINS(ad_name, r'\| (000|3|9)')
    AND NOT REGEXP_CONTAINS(LOWER(ad_name), r'\b(static|sale|collection|carousal|Static|Sale|Collection|Carousal|Sales|sales|offer|Offer)\b')
    order by ad_id,date_start
  )
  order by ad_id, date_start
)
where rn = 1 and cumulative_spend != 0
),

best_tier_data as
(
SELECT 
ad_id,
min(tier_rank),
case  --CASE TO ASSIGN THE BEST TIER
  when min(tier_rank) = 1 then 'TITANIUM'
  when min(tier_rank) = 2 then 'POT. TITANIUM'
  when min(tier_rank) = 3 then 'PLATINUM'
  when min(tier_rank) = 4 then 'POT PLATINUM'
  when min(tier_rank) = 5 then 'GOLD'
  when min(tier_rank) = 6 then 'SILVER'
  when min(tier_rank) = 999 then 'OTHERS'
  else 'OTHERS'
end as best_tier
from
(
  select
  ad_id,
  -- date_start,
  -- D0,
  -- D1,
  -- D2,
  -- D3,
  -- D4,
  -- D5,
  -- D6,  
  case
    when consistent_tier = 'TITANIUM' then 1
    when consistent_tier = 'POT. TITANIUM' then 2
    when consistent_tier = 'PLATINUM' then 3
    when consistent_tier = 'POT. PLATINUM' then 4
    when consistent_tier = 'GOLD' then 5
    when consistent_tier = 'SILVER' then 6
    when consistent_tier = 'OTHERS' then 999
    ELSE 999
  end as tier_rank
  FROM
  ( -- SUBQUERY TO MAP THE MOST CONSISTENT TIER
    select
      ad_id,
      date_start,
      -- D0,
      -- D1,
      -- D2,
      -- D3,
      -- D4,
      -- D5,
      -- D6,
      -- D0 as tier,

      case
        when D0 = 'TITANIUM'
        AND D1 = 'TITANIUM'
        and D2 = 'TITANIUM'
        and D3 = 'TITANIUM'
        and D4 = 'TITANIUM'
        and D5 = 'TITANIUM'
        and D6 = 'TITANIUM'
        then 'TITANIUM'

        when D0 = 'POT. TITANIUM'
        AND D1 = 'POT. TITANIUM'
        and D2 = 'POT. TITANIUM'
        and D3 = 'POT. TITANIUM'
        and D4 = 'POT. TITANIUM'
        and D5 = 'POT. TITANIUM'
        and D6 = 'POT. TITANIUM'
        then 'POT. TITANIUM' 

        when D0 = 'POT. PLATINUM'
        AND D1 = 'POT. PLATINUM'
        and D2 = 'POT. PLATINUM'
        and D3 = 'POT. PLATINUM'
        and D4 = 'POT. PLATINUM'
        and D5 = 'POT. PLATINUM'
        and D6 = 'POT. PLATINUM'
        then 'POT. PLATINUM'

        when D0 = 'PLATINUM'
        AND D1 = 'PLATINUM'
        and D2 = 'PLATINUM'
        and D3 = 'PLATINUM'
        and D4 = 'PLATINUM'
        and D5 = 'PLATINUM'
        and D6 = 'PLATINUM'
        then 'PLATINUM'

        when D0 = 'GOLD'
        AND D1 = 'GOLD'
        and D2 = 'GOLD'
        and D3 = 'GOLD'
        and D4 = 'GOLD'
        and D5 = 'GOLD'
        and D6 = 'GOLD'
        then 'GOLD'

        when D0 = 'SILVER'
        AND D1 = 'SILVER'
        and D2 = 'SILVER'
        and D3 = 'SILVER'
        and D4 = 'SILVER'
        and D5 = 'SILVER'
        and D6 = 'SILVER'
        then 'SILVER'

        when D0 = 'OTHERS'
        AND D1 = 'OTHERS'
        and D2 = 'OTHERS'
        and D3 = 'OTHERS'
        and D4 = 'OTHERS'
        and D5 = 'OTHERS'
        and D6 = 'OTHERS'
        then 'OTHERS'

        ELSE 'OTHERS'
      end as consistent_tier
    FROM
    ( --SUBQUERY TO GET D0,D1,D2,D3,D4,D5,D6 
      SELECT 
        a.ad_id, 
        a.date_start,
        a.tier as tier,
        a.tier AS D0,
        b.tier AS D1,
        c.tier AS D2,
        d.tier AS D3,
        e.tier AS D4,
        f.tier AS D5,
        g.tier AS D6
      FROM shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2 a
        LEFT JOIN shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2 b ON a.ad_id = b.ad_id AND DATE(b.date_start) = DATE(a.date_start) - INTERVAL 1 DAY
        LEFT JOIN shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2 c ON a.ad_id = c.ad_id AND DATE(c.date_start) = DATE(a.date_start) - INTERVAL 2 DAY
        LEFT JOIN shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2 d ON a.ad_id = d.ad_id AND DATE(d.date_start) = DATE(a.date_start) - INTERVAL 3 DAY
        LEFT JOIN shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2 e ON a.ad_id = e.ad_id AND DATE(e.date_start) = DATE(a.date_start) - INTERVAL 4 DAY
        LEFT JOIN shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2 f ON a.ad_id = f.ad_id AND DATE(f.date_start) = DATE(a.date_start) - INTERVAL 5 DAY
        LEFT JOIN shopify-pubsub-project.Facebook_Ads_Performance_Report.Final_Report2 g ON a.ad_id = g.ad_id AND DATE(g.date_start) = DATE(a.date_start) - INTERVAL 6 DAY
        -- where a.ad_id = '120214394066630308'
        -- WHERE a.tier = 'PLATINUM' AND  b.tier = 'PLATINUM' AND c.tier = 'PLATINUM' AND  d.tier = 'PLATINUM' AND e.tier = 'PLATINUM' AND f.tier = 'PLATINUM' AND g.tier = 'PLATINUM'
    ) --SUBQUERY TO GET D0,D1,D2,D3,D4,D5,D6
  ) --SUBQUERY TO MAP MOST CONSISTENT TIER
) -- SUBQUERY TO RANK THE MOST CONSISTENT QUERY
group by ad_id
)


select * except(rn)
from
(
select
sq.*,
cmm.influencer,
cmm.gender,
cmm.category,
cmm.product,
l7d.spend_L7D,
l7d.revenue_L7D,
td.tier,
btd.best_tier,
concat(extract(month from start_date),'-',extract(year from start_date)) as start_period,
extract(month from start_date) as start_month,
extract(year from start_date) as start_year,
row_number() over(partition by sq.ad_id,sq.date_start,sq.running_status order by sq.date_start) as rn
from
(
select
ad_id,
TRIM(SPLIT(ad_name, ' | ')[ORDINAL(ARRAY_LENGTH(SPLIT(ad_name, ' | ')))]) AS creative_id,
SPLIT(ad_name, ' | ')[SAFE_OFFSET(0)] AS funnel_name,

date_start,
ad_name,
min(date_start) over(partition by ad_id) as start_date,
max(date_start) over(partition by ad_id) as end_date,

DATE_DIFF((max(date_start) over(partition by ad_id)), (min(date_start) over(partition by ad_id)), DAY) AS duration,

coalesce(sum(case when lower(tgt_gender) = 'female' then spend end),0) as female_spend,
coalesce(sum(case when lower(tgt_gender) = 'male' then spend end),0) as male_spend,

coalesce(sum(case when lower(tgt_gender) = 'female' then revenue_1dc else 0 end),0) as female_revenue,
coalesce(sum(case when lower(tgt_gender) = 'male' then revenue_1dc else 0 end),0) as male_revenue,

coalesce(sum(spend),0) as daily_spend,
coalesce(sum(revenue_1dc),0) as daily_revenue,

row_number() over(partition by ad_id order by date_start) as day_no,
case
  when coalesce(sum(spend),0) > 0 then 'ACTIVE'
  else 'PAUSED'
end running_status,
from
(
  select
  ad_id,
  ad_name,
  gender as tgt_gender,
  date_start,
  spend,

  cast(JSON_EXTRACT_SCALAR(act,'$.1d_click') as float64) AS revenue_1dc,
  JSON_EXTRACT_SCALAR(act,'$.action_type') AS action_values_action_type,

  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_gender,
  unnest(json_extract_array(action_values)) as act

  where JSON_EXTRACT_SCALAR(act,'$.action_type') = 'purchase' --and ad_id = '120200086927160111'--'120200231086040111'
  AND REGEXP_CONTAINS(ad_name, r'\| (000|3|9)')
  AND NOT REGEXP_CONTAINS(LOWER(ad_name), r'\b(static|sale|collection|carousal|Static|Sale|Collection|Carousal|Sales|sales|offer|Offer)\b')
  order by ad_id,date_start
)
group by ad_id,date_start,ad_name
order by ad_id, date_start
) as sq
join shopify-pubsub-project.Facebook_Ads_Performance_Report.creative_master_main cmm
on TRIM(SPLIT(ad_name, ' | ')[ORDINAL(ARRAY_LENGTH(SPLIT(ad_name, ' | ')))]) = CMM.Creative_ID
left join last7day_data as l7d
on l7d.ad_id = sq.ad_id
join tiered_data td
on sq.ad_id = td.ad_id
and sq.date_start = td.date_start
left join best_tier_data btd
on sq.ad_id = btd.ad_id

-- where sq.ad_id = '120210099145410111'
-- order by ad_id, date_start
)
where rn = 1 --and female_spend != 0 and female_revenue != 0 and male_spend !=0 and male_revenue != 0
-- where  ad_id = '120209255464040111'
-- order by ad_id, date_start