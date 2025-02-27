-- #1 This will read  the data from Google Play store and Create a aggregated table 
create or Replace Table `shopify-pubsub-project.Project_Goonj_asia.Google_Playstore_Rating`
as
with raw_rating as 
( 
    select 
    date(date_trunc(review_given_at,month)) as Year_month,
    avg(score) as Avg_rating,
    sum(score) as Scores,
    count(review_id) as Customers
    from  `shopify-pubsub-project.pilgrim_bi_google_play.google_play_ratings`
    group by all
    order by 1),
    
base as 
(
    select 
    *,
    SUM(Scores) OVER (ORDER BY year_month) as Score_freq,
    SUM(Customers) OVER (ORDER BY year_month) as Customer_freq
    from raw_rating
    order by 1
    )

select 
*,
Score_freq/Customer_freq as Cumulative_Rating
from base;
