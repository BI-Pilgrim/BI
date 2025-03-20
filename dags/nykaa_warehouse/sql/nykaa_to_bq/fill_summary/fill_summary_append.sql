merge into `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.fill_summary` as target
using
(
select
* except(rn)
from
(
select
wh_location,
case
when lower(wh_location) = 'ahd' then 'Ahmedabad'
when lower(wh_location) = 'blr' then 'Bengaluru'
when lower(wh_location) = 'blr1' then 'Bengaluru (Branch 1)'
when lower(wh_location) = 'blr2' then 'Bengaluru (Branch 2)'
when lower(wh_location) = 'blrbeg' then 'Bengaluru (Begur)'
when lower(wh_location) = 'blrdee' then 'Bengaluru (Devanahalli)'
when lower(wh_location) = 'blryel' then 'Bengaluru (Yelahanka)'
when lower(wh_location) = 'bvi' then 'Bhubaneswar'
when lower(wh_location) = 'chn' then 'Chennai'
when lower(wh_location) = 'cwh' then 'Coimbatore'
when lower(wh_location) = 'ded' then 'Dehradun'
when lower(wh_location) = 'del1' then 'Delhi (Branch 1)'
when lower(wh_location) = 'del2' then 'Delhi (Branch 2)'
when lower(wh_location) = 'ggn' then 'Gurugram'
when lower(wh_location) = 'gkp' then 'Gorakhpur'
when lower(wh_location) = 'guw' then 'Guwahati'
when lower(wh_location) = 'hyd' then 'Hyderabad'
when lower(wh_location) = 'ind' then 'Indore'
when lower(wh_location) = 'kkj' then 'Kochi'
when lower(wh_location) = 'kol1' then 'Kolkata (Branch 1)'
when lower(wh_location) = 'kol2' then 'Kolkata (Branch 2)'
when lower(wh_location) = 'mtl' then 'Mangalore'
when lower(wh_location) = 'mum1 new' then 'Mumbai (Branch 1)'
when lower(wh_location) = 'mum2' then 'Mumbai (Branch 2)'
when lower(wh_location) = 'mumand' then 'Mumbai (Andheri)'
when lower(wh_location) = 'mumtha' then 'Mumbai (Thane)'
when lower(wh_location) = 'ncrpit' then 'NCR (Pitampura)'
when lower(wh_location) = 'nda' then 'Noida'
when lower(wh_location) = 'ngp' then 'Nagpur'
when lower(wh_location) = 'nwl' then 'Navi Mumbai'
when lower(wh_location) = 'patna' then 'Patna'
when lower(wh_location) = 'pun' then 'Pune'
when lower(wh_location) = 'rjg' then 'Rajkot'
when lower(wh_location) = 'rtn' then 'Raipur'
when lower(wh_location) = 'tauru' then 'Tauru'
when lower(wh_location) = 'wht' then 'Warangal'
when lower(wh_location) = 'mumsew' then 'Mumbai (Sewri)'
when lower(wh_location) = 'mumshi' then 'Mumbai (Sion)'
end as wh_location1,
-- nov,
-- dec,
jan,
pg_extracted_at,
row_number() over(partition by wh_location order by pg_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_nykaa.fill_summary
)
where rn = 1 and date(pg_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.wh_location = source.wh_location
and target.pg_extracted_at = source.pg_extracted_at
when matched and target.pg_extracted_at < source.pg_extracted_at
then update set
target.wh_location = source.wh_location,
target.wh_location1 = source.wh_location1,
target.jan = source.jan,
target.pg_extracted_at = source.pg_extracted_at
when not matched
then insert
(
wh_location,
wh_location1,
jan,
pg_extracted_at
)
values
(
source.wh_location,
source.wh_location1,
source.jan,
source.pg_extracted_at
)