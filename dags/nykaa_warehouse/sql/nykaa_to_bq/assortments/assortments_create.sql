create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.assortments`
partition by mail_received_at
as
select
* except(rn)
from
(
select
brand_name,
sku_desc,
pack_size,
ean_code,
hsn_code,
sku_status,
coalesce(cast(mrp as float64),0) mrp,
coalesce(ahd, 0) ahd,
coalesce(blr, 0) blr,
coalesce(blr1, 0) blr1,
coalesce(blr2, 0) blr2,
coalesce(blrbeg, 0) blrbeg,
coalesce(blrdee, 0) blrdee,
coalesce(blryel, 0) blryel,
coalesce(bvi, 0) bvi,
coalesce(chn, 0) chn,
coalesce(cwh, 0) cwh,
coalesce(ded, 0) ded,
coalesce(del1, 0) del1,
coalesce(del2, 0) del2,
coalesce(ggn, 0) ggn,
coalesce(gkp, 0) gkp,
coalesce(guw, 0) guw,
coalesce(hyd, 0) hyd,
coalesce(ind, 0) ind,
coalesce(kkj, 0) kkj,
coalesce(kol1, 0) kol1,
coalesce(kol2, 0) kol2,
coalesce(mtl, 0) mtl,
coalesce(mum1_new, 0) mum1_new,
coalesce(mum2, 0) mum2,
coalesce(mumand, 0) mumand,
coalesce(mumtha, 0) mumtha,
coalesce(ncrpit, 0) ncrpit,
coalesce(nda, 0) nda,
coalesce(ngp, 0) ngp,
coalesce(nwl, 0) nwl,
coalesce(patna, 0) patna,
coalesce(pun, 0) pun,
coalesce(rjg, 0) rjg,
coalesce(rtn, 0) rtn,
coalesce(tauru, 0) tauru,
coalesce(wht, 0) wht,
coalesce(grand_total, 0) grand_total,
pg_extracted_at,
coalesce(mumsew, 0) mumsew,
coalesce(mumshi, 0) mumshi,
sku_code,
sku_velocity,
RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
DATE(pg_mail_recieved_at) AS mail_received_at,
FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS report_day,
row_number() over(partition by ean_code,sku_status,RIGHT(TRIM(pg_mail_subject), 6) order by DATE(pg_mail_recieved_at)) as rn
from shopify-pubsub-project.pilgrim_bi_nykaa.assortments
-- where ean_code = '8904482500008' and sku_status = 'Active'
-- order by ean_code
)
where rn = 1