create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.Nykaa_sanity_check` as
with Sources as
  (
  select 
  'sku_lvl_dashboard' as table_name,
  'sku_lvl_dashboard' as source_table,
  max(date(last_grndate)) as Source_max_date,
  
  count(distinct case when date(last_grndate) = (select max(date(last_grndate)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_lvl_dashboard`) then (sku_status||ean_code||last_grndate||pack_size||RIGHT(TRIM(pg_mail_subject),6)) end) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.sku_lvl_dashboard`

  union all

  select 
  'fill_summary' as table_name,
  'fill_summary' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.fill_summary`) then concat(wh_location,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.fill_summary`

  union all

  select 
  'sku_level_fill' as table_name,
  'sku_level_fill' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_level_fill`) then concat(sku_code,pack_size,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.sku_level_fill`

  union all

  select 
  'inv_ageing' as table_name,
  'inv_ageing' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.inv_ageing`) then concat(ageing_bucket,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.inv_ageing`

  union all

  -- select 
  -- 'open_po_summary' as table_name,
  -- 'open_po_summary' as source_table,
  -- max(date(event_time)) as Source_max_date,
  
  -- count(distinct case when date(event_time) = (select max(date(event_time)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.open_po_summary`) then concat(wh_location,po_aging,current_status, appointment_date, RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  -- from `shopify-pubsub-project.pilgrim_bi_nykaa.open_po_summary`

  -- union all

  select 
  'sku_inv' as table_name,
  'sku_inv' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_inv`) then concat(brand_name,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.sku_inv`

  union all

  select 
  'assortments' as table_name,
  'assortments' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.assortments`) then concat(ean_code,sku_status,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.assortments`

  union all

  select 
  'open_rtv' as table_name,
  'open_rtv' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.open_rtv`) then concat(rtv_no,product_sku,rtv_status,reason,location_name,rtv_type,rtv_ageing,vendor_code,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.open_rtv`

  union all

  select 
  'appointment_adherence' as table_name,
  'appointment_adherence' as source_table,
  max(date(confirm_appointment_date)) as Source_max_date,
  
  count(distinct case when date(confirm_appointment_date) = (select max(date(confirm_appointment_date)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.appointment_adherence`) then concat(po_no,confirm_appointment_date,wh_remarks,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.appointment_adherence`

  union all

-- Since ads_recommendations table is derived from ads therefor count(distinct pk) of source and Stagingination are same
  select 
  'inward_discrepancy' as table_name,
  'inward_discrepancy' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.inward_discrepancy`) then concat(product_sku,location_name,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.inward_discrepancy`

  union all

-- Since ads_recommendations table is derived from ads therefor count(distinct pk) of source and Stagingination are same
  select 
  'brand_lvl_dashboard' as table_name,
  'brand_lvl_dashboard' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.brand_lvl_dashboard`) then concat(brand_name, RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.brand_lvl_dashboard`

  union all

-- Since ads_recommendations table is derived from ads therefor count(distinct pk) of source and Stagingination are same
  select 
  'velocity_lvl_dashboard' as table_name,
  'velocity_lvl_dashboard' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.velocity_lvl_dashboard`) then concat(brand_name,sku_velocity,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.velocity_lvl_dashboard`

  union all

-- Since ads_recommendations table is derived from ads therefor count(distinct pk) of source and Stagingination are same
  select 
  'grn_details' as table_name,
  'grn_details' as source_table,
  max(date(pg_extracted_at)) as Source_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.grn_details`) then concat(pocode,sku_code,po_date,RIGHT(TRIM(pg_mail_subject),6)) end ) as Source_pk_count
  from `shopify-pubsub-project.pilgrim_bi_nykaa.grn_details`

),

---------------------------------------------------------------------------------------------------------------------------------------------------------
Staging as 
(
  select 
  'sku_lvl_dashboard' as table_name,
  max(date(last_grndate)) as Staging_max_date,
  -- max(date(event_time)) as Latest_Valid_Date,
  count(distinct case when date(last_grndate) = (select max(date(last_grndate)) from `shopify-pubsub-project.pilgrim_bi_nykaa.sku_lvl_dashboard`) then (sku_status||ean_code||last_grndate||pack_size||reporting_week) end) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_lvl_dashboard`

  union all

  select 
  'fill_summary' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  -- max(date(pg_extracted_at)) as Latest_Valid_Date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.fill_summary`) then concat(wh_location,reporting_week) end) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.fill_summary`

  union all

  select 
  'sku_level_fill' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  -- max(date(event_time)) as Latest_Valid_Date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.sku_level_fill`) then concat(sku_code,pack_size,reporting_week) end) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_level_fill`

  union all

  select
  'inv_ageing' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.inv_ageing`) then concat(ageing_bucket,reporting_week) end) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.inv_ageing`

  union all

  -- select 
  -- 'open_po_summary' as table_name,
  -- max(date(event_time)) as Staging_max_date,
  -- count(distinct case when date(event_time) = (select max(date(event_time)) from `shopify-pubsub-project.pilgrim_bi_nykaa.open_po_summary`) then concat(object_id, event_time, account_id) end ) as Staging_pk_count
  -- from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.open_po_summary`

  -- union all

  select 
  'sku_inv' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.sku_inv`) then concat(brand_name,reporting_week) end) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_inv`

  union all

  select 
  'assortments' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.assortments`) then concat(ean_code,sku_status,reporting_week) end) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.assortments`

  union all

  select 
  'open_rtv' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.open_rtv`) then concat(rtv_no,product_sku,rtv_status,reason,location_name,rtv_type,rtv_ageing,vendor_code,reporting_week) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.open_rtv`

  union all

  select 
  'appointment_adherence' as table_name,
  max(date(confirm_appointment_date)) as Staging_max_date,
  count(distinct case when date(confirm_appointment_date) = (select max(date(confirm_appointment_date)) from `shopify-pubsub-project.pilgrim_bi_nykaa.appointment_adherence`) then concat(po_no,confirm_appointment_date,wh_remarks,reporting_week) end) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.appointment_adherence`

  union all

  select 
  'inward_discrepancy' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.inward_discrepancy`) then concat(product_sku,location_name,reporting_week) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.inward_discrepancy`

  union all

  select
  'brand_lvl_dashboard' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.brand_lvl_dashboard`) then concat(brand_name, reporting_week) end) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.brand_lvl_dashboard`

  union all

  select
  'velocity_lvl_dashboard' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.velocity_lvl_dashboard`) then concat(brand_name,sku_velocity,reporting_week) end) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.velocity_lvl_dashboard`

  union all

  select 
  'grn_details' as table_name,
  max(date(pg_extracted_at)) as Staging_max_date,
  count(distinct case when date(pg_extracted_at) = (select max(date(pg_extracted_at)) from `shopify-pubsub-project.pilgrim_bi_nykaa.grn_details`) then concat(pocode,sku_code,po_date,reporting_week) end ) as Staging_pk_count
  from  `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.grn_details`

)


  select
    So.table_name,
    So.source_table,
    Date(So.Source_max_date) as Source_max_date,
    Date(St.Staging_max_date) as Staging_max_date,
    Current_date() as Date1,
    So.Source_pk_count,
    St.Staging_pk_count,

  from Sources as So
  left join Staging as St
  on So.table_name = St.table_name
  -- where Source_max_date < Latest_Valid_Date
  order by table_name