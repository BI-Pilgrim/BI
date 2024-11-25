create or replace table pilgrim_bi_flipkart_seller.earn_more_report(
    runid bigint,
    product_id string,
    sku_id string,
    category string,
    brand string,
    vertical string,
    order_date date,
    fulfillment_type string,
    location_id string,
    gross_units int,
    gmv int,
    cancellation_units int,
    cancellation_amount numeric(10,4),
    return_units int,
    return_amount numeric(10,4),
    final_sale_units int,
    final_sale_amount numeric(10,4)
) 
partition by (order_date)
cluster by (runid);