CREATE TABLE pilgrim_bi_purple.claims_report(
runid bigint,
date date,
brand_name string,
ean_code string,
sku string,
nmv Numeric(12,4),
mrp Numeric(10,2),
qty int,
orders int,
price_off_per Numeric(12,4),
abs_price_off Numeric(12,4),
off_invoice Numeric(12,4)
)
partition by (date)
cluster by (runid)