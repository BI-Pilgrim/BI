
MERGE INTO  `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Smart_collections` AS target

USING (
WITH expanded_rules AS (
  SELECT 
    id AS collection_id,
    _airbyte_extracted_at,
    title AS collection_title,
    handle AS collection_handle,
    shop_url,
    body_html,
    sort_order,
    updated_at AS collection_updated_at,
    disjunctive,
    published_at,
    published_scope,
    template_suffix,
    admin_graphql_api_id,
    rules,
    JSON_EXTRACT_SCALAR(rule, '$') AS raw_rule
  FROM
    `shopify-pubsub-project.pilgrim_bi_airbyte.smart_collections`,
    UNNEST(JSON_EXTRACT_ARRAY(rules)) AS rule
),
flattened_rules AS (
  SELECT 
    er.collection_id,
    er._airbyte_extracted_at,
    er.collection_title,
    er.collection_handle,
    er.shop_url,
    er.body_html,
    er.sort_order,
    er.collection_updated_at,
    er.disjunctive,
    er.published_at,
    er.published_scope,
    er.template_suffix,
    er.admin_graphql_api_id,
    REGEXP_REPLACE(raw_rule, r"'", '"') AS fixed_rule
  FROM 
    expanded_rules AS er
)
SELECT DISTINCT
  _airbyte_extracted_at,
  collection_id,
  collection_title,
  collection_handle,
  shop_url,
  body_html,
  sort_order,
  collection_updated_at,
  disjunctive,
  published_at,
  published_scope,
  template_suffix,
  admin_graphql_api_id,
  JSON_EXTRACT_SCALAR(fixed_rule, '$.column') AS type,
  JSON_EXTRACT_SCALAR(fixed_rule, '$.relation') AS relation,
  JSON_EXTRACT_SCALAR(fixed_rule, '$.condition') AS condition
FROM 
  flattened_rules

WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.collection_id = source.collection_id and 
target.condition = source.condition

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target.collection_id = source.collection_id,
target.collection_title = source.collection_title,
target.collection_handle = source.collection_handle,
target.shop_url = source.shop_url,
target.body_html = source.body_html,
target.sort_order = source.sort_order,
target.collection_updated_at = source.collection_updated_at,
target.disjunctive = source.disjunctive,
target.published_at = source.published_at,
target.published_scope = source.published_scope,
target.template_suffix = source.template_suffix,
target.admin_graphql_api_id = source.admin_graphql_api_id,
target.type = source.type,
target.relation = source.relation,
target.condition = source.condition


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
collection_id,
collection_title,
collection_handle,
shop_url,
body_html,
sort_order,
collection_updated_at,
disjunctive,
published_at,
published_scope,
template_suffix,
admin_graphql_api_id,
type,
relation,
condition

 )

  VALUES (
source._airbyte_extracted_at,
source.collection_id,
source.collection_title,
source.collection_handle,
source.shop_url,
source.body_html,
source.sort_order,
source.collection_updated_at,
source.disjunctive,
source.published_at,
source.published_scope,
source.template_suffix,
source.admin_graphql_api_id,
source.type,
source.relation,
source.condition
  )
