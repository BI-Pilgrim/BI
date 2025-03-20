CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_budget_rules` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT 
_airbyte_extracted_at,
ruleId,
ruleState,
ruleStatus,
DATE(TIMESTAMP_MICROS(CAST(createdDate AS INT64) * 1000)) AS createdDate,
JSON_EXTRACT_SCALAR(ruleDetails, '$.budgetIncreaseBy.value') AS budget_increase_value_pct,
JSON_EXTRACT_SCALAR(ruleDetails, '$.duration.dateRangeTypeRuleDuration.startDate') AS duration_start_date,
JSON_EXTRACT_SCALAR(ruleDetails, '$.duration.dateRangeTypeRuleDuration.endDate') AS duration_end_date,
JSON_EXTRACT_SCALAR(ruleDetails, '$.name') AS name,
JSON_EXTRACT_SCALAR(ruleDetails, '$.performanceMeasureCondition') AS performance_measure_condition,
JSON_EXTRACT_SCALAR(ruleDetails, '$.recurrence.daysOfWeek') AS recurrence_days_of_week,
JSON_EXTRACT_SCALAR(ruleDetails, '$.recurrence.intraDaySchedule[0].startTime') AS recurrence_intraday_start_time,
JSON_EXTRACT_SCALAR(ruleDetails, '$.recurrence.intraDaySchedule[0].endTime') AS recurrence_intraday_end_time,
JSON_EXTRACT_SCALAR(ruleDetails, '$.recurrence.type') AS recurrence_type,
JSON_EXTRACT_SCALAR(ruleDetails, '$.ruleType') AS rule_type,
DATE(TIMESTAMP_MICROS(CAST(createdDate AS INT64) * 1000)) AS lastUpdatedDate,
FROM (
  SELECT *, 
  ROW_NUMBER() OVER(PARTITION BY ruleId) as rn
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_display_budget_rules`
)