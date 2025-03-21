MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_budget_rules` AS target
USING (
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
    DATE(TIMESTAMP_MICROS(CAST(createdDate AS INT64) * 1000)) AS lastUpdatedDate
  FROM 
    `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_display_budget_rules`
  WHERE 
    _airbyte_extracted_at > (
      SELECT MAX(_airbyte_extracted_at) 
      FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_budget_rules`
    )
) AS source
ON FALSE
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    ruleId,
    ruleState,
    ruleStatus,
    createdDate,
    budget_increase_value_pct,
    duration_start_date,
    duration_end_date,
    name,
    performance_measure_condition,
    recurrence_days_of_week,
    recurrence_intraday_start_time,
    recurrence_intraday_end_time,
    recurrence_type,
    rule_type,
    lastUpdatedDate
  )
  VALUES (
    source._airbyte_extracted_at,
    source.ruleId,
    source.ruleState,
    source.ruleStatus,
    source.createdDate,
    source.budget_increase_value_pct,
    source.duration_start_date,
    source.duration_end_date,
    source.name,
    source.performance_measure_condition,
    source.recurrence_days_of_week,
    source.recurrence_intraday_start_time,
    source.recurrence_intraday_end_time,
    source.recurrence_type,
    source.rule_type,
    source.lastUpdatedDate
  );
