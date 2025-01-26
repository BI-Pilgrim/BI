MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.order_status_metrics` AS target
USING `shopify-pubsub-project.easycom.order_status_metrics` AS source
ON target.date = source.date AND target.status = source.status

-- Insert new rows that do not already exist in the target table
WHEN NOT MATCHED THEN
INSERT (date, status, count, analysis_date)
VALUES (source.date, source.status, source.count, source.analysis_date);
