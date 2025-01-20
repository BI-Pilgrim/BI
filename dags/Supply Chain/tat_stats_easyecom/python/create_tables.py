from google.cloud import bigquery
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def create_bigquery_tables():
    """Create BigQuery tables if they don't exist"""
    try:
        logging.info("Starting BigQuery table creation")
        client = bigquery.Client()
        dataset_ref = client.dataset('easycom')
        
        # Define table schemas
        daily_metrics_schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("total_orders", "INTEGER"),
            bigquery.SchemaField("orders_before_cutoff", "INTEGER"),
            bigquery.SchemaField("orders_after_cutoff", "INTEGER"),
            bigquery.SchemaField("total_due_dispatches", "INTEGER"),
            bigquery.SchemaField("delayed_dispatch_count", "INTEGER"),
            bigquery.SchemaField("order_status_breakdown", "STRING"),
            bigquery.SchemaField("shipping_status_breakdown", "STRING"),
            bigquery.SchemaField("analysis_date", "TIMESTAMP")
        ]
        
        warehouse_metrics_schema = [
            bigquery.SchemaField("warehouse_id", "STRING"),
            bigquery.SchemaField("warehouse_name", "STRING"),
            bigquery.SchemaField("total_orders", "INTEGER"),
            bigquery.SchemaField("delayed_orders", "INTEGER"),
            bigquery.SchemaField("compliance_rate", "FLOAT"),
            bigquery.SchemaField("avg_delay_hours", "FLOAT"),
            bigquery.SchemaField("median_delay_hours", "FLOAT"),
            bigquery.SchemaField("delay_distribution", "STRING"),
            bigquery.SchemaField("analysis_date", "TIMESTAMP"),
            bigquery.SchemaField("date", "DATE")
        ]
        
        tables = {
            'daily_metrics': daily_metrics_schema,
            'warehouse_metrics': warehouse_metrics_schema
        }
        
        for table_name, schema in tables.items():
            table_ref = dataset_ref.table(table_name)
            if table_ref.exists():
                logging.info(f"Table {table_name} already exists")
                continue
            table = bigquery.Table(table_ref, schema=schema)
            try:
                client.create_table(table)
                logging.info(f"Created table {table_name}")
            except Exception as e:
                logging.error(f"Error creating table {table_name}: {e}")
                raise
        
        logging.info("Successfully created/verified BigQuery tables")
    except Exception as e:
        logging.error(f"Error creating BigQuery tables: {str(e)}")
        raise

if __name__ == "__main__":
    create_bigquery_tables() 