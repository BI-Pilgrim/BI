from datetime import datetime, timedelta
from dispatch_timing_analyzer import DispatchTimingAnalyzer
import logging
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def process_and_store_data():
    """Process TAT data and store in BigQuery"""
    try:
        logging.info("Starting data processing")
        # Calculate date range (last 60 days)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=60)
        
        # Initialize analyzer
        analyzer = DispatchTimingAnalyzer(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )
        
        # Get analysis results
        logging.info(f"Analyzing data from {start_date} to {end_date}")
        results = analyzer.analyze_dispatch_compliance()
        
        # Prepare daily metrics data
        daily_rows = []
        for metric in results['daily_metrics']:
            daily_rows.append({
                'date': metric['date'],
                'total_orders': metric['total_orders'],
                'orders_before_cutoff': metric['orders_before_cutoff'],
                'orders_after_cutoff': metric['orders_after_cutoff'],
                'total_due_dispatches': metric['total_due_dispatches'],
                'delayed_dispatch_count': metric['delayed_dispatch_count'],
                'order_status_breakdown': str(metric['order_status_breakdown']),
                'shipping_status_breakdown': str(metric['shipping_status_breakdown']),
                'analysis_date': datetime.now()
            })
        
        # Load daily metrics
        analyzer.load_data_to_bigquery(daily_rows, 'daily_metrics')
        
        # Prepare and load warehouse metrics
        warehouse_rows = []
        for metric in results['warehouse_metrics']:
            for date in pd.date_range(start_date, end_date):
                warehouse_rows.append({
                    'warehouse_id': metric['warehouse_id'],
                    'warehouse_name': metric['warehouse_name'],
                    'total_orders': metric['total_orders'],
                    'delayed_orders': metric['delayed_orders'],
                    'compliance_rate': metric['compliance_rate'],
                    'avg_delay_hours': metric['avg_delay_hours'],
                    'median_delay_hours': metric['median_delay_hours'],
                    'delay_distribution': str(metric['delay_distribution']),
                    'analysis_date': datetime.now(),
                    'date': date.date()
                })
        
        analyzer.load_data_to_bigquery(warehouse_rows, 'warehouse_metrics')
        
        logging.info("Successfully completed data processing")
    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        raise

if __name__ == "__main__":
    process_and_store_data() 