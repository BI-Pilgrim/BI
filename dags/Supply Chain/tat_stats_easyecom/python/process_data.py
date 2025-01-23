from datetime import datetime, timedelta
from dispatch_timing_analyzer import DispatchTimingAnalyzer
import logging
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def calculate_avg_delay(delayed_dispatch_details):
    """Calculate average delay in hours from delayed dispatch details"""
    if not delayed_dispatch_details:
        return 0.0
    
    delays = []
    for order in delayed_dispatch_details:
        if isinstance(order, dict):
            if 'actual_dispatch' in order and 'expected_dispatch' in order:
                try:
                    actual = datetime.strptime(order['actual_dispatch'], '%Y-%m-%d')
                    expected = datetime.strptime(order['expected_dispatch'], '%Y-%m-%d')
                    delay_hours = (actual - expected).total_seconds() / 3600
                    delays.append(delay_hours)
                except (ValueError, TypeError):
                    continue
    
    return float(round(sum(delays) / len(delays), 1)) if delays else 0.0

def calculate_median_delay(delayed_dispatch_details):
    """Calculate median delay in hours from delayed dispatch details"""
    if not delayed_dispatch_details:
        return 0.0
    
    delays = []
    for order in delayed_dispatch_details:
        if isinstance(order, dict):
            if 'actual_dispatch' in order and 'expected_dispatch' in order:
                try:
                    actual = datetime.strptime(order['actual_dispatch'], '%Y-%m-%d')
                    expected = datetime.strptime(order['expected_dispatch'], '%Y-%m-%d')
                    delay_hours = (actual - expected).total_seconds() / 3600
                    delays.append(delay_hours)
                except (ValueError, TypeError):
                    continue
    
    if not delays:
        return 0.0
    
    sorted_delays = sorted(delays)
    mid = len(sorted_delays) // 2
    
    if len(sorted_delays) % 2 == 0:
        return float(round((sorted_delays[mid - 1] + sorted_delays[mid]) / 2, 1))
    return float(round(sorted_delays[mid], 1))

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
        
        # Update daily metrics preparation
        daily_rows = []
        for metric in results['daily_metrics']:
            total_orders = metric['total_orders']
            delayed_orders = metric['delayed_dispatch_count']
            compliance_rate = ((total_orders - delayed_orders) / total_orders * 100) if total_orders > 0 else 0
            
            daily_rows.append({
                'date': metric['date'],
                'total_orders': metric['total_orders'],
                'delayed_dispatch_count': metric['delayed_dispatch_count'],
                'compliance_rate': compliance_rate,
                'avg_delay_hours': calculate_avg_delay(metric['delayed_dispatch_details']),
                'median_delay_hours': calculate_median_delay(metric['delayed_dispatch_details']),
                'order_status_breakdown': str(metric['order_status_breakdown']),
                'shipping_status_breakdown': str(metric['shipping_status_breakdown']),
                'analysis_date': datetime.now()
            })
        
        # Get raw data for warehouse metrics calculation
        raw_data = analyzer.fetch_order_dispatch_data()
        
        # Prepare warehouse metrics by date and location
        warehouse_rows = []
        for date in pd.date_range(start_date, end_date):
            date_str = date.strftime('%Y-%m-%d')
            # Filter data for current date
            date_data = raw_data[raw_data['business_date'].dt.date == date.date()]
            
            # Group by location for the specific date
            location_groups = date_data.groupby(['location_key', 'import_warehouse_name'])
            
            for (location_key, warehouse_name), group in location_groups:
                total_orders = len(group)
                if total_orders == 0:
                    continue
                    
                delayed_orders = len(group[group['is_delayed']])
                compliance_rate = ((total_orders - delayed_orders) / total_orders * 100) if total_orders > 0 else 0
                
                # Calculate delays only for delayed orders
                delayed_group = group[group['is_delayed']]
                avg_delay = delayed_group['delay_hours'].mean() if len(delayed_group) > 0 else 0
                median_delay = delayed_group['delay_hours'].median() if len(delayed_group) > 0 else 0
                
                warehouse_rows.append({
                    'location_key': location_key,
                    'date': date.date(),
                    'warehouse_name': warehouse_name,
                    'total_orders': total_orders,
                    'delayed_orders': delayed_orders,
                    'compliance_rate': round(compliance_rate, 2),
                    'avg_delay_hours': round(float(avg_delay), 1),
                    'median_delay_hours': round(float(median_delay), 1),
                    'analysis_date': datetime.now(),
                })
        
        # Load all data
        analyzer.load_data_to_bigquery(daily_rows, 'daily_metrics')
        analyzer.load_data_to_bigquery(warehouse_rows, 'warehouse_metrics')
        
        logging.info("Successfully completed data processing")
    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        raise


if __name__ == "__main__":
    process_and_store_data() 