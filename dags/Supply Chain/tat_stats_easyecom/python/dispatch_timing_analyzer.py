from datetime import datetime, time, timedelta
import pandas as pd
from google.cloud import bigquery
import os
import json
import logging
import base64
from sqlalchemy import create_engine
from google.oauth2 import service_account


class DispatchTimingAnalyzer:
    def __init__(self, start_date, end_date):
        try:
            self.start_date = start_date
            self.end_date = end_date
            self.CUTOFF_TIME = time(14, 00)  # 2:00 PM
            self.analysis_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            self.ALLOWED_LOCATION_KEYS = [
                'en9567578596',
                'ne27702606481',
                'en3857776321',
                'en11609416009'
            ]
            
            # Initialize BigQuery connection similar to get_orders.py
            self.project_id = "shopify-pubsub-project"
            self.dataset_id = "easycom"
            
            # Get credentials from environment similar to get_orders
            credentials_info = self.get_google_credentials_info()
            credentials_info = base64.b64decode(credentials_info).decode("utf-8")
            credentials_info = json.loads(credentials_info)

            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
            
            # Create engine for SQLAlchemy operations if needed
            connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"
            self.engine = create_engine(connection_string, credentials_info=credentials_info)
            
        except Exception as e:
            logging.error(f"Error initializing DispatchTimingAnalyzer: {str(e)}")
            raise

    def get_google_credentials_info(self):
        """Get Google credentials from environment variables"""
        return os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')

    def load_data_to_bigquery(self, data, table_name):
        """Load data to BigQuery using truncate and load pattern"""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            
            # Convert data to DataFrame
            df = pd.DataFrame(data)
            
            # Configure job to truncate existing data before loading
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # This ensures table is emptied before loading
            )

            # Load data
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for job to complete

            logging.info(f"Truncated existing data and loaded {len(data)} new rows into {table_id}")
            
        except Exception as e:
            logging.error(f"Error loading data to BigQuery: {str(e)}")
            raise

    def create_table(self, table_name, schema):
        """Create BigQuery table if it doesn't exist"""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            table = bigquery.Table(table_id, schema=schema)
            
            try:
                self.client.get_table(table_id)
                logging.info(f"Table {table_id} already exists")
            except Exception:
                self.client.create_table(table)
                logging.info(f"Created table {table_id}")
                
        except Exception as e:
            logging.error(f"Error creating table: {str(e)}")
            raise

    def fetch_order_dispatch_data(self):
        """Fetch order and dispatch data directly from BigQuery"""
        try:
            logging.info(f"Fetching order dispatch data from {self.start_date} to {self.end_date}")
            start_date_obj = datetime.strptime(self.start_date, '%Y-%m-%d')
            # Get data from one day before to account for 2 PM cutoff
            previous_day = (start_date_obj - timedelta(days=1)).strftime('%Y-%m-%d')
            
            query = f"""
            WITH filtered_orders AS (
                SELECT 
                    order_id,
                    order_date,
                    invoice_date,
                    manifest_date,
                    order_status,
                    shipping_status,
                    last_update_date,
                    location_key,
                    import_warehouse_name,
                    -- Create a new column for the business day (2 PM to 2 PM next day)
                    CASE
                        WHEN TIME(order_date) >= '14:00:00'
                        THEN DATE(order_date)
                        ELSE DATE_SUB(DATE(order_date), INTERVAL 1 DAY)
                    END as business_date
                FROM `shopify-pubsub-project.easycom.orders`
                WHERE DATE(order_date) BETWEEN '{previous_day}' AND '{self.end_date}'
                AND order_status != 'Cancelled'
                AND location_key IN UNNEST({self.ALLOWED_LOCATION_KEYS})
                AND order_date IS NOT NULL
            )
            SELECT *
            FROM filtered_orders
            WHERE business_date >= '{self.start_date}'
            ORDER BY order_date
            """
            
            # Execute query directly in BigQuery
            query_job = self.client.query(query)
            df = query_job.to_dataframe()
            
            logging.info(f"Retrieved {len(df) if df is not None else 0} rows of data")
            if df is not None and not df.empty:
                df = df[df['order_date'].notna()]
                df = df[df['order_date'] <= datetime.now()]
                
                # Pre-calculate commonly used datetime conversions
                df['order_date'] = pd.to_datetime(df['order_date'])
                df['manifest_date'] = pd.to_datetime(df['manifest_date'])
                df['business_date'] = pd.to_datetime(df['business_date'])
            
            return df
        except Exception as e:
            logging.error(f"Error fetching order dispatch data: {str(e)}")
            raise
    
    def analyze_dispatch_compliance(self):
        """Analyze dispatch timing compliance"""
        df = self.fetch_order_dispatch_data()
        
        # Add better error handling for empty dataframe
        if df is None or df.empty:
            return {
                'daily_metrics': [],
                'summary': {
                    'total_orders': 0,
                    'total_orders_before_cutoff': 0,
                    'total_delayed_dispatches': 0,
                    'compliance_rate': 0,
                    'delay_statistics': {
                        'average_delay': 0,
                        'median_delay': 0,
                        'min_delay': 0,
                        'max_delay': 0,
                        'delay_distribution': {
                            'q25': 0,
                            'q75': 0
                        }
                    },
                    'analysis_period': {
                        'start_date': self.start_date,
                        'end_date': self.end_date
                    }
                }
            }
        
        # Convert date columns to datetime
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['manifest_date'] = pd.to_datetime(df['manifest_date'])
        
        # Calculate expected dispatch date based on invoice time
        df['expected_dispatch_date'] = df.apply(self._calculate_expected_dispatch, axis=1)
        
        # Calculate actual dispatch date and delays - handle NaT values
        df['actual_dispatch_date'] = df['manifest_date'].dt.date.fillna(pd.NaT)
        
        # Update is_delayed calculation to handle NaT values
        df['is_delayed'] = df.apply(
            lambda x: (x['actual_dispatch_date'] > x['expected_dispatch_date'] 
                      if pd.notnull(x['actual_dispatch_date']) 
                      else True),  # Consider unmanifested orders as delayed
            axis=1
        )
        
        # Update delay calculations to handle NaT values
        df['delay_days'] = df.apply(
            lambda x: ((x['actual_dispatch_date'] - x['expected_dispatch_date']).days
                      if pd.notnull(x['actual_dispatch_date']) and 
                         x['actual_dispatch_date'] > x['expected_dispatch_date']
                      else 0),
            axis=1
        )
        
        # Calculate delay hours for delayed orders
        def calculate_delay_hours(row):
            if pd.notnull(row['manifest_date']) and row['is_delayed']:
                # Get cutoff time on expected dispatch date
                cutoff_datetime = datetime.combine(row['expected_dispatch_date'], self.CUTOFF_TIME)
                delay = row['manifest_date'] - cutoff_datetime
                return delay.total_seconds() / 3600
            elif row['is_delayed']:
                cutoff_datetime = datetime.combine(row['expected_dispatch_date'], self.CUTOFF_TIME)
                delay = datetime.now() - cutoff_datetime
                return delay.total_seconds() / 3600  
            else:
                return 0

        df['delay_hours'] = df.apply(calculate_delay_hours, axis=1)
        
        daily_metrics, warehouse_metrics = self._generate_analysis_report(df)
        
        # Create the complete analysis result
        analysis_result = {
            'daily_metrics': daily_metrics,
            'warehouse_metrics': warehouse_metrics,
            'summary': self._calculate_summary(daily_metrics, df)
        }
        
        return analysis_result
    
    def _calculate_expected_dispatch(self, row):
        """Calculate expected dispatch date based on order time"""
        order_datetime = row['order_date']
        order_time = order_datetime.time()
        
        # If order is placed after cutoff, expect dispatch next business day
        if order_time > self.CUTOFF_TIME:
            return (order_datetime + timedelta(days=1)).date()
        return order_datetime.date()
    
    def _generate_analysis_report(self, df):
        """Generate detailed analysis report with optimized calculations"""
        daily_metrics = []
        
        # Pre-filter display_df once
        display_df = df[df['business_date'].dt.date >= datetime.strptime(self.start_date, '%Y-%m-%d').date()].copy()
        
        # Group by business_date instead of order_date_only
        grouped = display_df.groupby('business_date')
        
        # Calculate warehouse metrics first
        warehouse_metrics = self._analyze_warehouse_performance(display_df)
        
        for business_date, group in grouped:
            business_date = business_date.date()
            
            # Get all orders for this business day (already filtered from 2 PM to 2 PM)
            total_orders = len(group)
            
            # Calculate delayed orders
            delayed_orders = group[group['is_delayed'] & group['manifest_date'].notna()]
            
            # Status breakdowns for the business day
            status_breakdown = group['order_status'].value_counts().to_dict()
            status_breakdown = {str(k): int(v) for k, v in status_breakdown.items()}

            shipping_status_breakdown = group['shipping_status'].value_counts().to_dict()
            shipping_status_breakdown = {str(k): int(v) for k, v in shipping_status_breakdown.items()}
            
            metrics = {
                'date': business_date.strftime('%Y-%m-%d'),
                'total_orders': int(total_orders),
                'total_due_dispatches': int(total_orders),  # All orders in the period are due
                'delayed_dispatch_count': int(len(delayed_orders)),
                'delayed_dispatch_details': self._get_delay_breakdown(delayed_orders),
                'order_status_breakdown': status_breakdown,
                'shipping_status_breakdown': shipping_status_breakdown
            }
            
            daily_metrics.append(metrics)
        
        return daily_metrics, warehouse_metrics
    
    def _get_delay_breakdown(self, delayed_orders):
        """Get detailed breakdown of delayed orders"""
        if delayed_orders.empty:
            return []
            
        details = []
        for _, order in delayed_orders.iterrows():
            details.append({
                'order_id': order['order_id'],
                'order_datetime': order['order_date'].strftime('%Y-%m-%d %H:%M:%S'),
                'expected_dispatch': order['expected_dispatch_date'].strftime('%Y-%m-%d'),
                'actual_dispatch': order['actual_dispatch_date'].strftime('%Y-%m-%d') if pd.notnull(order['manifest_date']) else 'Not Dispatched',
                'delay_days': order['delay_days'] if pd.notnull(order['delay_days']) else 'N/A',
                'current_status': order['order_status']
            })
        return details
    
    def _calculate_summary(self, daily_metrics, df):
        """Calculate overall summary statistics"""
        # Convert numpy values to native Python types
        total_orders = int(sum(day['total_orders'] for day in daily_metrics))
        total_delayed = int(sum(day['delayed_dispatch_count'] for day in daily_metrics))
        
        # Handle delayed orders calculations
        delayed_orders = df[df['is_delayed']]
        delay_stats = {
            'mean_delay_hours': float(delayed_orders['delay_hours'].mean()) if not delayed_orders.empty else 0.0,
            'median_delay_hours': float(delayed_orders['delay_hours'].median()) if not delayed_orders.empty else 0.0,
            'std_delay_hours': float(delayed_orders['delay_hours'].std()) if not delayed_orders.empty else 0.0,
            'variance_delay_hours': float(delayed_orders['delay_hours'].var()) if not delayed_orders.empty else 0.0,
            'q25_delay_hours': float(delayed_orders['delay_hours'].quantile(0.25)) if not delayed_orders.empty else 0.0,
            'q75_delay_hours': float(delayed_orders['delay_hours'].quantile(0.75)) if not delayed_orders.empty else 0.0,
            'min_delay_hours': float(delayed_orders['delay_hours'].min()) if not delayed_orders.empty else 0.0,
            'max_delay_hours': float(delayed_orders['delay_hours'].max()) if not delayed_orders.empty else 0.0
        }

        return {
            'total_orders': total_orders,
            'total_delayed_dispatches': total_delayed,
            'compliance_rate': float(round((1 - total_delayed / total_orders) * 100, 2)) if total_orders > 0 else 0.0,
            'delay_statistics': delay_stats,
            'analysis_period': {
                'start_date': self.start_date,
                'end_date': self.end_date
            }
        }
    
    def _analyze_warehouse_performance(self, df):
        """Optimized warehouse performance analysis"""
        metrics = []
        
        # Vectorized calculations
        grouped = df.groupby(['location_key', 'import_warehouse_name'])
        
        for (location_key, warehouse_name), group in grouped:
            delayed_mask = group['is_delayed']
            delayed_orders = group[delayed_mask]
            
            total_orders = int(len(group))  # Convert to int
            total_delayed = int(delayed_mask.sum())  # Convert to int
            
            delay_hours = delayed_orders['delay_hours']
            
            # Handle empty delay_hours case
            if len(delay_hours) > 0:
                delay_stats = {
                    'q25': float(round(delay_hours.quantile(0.25), 1)),
                    'q75': float(round(delay_hours.quantile(0.75), 1)),
                    'min': float(round(delay_hours.min(), 1)),
                    'max': float(round(delay_hours.max(), 1))
                }
            else:
                delay_stats = {'q25': 0.0, 'q75': 0.0, 'min': 0.0, 'max': 0.0}
            
            metrics.append({
                'location_key': str(location_key),
                'warehouse_name': str(warehouse_name),
                'total_orders': total_orders,
                'delayed_orders': total_delayed,
                'compliance_rate': float(round(100 * (1 - total_delayed / total_orders), 2)) if total_orders > 0 else 0.0,
                'avg_delay_hours': float(round(delay_hours.mean(), 1)) if len(delay_hours) > 0 else 0.0,
                'median_delay_hours': float(round(delay_hours.median(), 1)) if len(delay_hours) > 0 else 0.0,
                'delay_distribution': delay_stats
            })
        
        return sorted(metrics, key=lambda x: x['compliance_rate'], reverse=True)

    def save_raw_data_to_csv(self, output_dir='data_exports'):
        """Save the raw analysis data and results to CSV files"""
        df = self.fetch_order_dispatch_data()
        if df is None or df.empty:
            return {
                'success': False,
                'message': 'No data available to save',
                'files': []
            }

        try:
            # Create absolute path for output directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))  # Go up 3 levels
            output_dir = os.path.join(base_dir, output_dir)
            
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Add debug logging
            print(f"Saving files to: {output_dir}")  # Temporary debug print

            saved_files = []
            base_filename = f'dispatch_analysis_{self.analysis_id}'
            
            # Save main analysis data with absolute path
            analysis_file = os.path.join(output_dir, f'{base_filename}_raw.csv')
            df.to_csv(analysis_file, index=False)
            saved_files.append(analysis_file)

            # Get analysis results
            results = self.analyze_dispatch_compliance()
            
            # Save daily metrics
            daily_metrics_df = pd.DataFrame(results['daily_metrics'])
            daily_metrics_file = os.path.join(output_dir, f'{base_filename}_daily_metrics.csv')
            daily_metrics_df.to_csv(daily_metrics_file, index=False)
            saved_files.append(daily_metrics_file)

            # Save warehouse metrics
            warehouse_metrics_df = pd.DataFrame(results['warehouse_metrics'])
            warehouse_file = os.path.join(output_dir, f'{base_filename}_warehouse_metrics.csv')
            warehouse_metrics_df.to_csv(warehouse_file, index=False)
            saved_files.append(warehouse_file)

            # Save summary as JSON
            summary_file = os.path.join(output_dir, f'{base_filename}_summary.json')
            with open(summary_file, 'w') as f:
                json.dump(results['summary'], f, indent=2)
            saved_files.append(summary_file)

            return {
                'success': True,
                'message': f'Successfully saved all analysis files to {output_dir}',
                'files': saved_files,
                'analysis_id': self.analysis_id
            }

        except Exception as e:
            # Add more detailed error information
            import traceback
            error_details = traceback.format_exc()
            return {
                'success': False,
                'message': f'Error saving files: {str(e)}',
                'error_details': error_details,
                'files': saved_files
            } 