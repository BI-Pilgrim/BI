from airflow.decorators import task, dag
from datetime import datetime, timedelta
from easy_com.reports.get_reports import easyEComReportsAPI

    
@dag("sync_reports_data", schedule='0 1 * * *', start_date=datetime(year=2024,month=1,day=1))
def sync_reports_data():
    
    @task.python
    def sync():
        easyEComReportsAPI().sync_data()
        return ""
    resp = sync()


@dag("update_csv_url_and_status", schedule='0 */2 * * *', start_date=datetime(year=2024,month=1,day=1))
def update_csv_url_and_status():
    """
    This api sync data every 2 hours and check if the report is avvailable and get the csv url and status and updates it
    """
    from easy_com.reports.download_reports import easyEComDownloadReportsAPI
    
    @task.python
    def sync():
        easyEComDownloadReportsAPI().sync_data()
    sync()


# all the below Report uploading tasks should run once every day at 5 am

@dag("sync_mini_sales_report", schedule='0 4 * * *', start_date=datetime(year=2024,month=1,day=1))
def sync_mini_sales_report():
    from easy_com.reports.parsers.mini_sales_report import MiniSalesReportParserAPI
    
    @task.python
    def sync():
        MiniSalesReportParserAPI().sync_data()
    sync()

@dag("sync_tax_report", schedule='0 5 * * *', start_date=datetime(year=2024,month=1,day=1))
def sync_tax_report():
    from easy_com.reports.parsers.tax_report import TaxReportParserAPI
    
    @task.python
    def sync():
        TaxReportParserAPI().sync_data()
    sync()

@dag("sync_returns_report", schedule='0 5 * * *', start_date=datetime(year=2024,month=1,day=1))
def sync_returns_report():
    from easy_com.reports.parsers.returns_report import ReturnsReportParserAPI
    
    @task.python
    def sync():
        ReturnsReportParserAPI().sync_data()
    sync()

@dag("sync_pending_returns_report", schedule='0 5 * * *', start_date=datetime(year=2024,month=1,day=1))
def sync_pending_returns_report():
    from easy_com.reports.parsers.pending_returns_report import PendingReturnsReportParserAPI
    
    @task.python
    def sync():
        PendingReturnsReportParserAPI().sync_data()
    sync()

@dag("sync_grn_details_report", schedule='0 5 * * *', start_date=datetime(year=2024,month=1,day=1))
def sync_grn_details_report():
    from easy_com.reports.parsers.grn_details_report import GRNDetailsReportParserAPI
    
    @task.python
    def sync():
        GRNDetailsReportParserAPI().sync_data()
    sync()

@dag("sync_status_wise_stock_report", schedule='0 5 * * *', start_date=datetime(year=2024,month=1,day=1))
def sync_status_wise_stock_report():
    from easy_com.reports.parsers.status_wise_stock_report import StatusWiseStockReportParserAPI
    
    @task.python
    def sync():
        StatusWiseStockReportParserAPI().sync_data()
    sync()

@dag("sync_inventory_report", schedule='0 6 * * *', start_date=datetime(year=2024,month=1,day=1))
def sync_inventory_report():
    from easy_com.reports.parsers.inventory_aging_report import InventoryAgingReportParserAPI
    from easy_com.reports.parsers.inventory_view_by_bin_report import InventoryViewByBinReportParserAPI
    
    @task.python
    def sync_inventory_aging():
        InventoryAgingReportParserAPI().sync_data()
    
    @task.python
    def sync_inventory_by_bin():
        InventoryViewByBinReportParserAPI().sync_data()

    sync_inventory_aging()
    sync_inventory_by_bin()

var_sync_reports_data = sync_reports_data()
var_update_csv_url_and_status = update_csv_url_and_status()
var_sync_mini_sales_report = sync_mini_sales_report()
var_sync_tax_report = sync_tax_report()
var_sync_returns_report = sync_returns_report()
var_sync_pending_returns_report = sync_pending_returns_report()
var_sync_grn_details_report = sync_grn_details_report()
var_sync_status_wise_stock_report = sync_status_wise_stock_report()
var_sync_inventory_report = sync_inventory_report()