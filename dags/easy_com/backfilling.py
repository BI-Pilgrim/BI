import sys, os
sys.path.append(os.getcwd())
print(sys.path)
# this is a script to backfill data to bigquery month wise where we take the input of the month and year and sync all the data for that month
# in chunks of 5 days



from easy_com.countries.get_countries import easyEComCountriesAPI
from easy_com.vendors.get_vendors import easyEComVendorsAPI

from datetime import datetime, timedelta
import pickle
import calendar

# reports data sync
from easy_com.reports.get_reports import easyEComReportsAPI
from easy_com.orders.get_orders import easyEComOrdersAPI
from easy_com.purchase_order.get_purchase_orders import easyEComPurchaseOrdersAPI
import time

def backfill_reports(year, month):

    start_date = datetime(year, month, 1)

    # Get the last day of the month 
    last_day = calendar.monthrange(year, month)[1] 
    # Create the end_date 
    end_date = datetime(year, month, last_day)

    
    easyEComReportsAPI().sync_data(start_date=start_date, end_date=end_date)

def backfill_sales_reports():
    start_date = datetime(2025, 3, 1, 5, 0, 0)
    end_date = datetime(2025, 3, 2, 5, 0, 0)
    easyEComReportsAPI().sync_data(start_date=start_date, end_date=end_date, report_types=['TAX_REPORT_SALES'], snip_time=False)

def ranges(start_date:datetime, end_date:datetime, nhours:int):
    ret_ranges = []
    delta = timedelta(hours=nhours-1)
    period_start = start_date
    period_end = start_date + delta
    while(period_start<end_date):
        ret_ranges.append((period_start, period_end))
        period_start = period_end # + timedelta(hours=1)
        period_end = min(period_start + delta, end_date)
    return ret_ranges

def ranges2(start_date:datetime, end_date:datetime, nhours:int):
    delta = timedelta(hours=nhours-1)
    period_start = end_date - delta
    period_end = end_date
    while(start_date<period_end):
        yield (period_start, period_end)
        period_end = period_start  # + timedelta(hours=1)
        period_start = max(period_start - delta, start_date)

def back_fill_orders(start_date, end_date):

    # sync it in chunks of 6 days make sure teh end date should not cross the end of month
    data = None
    last_ran_pair = None
    
    if(os.path.exists("eecom_orders_prev_run_range.pkl")):
        with open("eecom_orders_prev_run_range.pkl", "rb") as f:
            last_ran_pair = pickle.load(f)
        
    # if(last_ran_pair): print(last_ran_pair, start_date, start_date < last_ran_pair[0])
    if last_ran_pair is None or start_date < last_ran_pair[0]: 
        
        data = easyEComOrdersAPI().sync_data(start_date=start_date, end_date=end_date)
        if (data and data == "No data found") or not data:
            print("No data found between the dates {} to {}".format(start_date, end_date))
        
        with open("eecom_orders_prev_run_range.pkl", "wb") as f:
            pickle.dump((start_date, end_date), f)
    else:
        print("Skipping between the dates {} to {}".format(start_date, end_date))
        

def backfill_purchase_orders(start_date, end_date):
    data = None
    last_ran_pair = None

    if(os.path.exists("eecom_purchase_orders_prev_run_range.pkl")):
        with open("eecom_purchase_orders_prev_run_range.pkl", "rb") as f:
            last_ran_pair = pickle.load(f)

    if last_ran_pair is None or start_date < last_ran_pair[0]:
        data = easyEComPurchaseOrdersAPI().sync_data(start_date=start_date, end_date=end_date)
        if (data and data == "No data found") or not data:
            print("No data found between the dates {} to {}".format(start_date, end_date))

        with open("eecom_purchase_orders_prev_run_range.pkl", "wb") as f:
            pickle.dump((start_date, end_date), f)


def back_fill_any(filler_class, start_date, end_date):

    print(start_date, end_date)
    # sync it in chunks of 6 days make sure teh end date should not cross the end of month
    data = None
    last_ran_pair = None
    
    prev_run_range_pkl = f"{filler_class.__name__}_prev_run_range.pkl"
    
    if(os.path.exists(prev_run_range_pkl)):
        with open(prev_run_range_pkl, "rb") as f:
            last_ran_pair = pickle.load(f)
        
    # if(last_ran_pair): print(last_ran_pair, start_date, start_date < last_ran_pair[0])
    if last_ran_pair is None or start_date < last_ran_pair[0]: 
        
        data = filler_class().sync_data(start_date, end_date)
        if (data and data == "No data found") or not data:
            print("No data found between the dates {} to {}".format(start_date, end_date))
        
        with open(prev_run_range_pkl, "wb") as f:
            pickle.dump((start_date, end_date), f)
    else:
        print("Skipping between the dates {} to {}".format(start_date, end_date))
        
def run_range(filler_class, start_date, end_date, nhours):
    run_ranges = ranges2(start_date, end_date, nhours)
    # print(list(run_ranges))
    for range_ in run_ranges:
        print(range_)
        back_fill_any(filler_class, range_[0], range_[1])

# if __name__ == "__main__":
#     from tqdm import tqdm
#     run_ranges = sorted(ranges(datetime(2021,1,1), datetime.now(), 6), reverse=True)
#     print(run_ranges)
#     # for range_ in tqdm(run_ranges):
#     for range_ in run_ranges:
#         back_fill_orders(range_[0], range_[1])

# if __name__ == "__main__":
#     run_ranges = sorted(ranges(datetime(2023,12,1), datetime.now(), 144), reverse=True)
#     print(run_ranges)
#     for range_ in run_ranges:
#         backfill_purchase_orders(range_[0], range_[1])

# if __name__ == "__main__":
#     # from easy_com.return_orders.get_all_return_orders import easyEComAllReturnOrdersAPI
#     # easyEComAllReturnOrdersAPI().sync_data()

#     from easy_com.return_orders.get_pending_return_orders import easyEComPendingReturnOrdersAPI
#     easyEComPendingReturnOrdersAPI().sync_data()

#     from easy_com.inventory_details.get_inventory_details import easyEComInventoryDetailsAPI
#     easyEComInventoryDetailsAPI().sync_data()

# if __name__ == "__main__":
#     # backfill_reports(2024, 11)
#     backfill_reports(2024, 10)

# if __name__ == "__main__":
#     from easy_com.reports.get_reports import easyEComReportsAPI
#     easyEComReportsAPI().delete_record_id('104156123')
#     easyEComReportsAPI().delete_record_id('104156127')
#     easyEComReportsAPI().delete_record_id('104156130')    

# if __name__ == "__main__":
#     from easy_com.reports.download_reports import easyEComDownloadReportsAPI
#     easyEComDownloadReportsAPI().sync_data()

# if __name__ == "__main__":
    
#     from easy_com.reports.parsers.tax_report import TaxReportParserAPI, constants
#     TaxReportParserAPI(report_type=constants.ReportTypes.TAX_REPORT_SALES.value).sync_data()
#     TaxReportParserAPI(report_type=constants.ReportTypes.TAX_REPORT_RETURN.value).sync_data()
    
#     # while True:nstart)))

#     # from easy_com.reports.parsers.mini_sales_report import MiniSalesReportParserAPI
#     # MiniSalesReportParserAPI().sync_data()

#     from easy_com.reports.parsers.tax_report import TaxReportParserAPI
#     TaxReportParserAPI().sync_data()

#     from easy_com.reports.parsers.returns_report import ReturnsReportParserAPI
#     ReturnsReportParserAPI().sync_data()

#     from easy_com.reports.parsers.pending_returns_report import PendingReturnsReportParserAPI
#     PendingReturnsReportParserAPI().sync_data()

#     from easy_com.reports.parsers.grn_details_report import GRNDetailsReportParserAPI
#     GRNDetailsReportParserAPI().sync_data()

#     from easy_com.reports.parsers.status_wise_stock_report import StatusWiseStockReportParserAPI
#     StatusWiseStockReportParserAPI().sync_data()

#     from easy_com.reports.parsers.inventory_aging_report import InventoryAgingReportParserAPI
#     InventoryAgingReportParserAPI().sync_data()

#     from easy_com.reports.parsers.inventory_view_by_bin_report import InventoryViewByBinReportParserAPI
#     InventoryViewByBinReportParserAPI().sync_data()


#     # easyEComCountriesAPI().sync()
#     # easyEComVendorsAPI().sync()

#     easyEComReportsAPI().sync_data(start_date=datetime(2025, 2, 1), end_date=datetime(2025, 2, 6))


#     from easy_com.reports.get_reports import easyEComReportsAPI
#     import time
#     from datetime import datetime
#     easyEComReportsAPI().sync_data(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 2, 6))

#     from easy_com.reports.download_reports import easyEComDownloadReportsAPI
#     easyEComDownloadReportsAPI().sync_data()


#     from easy_com.reports.parsers.tax_report import TaxReportParserAPI, constants
#     TaxReportParserAPI(report_type=constants.ReportTypes.TAX_REPORT_SALES.value).sync_data()
#     TaxReportParserAPI(report_type=constants.ReportTypes.TAX_REPORT_RETURN.value).sync_data()
        
#         # while True:nstart)))

#     from easy_com.reports.parsers.mini_sales_report import MiniSalesReportParserAPI
#     MiniSalesReportParserAPI().sync_data()

if __name__ == "__main__":
    # backfill_sales_reports()
    from easy_com.reports.parsers.tax_report import TaxReportParserAPI, constants
    from easy_com.reports.download_reports import easyEComDownloadReportsAPI
    # easyEComDownloadReportsAPI().sync_data()
    TaxReportParserAPI(report_type=constants.ReportTypes.TAX_REPORT_SALES.value).sync_data()
