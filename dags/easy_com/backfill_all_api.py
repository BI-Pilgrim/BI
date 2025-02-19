import sys, os
sys.path.append(os.getcwd())

from easy_com.countries.get_countries import easyEComCountriesAPI
from easy_com.vendors.get_vendors import easyEComVendorsAPI
from easy_com.states.get_states import easyEComStatesAPI
from easy_com.return_orders.get_all_return_orders import easyEComAllReturnOrdersAPI
from easy_com.reports.get_reports import easyEComReportsAPI
from easy_com.backfilling import backfill_reports, run_range
from datetime import datetime, date


from easy_com.orders.get_orders import easyEComOrdersAPI
from easy_com.grn_details.get_grn_details import easyEComGrnDetailsAPI
from easy_com.return_orders.get_all_return_orders import easyEComAllReturnOrdersAPI
from easy_com.return_orders.get_pending_return_orders import easyEComPendingReturnOrdersAPI
from easy_com.inventory_details.get_inventory_details import easyEComInventoryDetailsAPI
from easy_com.inventory_snapshot.get_inventory_snapshot_details import easyEComInventorySnapshotDetailsAPI
from easy_com.purchase_order.get_purchase_orders import easyEComPurchaseOrdersAPI

from easy_com.vendors.get_vendors import easyEComVendorsAPI
from easy_com.countries.get_countries import easyEComCountriesAPI
from easy_com.locations.get_locations import easyEComLocationsAPI
from easy_com.kits.get_kits import easyEComKitsAPI
from easy_com.customers.get_customers import easyEComCustomersAPI
from easy_com.master_products.get_master_products import easyEComMasterProductAPI
from easy_com.marketplaces.get_marketplaces import easyEComMarketPlacesAPI


# easyEComInventorySnapshotDetailsAPI().sync_data()
# easyEComPurchaseOrdersAPI().sync_data()


from easy_com.reports.download_reports import easyEComDownloadReportsAPI
from easy_com.reports.parsers.mini_sales_report import MiniSalesReportParserAPI
from easy_com.reports.parsers.tax_report import TaxReportParserAPI
from easy_com.reports.parsers.tax_report import TaxReportParserAPI
from easy_com.reports.parsers.returns_report import ReturnsReportParserAPI
from easy_com.reports.parsers.pending_returns_report import PendingReturnsReportParserAPI
from easy_com.reports.parsers.grn_details_report import GRNDetailsReportParserAPI
from easy_com.reports.parsers.status_wise_stock_report import StatusWiseStockReportParserAPI
from easy_com.reports.parsers.inventory_aging_report import InventoryAgingReportParserAPI
from easy_com.reports.parsers.inventory_view_by_bin_report import InventoryViewByBinReportParserAPI
from easy_com.reports.constants import ReportTypes

# easyEComDownloadReportsAPI().sync_data()
# MiniSalesReportParserAPI().sync_data()
# TaxReportParserAPI(report_type = ReportTypes.TAX_REPORT_RETURN.value).sync_data()
# TaxReportParserAPI(report_type = ReportTypes.TAX_REPORT_SALES.value).sync_data()
# ReturnsReportParserAPI().sync_data()
# PendingReturnsReportParserAPI().sync_data()
# GRNDetailsReportParserAPI().sync_data()
# StatusWiseStockReportParserAPI().sync_data()
# InventoryAgingReportParserAPI().sync_data()
# InventoryViewByBinReportParserAPI().sync_data()

if __name__ == "__main__":
    # easyEComVendorsAPI().sync_data() ##
    # easyEComAllReturnOrdersAPI().sync_data() ##
    # easyEComVendorsAPI().sync_data() ###
    # easyEComCountriesAPI().sync_data() ###
    # easyEComLocationsAPI().sync_data() ###
    # easyEComKitsAPI().sync_data() ###
    # easyEComCustomersAPI().sync_data() ###
    # easyEComMasterProductAPI().sync_data() ###
    # easyEComMarketPlacesAPI().sync_data() ###

    # easyEComGrnDetailsAPI().sync_data() ###
    # easyEComAllReturnOrdersAPI().sync_data() ###
    # easyEComPendingReturnOrdersAPI().sync_data() ###
    # easyEComInventoryDetailsAPI().sync_data() ###
    
    # easyEComReportsAPI().sync_data() #


    # easyEComDownloadReportsAPI().sync_data()
    # MiniSalesReportParserAPI().sync_data()
    # TaxReportParserAPI(report_type = ReportTypes.TAX_REPORT_RETURN.value).sync_data()
    # TaxReportParserAPI(report_type = ReportTypes.TAX_REPORT_SALES.value).sync_data()
    # ReturnsReportParserAPI().sync_data()
    # PendingReturnsReportParserAPI().sync_data()
    # GRNDetailsReportParserAPI().sync_data()
    # StatusWiseStockReportParserAPI().sync_data()
    # InventoryAgingReportParserAPI().sync_data()
    # InventoryViewByBinReportParserAPI().sync_data()


    # back_fill_any()

    # from multiprocessing import Pool
    # pool = Pool(3)

    # pool.apply
    # backfill_reports(2025, 2)
    if sys.argv[1] == 'inv':
        run_range(easyEComInventorySnapshotDetailsAPI, datetime(2025, 1, 1), datetime(2025, 2, 4), 6)
    if sys.argv[1] == 'ord':
        run_range(easyEComOrdersAPI, datetime(2025, 1, 1), datetime(2025, 1, 23), 6)
        # ranges2(datetime(2025, 1, 1), datetime(2025, 1, 29), 20)
    if sys.argv[1] == 'pord':
        run_range(easyEComPurchaseOrdersAPI, datetime(2025, 1, 1), datetime(2025, 1, 31), 144)
    # back_fill_any(date(2025, 1, 1), date(2025, 1, 31))

