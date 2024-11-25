# this is a script to sync all of easycom data to bigquery

# vendor data sync
from easy_com.vendors.get_vendors import easyEComVendorsAPI
easyEComVendorsAPI().sync_data()

# states and countries data sync
from easy_com.countries.get_countries import easyEComCountriesAPI
easyEComCountriesAPI().sync_data()

# locations data sync
from easy_com.locations.get_locations import easyEComLocationsAPI
easyEComLocationsAPI().sync_data()

# kit data sync
from easy_com.kits.get_kits import easyEComKitsAPI
easyEComKitsAPI().sync_data()

# customer data sync
from easy_com.customers.get_customers import easyEComCustomersAPI
easyEComCustomersAPI().sync_data()

# master product data sync
from easy_com.master_products.get_master_products import easyEComMasterProductAPI
easyEComMasterProductAPI().sync_data()

# marketplaces data sync
from easy_com.marketplaces.get_marketplaces import easyEComMarketPlacesAPI
easyEComMarketPlacesAPI().sync_data()

#orders data sync
from easy_com.orders.get_orders import easyEComOrdersAPI
easyEComOrdersAPI().sync_data()

# grn details data sync
from easy_com.reports.parsers.grn_details_report import GRNDetailsReportParserAPI
GRNDetailsReportParserAPI().sync_data()

# Return Orders data sync
from easy_com.return_orders.get_all_return_orders import easyEComAllReturnOrdersAPI
easyEComAllReturnOrdersAPI().sync_data()

from easy_com.return_orders.get_pending_return_orders import easyEComPendingReturnOrdersAPI
easyEComPendingReturnOrdersAPI().sync_data()

# inventory data sync
from easy_com.inventory_details.get_inventory_details import easyEComInventoryDetailsAPI
easyEComInventoryDetailsAPI().sync_data()

from easy_com.inventory_snapshot.get_inventory_snapshot_details import easyEComInventorySnapshotDetailsAPI
easyEComInventorySnapshotDetailsAPI().sync_data()

from easy_com.purchase_order.get_purchase_orders import easyEComPurchaseOrdersAPI
easyEComPurchaseOrdersAPI().sync_data()


# reports data sync

# Sync new reports every day at 1 am i.e get all types of reports and creates a report id with status in_progress
from easy_com.reports.get_reports import easyEComReportsAPI
easyEComReportsAPI().sync_data()

# Update the csv url and status of the reports
from easy_com.reports.download_reports import easyEComDownloadReportsAPI
easyEComDownloadReportsAPI().sync_data()

# download and parse each report
from easy_com.reports.parsers.mini_sales_report import MiniSalesReportParserAPI
MiniSalesReportParserAPI().sync_data()

from easy_com.reports.parsers.tax_report import TaxReportParserAPI
TaxReportParserAPI().sync_data()

from easy_com.reports.parsers.returns_report import ReturnsReportParserAPI
ReturnsReportParserAPI().sync_data()

from easy_com.reports.parsers.pending_returns_report import PendingReturnsReportParserAPI
PendingReturnsReportParserAPI().sync_data()

from easy_com.reports.parsers.grn_details_report import GRNDetailsReportParserAPI
GRNDetailsReportParserAPI().sync_data()

from easy_com.reports.parsers.status_wise_stock_report import StatusWiseStockReportParserAPI
StatusWiseStockReportParserAPI().sync_data()

from easy_com.reports.parsers.inventory_aging_report import InventoryAgingReportParserAPI
InventoryAgingReportParserAPI().sync_data()

from easy_com.reports.parsers.inventory_view_by_bin_report import InventoryViewByBinReportParserAPI
InventoryViewByBinReportParserAPI().sync_data()