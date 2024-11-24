import enum

REPORTS_PRODUCT_TABLE_NAME = 'reports'

@enum.unique
class ReportTypes(enum.Enum):
    MINI_SALES_REPORT = 'MINI_SALES_REPORT'
    TAX_REPORT = 'TAX_REPORT'
    STATUS_WISE_STOCK_REPORT = 'STATUS_WISE_STOCK_REPORT'
    INVENTORY_AGING_REPORT = 'INVENTORY_AGING_REPORT'
    INVENTORY_VIEW_BY_BIN_REPORT = 'INVENTORY_VIEW_BY_BIN_REPORT'
    RETURN_REPORT = 'RETURN_REPORT'
    PENDING_RETURN_REPORT = 'PENDING_RETURN_REPORT'
    GRN_DETAILS_REPORT = 'GRN_DETAILS_REPORT'

    @classmethod
    def get_all_types(cls):
        return [report_type.value for report_type in cls]
    
@enum.unique
class InventoryAgingTypes(enum.Enum):
    AVAILABLE_AND_RESERVED = 'AVAILABLE_AND_RESERVED'
    REPAIR = 'REPAIR'
    DAMAGED = 'DAMAGED'

    @classmethod
    def get_all_types(cls):
        return [inventory_aging_type.value for inventory_aging_type in cls]


@enum.unique
class ReportStatus(enum.Enum):
    IN_PROGRESS = 'IN_PROGRESS'
    COMPLETED = 'COMPLETED' # completed and the url has been saved
    FAILED = 'FAILED'
    PROCESSED = 'PROCESSED' # downloaded, csv processed and the data has been saved

    @classmethod
    def get_all_statuses(cls):
        return [status.value for status in cls]