from typing import List, Optional
from pydantic import BaseModel

class Transaction(BaseModel):
    id: Optional[int] = None
    downloader_email: Optional[str] = None
    group_transaction_id: Optional[str] = None
    report_display_name: Optional[str] = None
    report_name: Optional[str] = None
    report_id: Optional[int] = None
    report_slug: Optional[str] = None
    is_premium: Optional[bool] = None
    is_favourite: Optional[bool] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    status: Optional[int] = None
    transaction_id: Optional[int] = None
    is_downloadable: Optional[bool] = None
    is_scheduled: Optional[bool] = None
    row_count: Optional[int] = None
    is_visualisation_enabled: Optional[bool] = None
    # Add other fields as necessary

class TransactionsResponseData(BaseModel):
    dashboard_name: Optional[str] = None
    dashboard_id: Optional[str] = None
    count: Optional[int] = None
    transactions: List[Transaction] = None

class TransactionsResponse(BaseModel):
    status: Optional[int] = None
    data: Optional[TransactionsResponseData] = None
