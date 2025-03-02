from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from easy_com.utils import Base
from easy_com.reports import constants
from sqlalchemy_utils import JSONType

class Reports(Base):
    __tablename__ = constants.REPORTS_PRODUCT_TABLE_NAME
    report_id = Column(String(255), nullable=False, primary_key=True)
    report_type = Column(String(255), nullable=False)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    created_on = Column(DateTime, nullable = False)
    status = Column(String(255), nullable=True)
    csv_url = Column(String, nullable=True)
    inventory_type = Column(String(255), nullable=True)
    gcs_uri = Column(String, nullable=True)
    ee_extracted_at = Column(DateTime(True))