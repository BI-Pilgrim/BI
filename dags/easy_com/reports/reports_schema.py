from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from easy_com.reports import constants
from sqlalchemy_utils import JSONType

Base = declarative_base()

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