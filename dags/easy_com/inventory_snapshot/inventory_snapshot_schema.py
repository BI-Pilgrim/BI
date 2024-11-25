# this is my sample data 
"""
    {
      "c_id": 26564,
      "companyname": "Demop",
      "job_type_id": 72,
      "entry_date": "2022-03-15 23:45:39",
      "file_url": "https://s3.ap-south-1.amazonaws.com/ee-uploaded-files/InventorySnapshotMonthlyReport/InventorySnapshotHistory_26564_20220316001118.csv?request-content-type=application/force-download"
    },
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from easy_com.inventory_snapshot import constants
from sqlalchemy_utils import JSONType

Base = declarative_base()

class InventorySnapshotDetails(Base):
    __tablename__ = constants.INVENTORY_SNAPSHOT_TABLE_NAME
    id = Column(String, primary_key=True) #since auto increment is not supported in big query
    c_id = Column(Integer, nullable=False)
    companyname = Column(String, nullable=True)
    job_type_id = Column(Integer, nullable=True)
    entry_date = Column(DateTime, nullable=True)
    file_url = Column(String, nullable=True)
    