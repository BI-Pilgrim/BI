# this is my sample data 
"""
{
    "code": 200,
    "data": [
        {
            "id": 1,
            "name": "Bangalore"
        },
        {
            "id": 2,
            "name": "Pune"
        },
        {
            "id": 3,
            "name": "Mumbai"
        }
    ]
}
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from easy_com.utils import Base
from easy_com.marketplaces import constants
from sqlalchemy_utils import JSONType

class MarketPlaces(Base):
    __tablename__ = constants.MARKETPLACE_TABLE_NAME
    marketplace_id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String(255), nullable=False)
    ee_extracted_at = Column(DateTime(True))

