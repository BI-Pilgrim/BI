# this is my sample data 
"""
{
  "code": 200,
  "countries": [
    {
      "id": 1,
      "country": "India",
      "default_currency_code": "INR",
      "code_2": "IN",
      "code_3": "IND"
    },
    {
      "id": 2,
      "country": "Pakistan",
      "default_currency_code": "PKR",
      "code_2": "PK",
      "code_3": "PAK"
    },
    {
      "id": 3,
      "country": "United States of America (USA)",
      "default_currency_code": "USD",
      "code_2": "US",
      "code_3": "USA"
    },
    {
      "id": 4,
      "country": "Afghanistan",
      "default_currency_code": "AFN",
      "code_2": "AF",
      "code_3": "AFG"
    },
  ]
}
"""
from sqlalchemy import Column, Integer, String, Date, create_engine, DateTime
from easy_com.utils import Base
from easy_com.countries import constants

class Countries(Base):
    __tablename__ = constants.COUNTRIES_TABLE_NAME
    country_id = Column(Integer, nullable=False, primary_key=True)
    country = Column(String(255), nullable=False)
    default_currency_code = Column(String(255), nullable=True)
    code_2 = Column(String(255), nullable=True)
    code_3 = Column(String(255), nullable=True)
    ee_extracted_at = Column(DateTime(True))