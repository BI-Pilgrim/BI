# this is my sample data {
#             "vendor_name": "TAKE2 COSMETICS",
#             "vendor_c_id": 188406,
#             "api_token": null,
#             "address": {
#                 "dispatch": {
#                     "address": "SILVASSA,",
#                     "city": "SILVASSA,",
#                     "state_id": 34,
#                     "state_name": "Dadra & Nagar Haveli",
#                     "zip": "396230",
#                     "country": "India"
#                 },
#                 "billing": {
#                     "address": "SILVASSA,",
#                     "city": "SILVASSA,",
#                     "state_id": 34,
#                     "state_name": "Dadra & Nagar Haveli",
#                     "zip": "396230",
#                     "country": "India"
#                 }
#             },
#             "dl_number": "",
#             "dl_expiry": "",
#             "fssai_number": "",
#             "fssai_expiry": ""
#         },
from sqlalchemy import Column, Integer, String, DateTime, create_engine
from easy_com.utils import Base
from easy_com.vendors import constants


class Vendor(Base):
    __tablename__ = constants.VENDORS_TABLE_NAME
    vendor_name = Column(String(255), nullable=False)
    vendor_c_id = Column(Integer, nullable=False, primary_key=True)
    api_token = Column(String(255), nullable=True)
    
    # Dispatch Address Columns
    dispatch_address = Column(String(255), nullable=True)
    dispatch_city = Column(String(100), nullable=True)
    dispatch_state_id = Column(Integer, nullable=True)
    dispatch_state_name = Column(String(100), nullable=True)
    dispatch_zip = Column(String(20), nullable=True)
    dispatch_country = Column(String(100), nullable=True)
    
    # Billing Address Columns
    billing_address = Column(String(255), nullable=True)
    billing_city = Column(String(100), nullable=True)
    billing_state_id = Column(Integer, nullable=True)
    billing_state_name = Column(String(100), nullable=True)
    billing_zip = Column(String(20), nullable=True)
    billing_country = Column(String(100), nullable=True)
    
    # Other Vendor Information
    dl_number = Column(String(50), nullable=True)
    dl_expiry = Column(String(50), nullable=True)
    fssai_number = Column(String(50), nullable=True)
    fssai_expiry = Column(String(50), nullable=True)
    
    ee_extracted_at = Column(DateTime(True))
