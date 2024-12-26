# this is my sample data 
# {
#   "code": 200,
#   "message": "Successful",
#   "data": [
#     {
#       "company_id": 171721,
#       "location_key": "ne29488101841",
#       "location_name": "prod_wh_2",
#       "is_store": 0,
#       "city": "Bangalore",
#       "state": "Karnataka",
#       "country": "India",
#       "zip": "560102",
#       "copy_master_from_primary": 1,
#       "address": "Banglore bangalore",
#       "api_token": "a08328ca0b823014c3ebf53dafe9867e7fbc8c56cec19f00f55744d25f92da03",
#       "userId": 180681,
#       "phone number": "9876598765",
#       "address type": {
#         "billing_address": {
#           "street": "Banglore bangalore",
#           "state": "Karnataka",
#           "zipcode": "560102",
#           "country": "India"
#         },
#         "pickup_address": {
#           "street": "Banglore bangalore",
#           "state": "Karnataka",
#           "zipcode": "560102",
#           "country": "India"
#         }
#       },
#       "stockHandle": 1
#     },
#     {
#       "company_id": 171719,
#       "location_key": "en29487414961",
#       "location_name": "Prod_wh_1",
#       "is_store": 0,
#       "city": "Pune",
#       "state": "Maharashtra",
#       "country": "India",
#       "zip": "411017",
#       "copy_master_from_primary": 1,
#       "address": "Pune pune",
#       "api_token": "32225e17d4ddcf0b2823796b71fcbe8025cde07f7f8f478799c8771c2e8a140c",
#       "userId": 180679,
#       "phone number": "9876543217",
#       "address type": {
#         "billing_address": {
#           "street": "Pune pune",
#           "state": "Maharashtra",
#           "zipcode": "411017",
#           "country": "India"
#         },
#         "pickup_address": {
#           "street": "Pune pune",
#           "state": "Maharashtra",
#           "zipcode": "411017",
#           "country": "India"
#         }
#       },
#       "stockHandle": 1
#     },
#     {
#       "company_id": 41670,
#       "location_key": "ty1736388900",
#       "location_name": "test sandeep account",
#       "is_store": 0,
#       "city": "Bangalore",
#       "state": "Karnataka",
#       "country": "India",
#       "zip": "562114",
#       "copy_master_from_primary": 0,
#       "address": "Bangalore",
#       "api_token": "ebacd51b26947af73f1c9c485352c911a1ee089ac7d2d68525223701dc3683bb",
#       "userId": 180934,
#       "phone number": "9999999999",
#       "address type": {
#         "billing_address": {
#           "street": "Bangalore",
#           "state": "Karnataka",
#           "zipcode": "562114",
#           "country": "India"
#         },
#         "pickup_address": {
#           "street": "Bangalore",
#           "state": "Karnataka",
#           "zipcode": "562114",
#           "country": "India"
#         }
#       },
#       "stockHandle": 0
#     }
#   ]
# }
from sqlalchemy import Column, Integer, String, Date, create_engine, DateTime
from easy_com.utils import Base
from easy_com.locations import constants

class Location(Base):
    __tablename__ = constants.LOCATIONS_TABLE_NAME
    company_id = Column(Integer, nullable=True)
    location_key = Column(String(255), nullable=False)
    location_name = Column(String(255), nullable=False, primary_key=True)
    is_store = Column(Integer, nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    country = Column(String(100), nullable=True)
    zip = Column(String(20), nullable=True)
    copy_master_from_primary = Column(Integer, nullable=True)
    address = Column(String(255), nullable=True)
    api_token = Column(String(255), nullable=True)
    user_id = Column(Integer, nullable=True)
    phone_number = Column(String(20), nullable=True)
    
    # Address Type Columns
    billing_street = Column(String(255), nullable=True)
    billing_state = Column(String(100), nullable=True)
    billing_zipcode = Column(String(20), nullable=True)
    billing_country = Column(String(100), nullable=True)

    # Pickup Address Columns
    pickup_street = Column(String(255), nullable=True)
    pickup_state = Column(String(100), nullable=True)
    pickup_zipcode = Column(String(20), nullable=True)
    pickup_country = Column(String(100), nullable=True)
    
    # Other Location Information
    stockHandle = Column(Integer, nullable=True)
    ee_extracted_at = Column(DateTime(True))
