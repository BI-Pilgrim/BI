# this is my sample data 
# {
#   "code": 200,
#   "message": " Successfull",
#   "data": [
#     {
#       "company_invoice_group_id": 25727,
#       "c_id": 58602,
#       "companyname": "test customer",
#       "pricingGroup": "testrr5522",
#       "customer_support_email": "testcustomer@easyecom.io",
#       "customer_support_contact": "9036512345",
#       "branddescription": null,
#       "currency_code": "INR",
#       "gstNum": "ABC1523EDR34",
#       "billingStreet": "kaikondrahalli,sarjapur road",
#       "billingCity": "bangalore",
#       "billingZipcode": "560035",
#       "billingState": "Arunachal Pradesh",
#       "billingCountry": "India",
#       "dispatchStreet": "kaikondrahalli,sarjapur road",
#       "dispatchCity": "bangalore",
#       "dispatchZipcode": "560035",
#       "dispatchState": "Arunachal Pradesh",
#       "dispatchCountry": "India"
#     },
#     {
#       "company_invoice_group_id": 25727,
#       "c_id": 58609,
#       "companyname": "test customer",
#       "pricingGroup": "ABC",
#       "customer_support_email": "testcustomer12@easyecom.io",
#       "customer_support_contact": "9036512345",
#       "branddescription": null,
#       "currency_code": "INR",
#       "gstNum": "ABC1523EDR34",
#       "billingStreet": "kaikondrahalli,sarjapur road",
#       "billingCity": "bangalore",
#       "billingZipcode": "560035",
#       "billingState": "Arunachal Pradesh",
#       "billingCountry": "India",
#       "dispatchStreet": "kaikondrahalli,sarjapur road",
#       "dispatchCity": "bangalore",
#       "dispatchZipcode": "560035",
#       "dispatchState": "Arunachal Pradesh",
#       "dispatchCountry": "India"
#     },
#     {
#       "company_invoice_group_id": null,
#       "c_id": 34517,
#       "companyname": "MAMA1",
#       "pricingGroup": null,
#       "customer_support_email": "MAMA1@gmail.com",
#       "customer_support_contact": "9999999999",
#       "branddescription": null,
#       "currency_code": "INR",
#       "gstNum": "GST19992",
#       "billingStreet": "kundanhall",
#       "billingCity": "Bangalore",
#       "billingZipcode": "560037",
#       "billingState": "Karnataka",
#       "billingCountry": "India",
#       "dispatchStreet": "kundanhall",
#       "dispatchCity": "Bangalore",
#       "dispatchZipcode": "560037",
#       "dispatchState": "Karnataka",
#       "dispatchCountry": "India"
#     },
#     {
#       "company_invoice_group_id": null,
#       "c_id": 54925,
#       "companyname": "sanket",
#       "pricingGroup": null,
#       "customer_support_email": "sanket@gmail.com",
#       "customer_support_contact": "",
#       "branddescription": null,
#       "currency_code": "INR",
#       "gstNum": "12131",
#       "billingStreet": "pune",
#       "billingCity": "pune",
#       "billingZipcode": "22121",
#       "billingState": "Maharashtra",
#       "billingCountry": "India",
#       "dispatchStreet": "pune",
#       "dispatchCity": "pune",
#       "dispatchZipcode": "121212",
#       "dispatchState": "Maharashtra",
#       "dispatchCountry": "India"
#     }
#   ]
# }

from sqlalchemy import Column, Integer, String, Date, create_engine, DateTime
from easy_com.utils import Base
from easy_com.customers import constants


class Customer(Base):
    __tablename__ = constants.CUSTOMERS_TABLE_NAME
    company_invoice_group_id = Column(Integer, nullable=True)
    c_id = Column(Integer, nullable=False, primary_key=True)
    company_name = Column(String(255), nullable=False)
    pricing_group = Column(String(255), nullable=True)
    customer_support_email = Column(String(255), nullable=True)
    customer_support_contact = Column(String(255), nullable=True)
    brand_description = Column(String(255), nullable=True)
    currency_code = Column(String(255), nullable=True)
    gst_num = Column(String(255), nullable=True)
    type_of_customer = Column(String(255), nullable=False) # can be either 'b2b' or 'stn'

    # Billing Address Columns
    billing_street = Column(String(255), nullable=True)
    billing_city = Column(String(100), nullable=True)
    billing_zipcode = Column(String(20), nullable=True)
    billing_state = Column(String(100), nullable=True)
    billing_country = Column(String(100), nullable=True)

    # Dispatch Address Columns
    dispatch_street = Column(String(255), nullable=True)
    dispatch_city = Column(String(100), nullable=True)
    dispatch_zipcode = Column(String(20), nullable=True)
    dispatch_state = Column(String(100), nullable=True)
    dispatch_country = Column(String(100), nullable=True)
    ee_extracted_at = Column(DateTime(True))