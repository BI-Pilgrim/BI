# this is my sample data 
"""
{
  "code": 200,
  "message": "Successful",
  "data": [
    {
      "grn_id": 141461,
      "grn_invoice_number": "123452",
      "total_grn_value": 2500,
      "grn_status_id": 1,
      "grn_status": "CREATED",
      "grn_created_at": "2022-03-14 12:13:01",
      "grn_invoice_date": "2022-03-14",
      "po_id": 151966,
      "po_number": 75,
      "po_ref_num": "jghvhgv",
      "po_status_id": 3,
      "po_created_date": "2022-03-14 12:11:05",
      "po_updated_date": "2022-03-14 12:11:05",
      "inwarded_warehouse": "Demop",
      "inwarded_warehouse_c_id": 26564,
      "vendor_name": "sanket",
      "vendor_c_id": 54925,
      "grn_items": [
        {
          "grn_detail_id": 4716218,
          "purchase_order_detail_id": 4399025,
          "cp_id": 32214929,
          "product_id": 15023098,
          "sku": "Bedside Starter Kit",
          "hsn": null,
          "model_no": "",
          "ean": null,
          "product_description": null,
          "original_quantity": 5,
          "pending_quantity": 0,
          "received_quantity": 5,
          "grn_detail_price": 500,
          "shelf_id": 110546,
          "expire_date": null,
          "batch_code": "bat1",
          "available": 5,
          "reserved": 0,
          "sold": 0,
          "repair": 0,
          "lost": 0,
          "damaged": 0,
          "gifted": 0,
          "return_to_source": 0,
          "return_available": 0,
          "qc_pending": 0,
          "qc_pass": 0,
          "qc_fail": 0,
          "transfer": 0,
          "discard": 0,
          "used_in_manufacturing": 0,
          "adjusted": 0,
          "near_expiry": 0,
          "expiry": 0
        }
      ]
    },
}
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from easy_com.grn_details import constants
from sqlalchemy_utils import JSONType

Base = declarative_base()

class GrnDetails(Base):
    __tablename__ = constants.GRN_DETAILS_TABLE_NAME
    grn_id = Column(Integer, primary_key=True)
    grn_invoice_number = Column(String, nullable=True)
    total_grn_value = Column(Float, nullable=True)
    grn_status_id = Column(Integer, nullable=True)
    grn_status = Column(String, nullable=True)
    grn_created_at = Column(DateTime, nullable=True)
    grn_invoice_date = Column(DateTime, nullable=True)
    po_id = Column(Integer, nullable=True)
    po_number = Column(Integer, nullable=True)
    po_ref_num = Column(String, nullable=True)
    po_status_id = Column(Integer, nullable=True)
    po_created_date = Column(DateTime, nullable=True)
    po_updated_date = Column(DateTime, nullable=True)
    inwarded_warehouse = Column(String, nullable=True)
    inwarded_warehouse_c_id = Column(Integer, nullable=True)
    vendor_name = Column(String, nullable=True)
    vendor_c_id = Column(Integer, nullable=True)
    grn_items = Column(String, nullable=True)