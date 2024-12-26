# this is my sample data 
"""
{
    "po_id": 155628,
    "total_po_value": "48440.0000",
    "po_number": 84,
    "po_ref_num": "Auto PO",
    "po_status_id": 5,
    "po_created_date": "2022-03-23 18:08:53",
    "po_updated_date": "2022-03-23 18:08:53",
    "po_created_warehouse": "demop",
    "po_created_warehouse_c_id": 26564,
    "vendor_name": "testcompany",
    "vendor_c_id": 58614,
    "po_items": [
        {
            "purchase_order_detail_id": 4479090,
            "cp_id": 31625085,
            "product_id": 14913935,
            "sku": "CSC&G005",
            "hsn": null,
            "model_no": "CSC&G005",
            "ean": null,
            "product_description": "Cusinart Chip and Dip",
            "original_quantity": 20,
            "pending_quantity": 0,
            "item_price": "2422.0000"
        }
    ]
}
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from easy_com.utils import Base
from easy_com.purchase_order import constants
from sqlalchemy_utils import JSONType

class PurchaseOrders(Base):
    __tablename__ = constants.PURCHASE_ORDERS_TABLE_NAME
    po_id = Column(Integer, primary_key=True)
    total_po_value = Column(Float, nullable=True)
    po_number = Column(Integer, nullable=True)
    po_ref_num = Column(String, nullable=True)
    po_status_id = Column(Integer, nullable=True)
    po_created_date = Column(DateTime, nullable=True)
    po_updated_date = Column(DateTime, nullable=True)
    po_created_warehouse = Column(String, nullable=True)
    po_created_warehouse_c_id = Column(Integer, nullable=True)
    vendor_name = Column(String, nullable=True)
    vendor_c_id = Column(Integer, nullable=True)
    po_items = Column(String, nullable=True)
    ee_extracted_at = Column(DateTime(True))