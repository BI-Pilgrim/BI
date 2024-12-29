# this is my sample data 
"""
{
  [
      {
        "credit_note_id": 4990539,
        "invoice_id": 126332252,
        "order_id": 98356275,
        "reference_code": "order-02",
        "company_name": "PUN_koregaon-park",
        "warehouseId": 40710,
        "seller_gst": "27AAWCS1087Q1Z8",
        "forward_shipment_pickup_address": "22, M M Marg, RBI Staff Colony, Mumbai Central, Mumbai",
        "forward_shipment_pickup_city": "Mumbai",
        "forward_shipment_pickup_state": "Maharashtra",
        "forward_shipment_pickup_pin_code": "400008",
        "forward_shipment_pickup_country": "India",
        "order_type": "B2C",
        "order_type_key": "retailorder",
        "replacement_order": 0,
        "marketplace": "Offline",
        "marketplace_id": 10,
        "salesmanUserId": 0,
        "order_date": "2023-01-19 05:30:00",
        "invoice_date": "2023-01-19 00:00:00",
        "import_date": "2023-01-19 10:59:19",
        "last_update_date": "2023-01-19 11:02:09",
        "manifest_date": "2023-01-19 11:03:36",
        "credit_note_date": "2023-01-19 00:00:00",
        "return_date": "2023-01-19",
        "manifest_no": "20230119110335",
        "invoice_number": "CMH1-2223-525",
        "credit_note_number": "RCMH1-2223-75",
        "marketplace_credit_note_num": "mp_credit_n0_124",
        "marketplace_invoice_num": "order-02",
        "batch_id": null,
        "batch_created_at": null,
        "payment_mode": "Online",
        "payment_mode_id": 1,
        "buyer_gst": "NA",
        "forward_shipment_customer_name": "indu",
        "forward_shipment_customer_contact_num": "9876543210",
        "forward_shipment_customer_address_line_1": "hssr layout",
        "forward_shipment_customer_address_line_2": null,
        "forward_shipment_customer_city": "bangalore",
        "forward_shipment_customer_pin_code": "625017",
        "forward_shipment_customer_state": "Tamil Nadu",
        "forward_shipment_customer_country": "India",
        "forward_shipment_customer_email": "indu@gmail.com",
        "forward_shipment_billing_name": "indu",
        "forward_shipment_billing_address_1": "hssr layout",
        "forward_shipment_billing_address_2": null,
        "forward_shipment_billing_city": "bangalore",
        "forward_shipment_billing_state": "Karnataka",
        "forward_shipment_billing_pin_code": "625017",
        "forward_shipment_billing_country": "India",
        "forward_shipment_billing_mobile": "9876543210",
        "order_quantity": 15,
        "total_invoice_amount": 147,
        "total_invoice_tax": 0,
        "invoice_collectable_amount": 0,
        "items": [
          {
            "company_product_id": 79774333,
            "product_id": 20295190,
            "suborder_id": 148238950,
            "suborder_num": "40710167410615945337277",
            "return_reason": "Complaint- wrong pic on the site",
            "inventory_status": "QC Pass",
            "shipment_type": "SelfShip",
            "suborder_quantity": 3,
            "returned_item_quantity": 3,
            "tax_type": "GST",
            "total_item_selling_price": "30",
            "credit_note_total_item_shipping_charge": null,
            "credit_note_total_item_miscellaneous": null,
            "item_tax_rate": 0,
            "credit_note_total_item_tax": 0,
            "credit_note_total_item_excluding_tax": 30,
            "sku": "Abb1",
            "productName": null,
            "description": null,
            "category": "Dresess",
            "brand": "Adidas",
            "model_no": "2345678",
            "product_tax_code": "6109",
            "ean": "NA",
            "size": "2",
            "cost": 100,
            "mrp": 100,
            "weight": 2,
            "length": 2,
            "width": 2,
            "height": 2
          },
        }
}
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from easy_com.return_orders import constants
from sqlalchemy_utils import JSONType

Base = declarative_base()



class BaseReturnOrders(Base):
    __abstract__ = True
    invoice_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, nullable=True)
    reference_code = Column(String, nullable=True)
    company_name = Column(String, nullable=True)
    ware_house_id = Column(Integer, nullable=True)
    seller_gst = Column(String, nullable=True)
    forward_shipment_pickup_address = Column(String, nullable=True)
    forward_shipment_pickup_city = Column(String, nullable=True)
    forward_shipment_pickup_state = Column(String, nullable=True)
    forward_shipment_pickup_pin_code = Column(String, nullable=True)
    forward_shipment_pickup_country = Column(String, nullable=True)
    order_type = Column(String, nullable=True)
    order_type_key = Column(String, nullable=True)
    replacement_order = Column(Integer, nullable=True)
    marketplace = Column(String, nullable=True)
    marketplace_id = Column(Integer, nullable=True)
    salesman_user_id = Column(Integer, nullable=True)
    order_date = Column(DateTime, nullable=True)
    invoice_date = Column(DateTime, nullable=True)
    import_date = Column(DateTime, nullable=True)
    last_update_date = Column(DateTime, nullable=True)
    manifest_date = Column(DateTime, nullable=True)
    return_date = Column(DateTime, nullable=True)
    manifest_no = Column(String, nullable=True)
    invoice_number = Column(String, nullable=True)
    marketplace_credit_note_num = Column(String, nullable=True)
    marketplace_invoice_num = Column(String, nullable=True)
    batch_id = Column(Integer, nullable=True)
    batch_created_at = Column(DateTime, nullable=True)
    payment_mode = Column(String, nullable=True)
    payment_mode_id = Column(Integer, nullable=True)
    buyer_gst = Column(String, nullable=True)
    forward_shipment_customer_name = Column(String, nullable=True)
    forward_shipment_customer_contact_num = Column(String, nullable=True)
    forward_shipment_customer_address_line_1 = Column(String, nullable=True)
    forward_shipment_customer_address_line_2 = Column(String, nullable=True)
    forward_shipment_customer_city = Column(String, nullable=True)
    forward_shipment_customer_pin_code = Column(String, nullable=True)
    forward_shipment_customer_state = Column(String, nullable=True)
    forward_shipment_customer_country = Column(String, nullable=True)
    forward_shipment_customer_email = Column(String, nullable=True)
    forward_shipment_billing_name = Column(String, nullable=True)
    forward_shipment_billing_address_1 = Column(String, nullable=True)
    forward_shipment_billing_address_2 = Column(String, nullable=True)
    forward_shipment_billing_city = Column(String, nullable=True)
    forward_shipment_billing_state = Column(String, nullable=True)
    forward_shipment_billing_pin_code = Column(String, nullable=True)
    forward_shipment_billing_country = Column(String, nullable=True)
    forward_shipment_billing_mobile = Column(String, nullable=True)
    order_quantity = Column(Integer, nullable=True)
    total_invoice_amount = Column(Float, nullable=True)
    total_invoice_tax = Column(Float, nullable=True)
    invoice_collectable_amount = Column(Float, nullable=True)
    items = Column(String, nullable=True)
    ee_extracted_at = Column(DateTime(True))

class AllReturnOrders(BaseReturnOrders):
    __tablename__ = constants.ALL_RETURN_ORDERS_TABLE_NAME
    credit_note_id = Column(Integer, primary_key=True)
    credit_note_date = Column(DateTime, nullable=True)
    credit_note_number = Column(String, nullable=True)

class PendingReturnOrders(BaseReturnOrders):
    __tablename__ = constants.PENDING_RETURN_ORDERS_TABLE_NAME
