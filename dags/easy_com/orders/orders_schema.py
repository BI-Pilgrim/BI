# this is my sample data 
"""
{
   "code": 200,
  "message": "Successful",
  "data": {
    "orders": [
      {
        "invoice_id": 176305783,
        "order_id": 143050997,
        "queue_message": null,
        "queue_status": 8,
        "order_priority": 0,
        "blockSplit": 0,
        "reference_code": "1382",
        "company_name": "Sanket_EasyEcom",
        "location_key": "en2842275969",
        "warehouseId": 53313,
        "seller_gst": "2354D",
        "import_warehouse_id": 53313,
        "import_warehouse_name": "Sanket_EasyEcom",
        "pickup_address": "Bangalore",
        "pickup_city": "Bengalore",
        "pickup_state": "Karnataka",
        "pickup_state_code": "29",
        "pickup_pin_code": "560102",
        "pickup_country": "India",
        "invoice_currency_code": "INR",
        "order_type": "B2C",
        "order_type_key": "retailorder",
        "replacement_order": 0,
        "marketplace": "Shopify13",
        "marketplace_id": 179,
        "qcPassed": 1,
        "salesmanUserId": 0,
        "order_date": "2023-09-15 14:47:25",
        "tat": "2023-09-16 14:53:16",
        "available_after": null,
        "invoice_date": "",
        "import_date": "2023-09-15 14:53:16",
        "last_update_date": "2023-09-15 14:53:17",
        "manifest_date": null,
        "manifest_no": null,
        "invoice_number": null,
        "marketplace_invoice_num": null,
        "shipping_last_update_date": null,
        "batch_id": 2197605,
        "batch_created_at": "2024-03-01 14:07:10",
        "message": null,
        "courier_aggregator_name": null,
        "courier": "SelfShip",
        "carrier_id": 23,
        "awb_number": null,
        "Package Weight": 2,
        "Package Height": 2,
        "Package Length": 22,
        "Package Width": 2,
        "order_status": "Open",
        "order_status_id": 2,
        "suborder_count": 1,
        "shipping_status": null,
        "shipping_status_id": null,
        "shipping_history": null,
        "delivery_date": null,
        "payment_mode": "PrePaid",
        "payment_mode_id": 5,
        "payment_gateway_transaction_number": null,
        "payment_gateway_name": "manual",
        "buyer_gst": "NA",
        "customer_name": "btm 1st stage",
        "contact_num": "9876543212",
        "address_line_1": "BTM Layout",
        "address_line_2": null,
        "city": "Bengaluru",
        "pin_code": "560029",
        "state": "Karnataka",
        "state_code": "29",
        "country": "India",
        "email": "wertyu987654@gmail.com",
        "latitude": null,
        "longitude": null,
        "billing_name": "btm 1st stage",
        "billing_address_1": "BTM Layout",
        "billing_address_2": null,
        "billing_city": "Bengaluru",
        "billing_state": "Karnataka",
        "billing_state_code": "29",
        "billing_pin_code": "560029",
        "billing_country": "India",
        "billing_mobile": "9876543212",
        "order_quantity": 3,
        "meta": null,
        "documents": null,
        "total_amount": 1304.73,
        "total_tax": 199.027,
        "total_shipping_charge": 0,
        "total_discount": 0,
        "collectable_amount": 0,
        "tcs_rate": 0,
        "tcs_amount": 0,
        "customer_code": "NA",
        "suborders": [
          {
            "suborder_id": 219776009,
            "suborder_num": "12192116244574",
            "item_status": "Assigned",
            "shipment_type": "SelfShip",
            "suborder_quantity": 3,
            "item_quantity": 3,
            "returned_quantity": 0,
            "cancelled_quantity": 0,
            "shipped_quantity": 3,
            "batch_codes": null,
            "serial_nums": "NA",
            "batchcode_serial": "NA",
            "batchcode_expiry": "NA",
            "tax_type": "GST",
            "suborder_history": {
              "qc_pass_datetime": "2023-09-15 14:53:16",
              "confirm_datetime": null,
              "print_datetime": null,
              "manifest_datetime": null
            },
            "meta": null,
            "selling_price": "1304.73",
            "total_shipping_charge": null,
            "total_miscellaneous": null,
            "tax_rate": 18,
            "tax": 199.0266,
            "product_id": 22912147,
            "company_product_id": 99689729,
            "sku": "Aurelia kurti123",
            "sku_type": "Normal",
            "sub_product_count": 1,
            "marketplace_sku": "Aurelia kurti123",
            "productName": "Aurelia kurti - yellow",
            "Identifier": "39975324483678",
            "description": null,
            "category": "Fashion",
            "brand": "easyecom-test-app",
            "model_no": "",
            "product_tax_code": null,
            "AccountingSku": null,
            "ean": "NA",
            "size": "NA",
            "cost": 200,
            "mrp": 399,
            "weight": 2,
            "length": 22,
            "width": 2,
            "height": 2,
            "scheme_applied": 0
          }
        ],
        "fulfillable_status": 1
      },
    ]
}
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from easy_com.orders import constants
from sqlalchemy_utils import JSONType

Base = declarative_base()

class Orders(Base):
    __tablename__ = constants.ORDERS_TABLE_NAME
    invoice_id = Column(Integer, nullable=False)
    order_id = Column(Integer, nullable=False, primary_key=True)
    queue_message = Column(String, nullable=True)
    queue_status = Column(Integer, nullable=True)
    order_priority = Column(Integer, nullable=True)
    block_split = Column(Integer, nullable=True)
    reference_code = Column(String, nullable=True)
    company_name = Column(String, nullable=True)
    location_key = Column(String, nullable=True)
    warehouse_id = Column(Integer, nullable=True)
    seller_gst = Column(String, nullable=True)
    import_warehouse_id = Column(Integer, nullable=True)
    import_warehouse_name = Column(String, nullable=True)
    pickup_address = Column(String, nullable=True)
    pickup_city = Column(String, nullable=True)
    pickup_state = Column(String, nullable=True)
    pickup_pin_code = Column(String, nullable=True)
    pickup_country = Column(String, nullable=True)
    invoice_currency_code = Column(String, nullable=True)
    order_type = Column(String, nullable=True)
    order_type_key = Column(String, nullable=True)
    replacement_order = Column(Integer, nullable=True)
    marketplace = Column(String, nullable=True)
    marketplace_id = Column(Integer, nullable=True)
    salesman_user_id = Column(Integer, nullable=True)
    order_date = Column(DateTime, nullable=True)
    tat = Column(DateTime, nullable=True)
    available_after = Column(DateTime, nullable=True)
    invoice_date = Column(DateTime, nullable=True)
    import_date = Column(DateTime, nullable=True)
    last_update_date = Column(DateTime, nullable=True)
    manifest_date = Column(DateTime, nullable=True)
    manifest_no = Column(String, nullable=True)
    invoice_number = Column(String, nullable=True)
    marketplace_invoice_num = Column(String, nullable=True)
    shipping_last_update_date = Column(DateTime, nullable=True)
    batch_id = Column(Integer, nullable=True)
    batch_created_at = Column(DateTime, nullable=True)
    message = Column(String, nullable=True)
    courier_aggregator_name = Column(String, nullable=True)
    courier = Column(String, nullable=True)
    carrier_id = Column(Integer, nullable=True)
    awb_number = Column(String, nullable=True)
    package_weight = Column(Float, nullable=True)
    package_height = Column(Float, nullable=True)
    package_length = Column(Float, nullable=True)
    package_width = Column(Float, nullable=True)
    order_status = Column(String, nullable=True)
    order_status_id = Column(Integer, nullable=True)
    suborder_count = Column(Integer, nullable=True)
    shipping_status = Column(String, nullable=True)
    shipping_status_id = Column(Integer, nullable=True)
    shipping_history = Column(String, nullable=True)
    delivery_date = Column(DateTime, nullable=True)
    payment_mode = Column(String, nullable=True)
    payment_mode_id = Column(Integer, nullable=True)
    payment_gateway_transaction_number = Column(String, nullable=True)
    payment_gateway_name = Column(String, nullable=True)
    buyer_gst = Column(String, nullable=True)
    customer_name = Column(String, nullable=True)
    contact_num = Column(String, nullable=True)
    address_line_1 = Column(String, nullable=True)
    address_line_2 = Column(String, nullable=True)
    city = Column(String, nullable=True)
    pin_code = Column(String, nullable=True)
    state = Column(String, nullable=True)
    state_code = Column(String, nullable=True)
    country = Column(String, nullable=True)
    email = Column(String, nullable=True)
    latitude = Column(String, nullable=True)
    longitude = Column(String, nullable=True)
    billing_name = Column(String, nullable=True)
    billing_address_1 = Column(String, nullable=True)
    billing_address_2 = Column(String, nullable=True)
    billing_city = Column(String, nullable=True)
    billing_state = Column(String, nullable=True)
    billing_state_code = Column(String, nullable=True)
    billing_pin_code = Column(String, nullable=True)
    billing_country = Column(String, nullable=True)
    billing_mobile = Column(String, nullable=True)
    order_quantity = Column(Integer, nullable=True)
    meta = Column(String, nullable=True) # json
    documents = Column(String, nullable=True) #json
    total_amount = Column(Float, nullable=True)
    total_tax = Column(Float, nullable=True)
    total_shipping_charge = Column(Float, nullable=True)
    total_discount = Column(Float, nullable=True)
    collectable_amount = Column(Float, nullable=True)
    tcs_rate = Column(Float, nullable=True)
    tcs_amount = Column(Float, nullable=True)
    customer_code = Column(String, nullable=True)
    suborders = Column(String, nullable=True) #json
    fulfillable_status = Column(String, nullable=True)


