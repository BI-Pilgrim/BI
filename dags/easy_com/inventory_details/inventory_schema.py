# this is my sample data 
"""
{
"companyName": "puneWarehouse",
        "location_key": "ne2869637761",
        "companyProductId": 79194900,
        "productId": 20152266,
        "availableInventory": 0,
        "virtual_inventory_count": 0,
        "sku": "112233",
        "accountingSku": null,
        "accountingUnit": null,
        "mrp": 200,
        "creationDate": "2023-01-03 16:43:30",
        "lastUpdateDate": "2023-04-06 16:11:42",
        "cost": 200,
        "skuTaxRate": 0.18,
        "color": "White",
        "size": "2",
        "weight": 2,
        "height": 2,
        "length": 2,
        "width": 2,
        "sellingPriceThreshold": null,
        "inventoryThreshold": null,
        "category": "Mobile",
        "ImageUrl": null,
        "brand": "Vivo",
        "productName": "charger",
        "modelNo": "12345678",
        "productUniqueCode": "NA",
        "description": null,
        "is_combo": 1
}
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from easy_com.inventory_details import constants
from sqlalchemy_utils import JSONType

Base = declarative_base()

class InventoryDetails(Base):
    __tablename__ = constants.INVENTORY_DETAILS_TABLE_NAME
    company_name = Column(String, nullable=True)
    location_key = Column(String, nullable=True)
    company_product_id = Column(Integer, nullable=False, primary_key=True)
    product_id = Column(Integer, nullable=True)
    available_inventory = Column(Integer, nullable=True)
    virtual_inventory_count = Column(Integer, nullable=True)
    sku = Column(String, nullable=True)
    accounting_sku = Column(String, nullable=True)
    accounting_unit = Column(String, nullable=True)
    mrp = Column(Float, nullable=True)
    creation_date = Column(DateTime, nullable=True)
    last_update_date = Column(DateTime, nullable=True)
    cost = Column(Float, nullable=True)
    sku_tax_rate = Column(Float, nullable=True)
    color = Column(String, nullable=True)
    size = Column(String, nullable=True)
    weight = Column(Float, nullable=True)
    height = Column(Float, nullable=True)
    length = Column(Float, nullable=True)
    width = Column(Float, nullable=True)
    selling_price_threshold = Column(Float, nullable=True)
    inventory_threshold = Column(Float, nullable=True)
    category = Column(String, nullable=True)
    image_url = Column(String, nullable=True)
    brand = Column(String, nullable=True)
    product_name = Column(String, nullable=True)
    model_no = Column(String, nullable=True)
    product_unique_code = Column(String, nullable=True)
    description = Column(String, nullable=True)
    is_combo = Column(Integer, nullable=True)

