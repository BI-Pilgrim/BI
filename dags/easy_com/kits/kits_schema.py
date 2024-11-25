# this is my sample data 
"""
{
    "productId": 21402954,
    "sku": "PGKR-BR",
    "accountingSku": "PGKR-BR",
    "accountingUnit": "nos",
    "mrp": 1300,
    "add_date": "2020-02-11 18:46:40",
    "lastUpdateDate": "2024-11-07 19:20:57",
    "cost": 214.5,
    "HSNCode": "33049990",
    "colour": "NA",
    "weight": 645,
    "height": 6.3499999999999996,
    "length": 25.399999999999999,
    "width": 18.399999999999999,
    "size": "NA",
    "material_type": 1,
    "modelNumber": "PGKR-BR",
    "modelName": "Jeju Body Ritual (Body Butter & Body Scrub)",
    "category": "Skincare",
    "brand": "Pilgrim",
    "c_id": 11537,
    "subProducts": [
        {
            "sku": "PGKBS1",
            "productId": 21402945,
            "qty": 1,
            "description": null,
            "cost": 107.25,
            "availableInventory": 0
        },
        {
            "sku": "PGKBB1",
            "productId": 21402946,
            "qty": 1,
            "description": null,
            "cost": 107.25,
            "availableInventory": 0
        },
        {
            "sku": "PGJB1",
            "productId": 21405933,
            "qty": 1,
            "description": null,
            "cost": 24,
            "availableInventory": 0
        }
    ]
}
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from easy_com.kits import constants
from sqlalchemy_utils import JSONType

Base = declarative_base()

class Kits(Base):
    __tablename__ = constants.KITS_TABLE_NAME
    product_id = Column(Integer, nullable=False, primary_key=True)
    sku = Column(String(255), nullable=False)
    accounting_sku = Column(String(255), nullable=True)
    accounting_unit = Column(String(255), nullable=True)
    mrp = Column(Float, nullable=True)
    add_date = Column(DateTime, nullable=True)
    last_update_date = Column(DateTime, nullable=True)
    cost = Column(Float, nullable=True)
    hsn_code = Column(String(255), nullable=True)
    colour = Column(String(255), nullable=True)
    weight = Column(Float, nullable=True)
    height = Column(Float, nullable=True)
    length = Column(Float, nullable=True)
    width = Column(Float, nullable=True)
    size = Column(String(255), nullable=True)
    material_type = Column(Integer, nullable=True)
    model_number = Column(String(255), nullable=True)
    model_name = Column(String(255), nullable=True)
    category = Column(String(255), nullable=True)
    brand = Column(String(255), nullable=True)
    c_id = Column(Integer, nullable=True)
    sub_products = Column(String, nullable=True)

