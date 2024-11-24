# this is my sample data 
"""
{
    "data": [
        {
        "cp_id": 76120647,
        "product_id": 19452180,
        "sku": "CCSKU23",
        "product_name": "CCount",
        "description": null,
        "active": 1,
        "created_at": "2022-10-11 16:10:30",
        "updated_at": "2023-09-07 12:18:07",
        "inventory": 0,
        "product_type": "normal_product",
        "brand": "CCC",
        "colour": "NA",
        "category_id": 196211,
        "brand_id": 7147017,
        "accounting_sku": null,
        "accounting_unit": null,
        "category_name": "CC5",
        "expiry_type": 0,
        "company_name": "testdipak",
        "c_id": 59032,
        "height": 3,
        "length": 3,
        "width": 3,
        "weight": 200,
        "cost": 150,
        "mrp": 200,
        "size": "NA",
        "cp_sub_products_count": 0,
        "model_no": "6513",
        "hsn_code": null,
        "tax_rate": 0.18,
        "product shelf life": null,
        "product_image_url": null,
        "cp_inventory": 0,
        "custom_fields": [
            {
            "cp_id": 76120647,
            "field_name": "test_prod_1",
            "value": "123",
            "enabled": 1
            }
        ]
        },
    ]
}
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from sqlalchemy.ext.declarative import declarative_base
from easy_com.master_products import constants
from sqlalchemy_utils import JSONType

Base = declarative_base()

class MasterProducts(Base):
    __tablename__ = constants.MASTER_PRODUCT_TABLE_NAME
    cp_id = Column(Integer, nullable=False, primary_key=True)
    product_id = Column(Integer, nullable=False)
    sku = Column(String(255), nullable=False)
    product_name = Column(String(255), nullable=True)
    description = Column(String, nullable=True)
    active = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    inventory = Column(Integer, nullable=True)
    product_type = Column(String(255), nullable=True)
    brand = Column(String(255), nullable=True)
    colour = Column(String(255), nullable=True)
    category_id = Column(Integer, nullable=True)
    brand_id = Column(Integer, nullable=True)
    accounting_sku = Column(String(255), nullable=True)
    accounting_unit = Column(String(255), nullable=True)
    category_name = Column(String(255), nullable=True)
    expiry_type = Column(Integer, nullable=True)
    company_name = Column(String(255), nullable=True)
    c_id = Column(Integer, nullable=True)
    height = Column(Float, nullable=True)
    length = Column(Float, nullable=True)
    width = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    cost = Column(Float, nullable=True)
    mrp = Column(Float, nullable=True)
    size = Column(String(255), nullable=True)
    cp_sub_products_count = Column(Integer, nullable=True)
    model_no = Column(String(255), nullable=True)
    hsn_code = Column(String(255), nullable=True)
    tax_rate = Column(Float, nullable=True)
    product_shelf_life = Column(String(255), nullable=True)
    product_image_url = Column(String(255), nullable=True)
    cp_inventory = Column(Integer, nullable=True)
    custom_fields = Column(String, nullable=True)
    sub_products = Column(String, nullable=True)
    