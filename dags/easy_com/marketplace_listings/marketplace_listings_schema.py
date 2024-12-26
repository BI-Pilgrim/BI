# this is my sample data 
"""
{
    "code": 200,
    "data": [
        {
            "name": "Shopify",
            "sku": "42651319861477",
            "MasterSKU": "42651319861477",
            "mrp": 1639,
            "site_uid": "42651319861477",
            "listing_ref_number": "44745871720677",
            "UID": null,
            "identifier": "42651319861477",
            "title": "24K Gold Facial Kit with Pilgrim VANITY BAG worth 599"
        },
        {
            "name": "Shopify",
            "sku": "42718683824357",
            "MasterSKU": "42718683824357",
            "mrp": 1639,
            "site_uid": "42718683824357",
            "listing_ref_number": "44813406699749",
            "UID": null,
            "identifier": "42718683824357",
            "title": "24K Gold Facial Kit with Pilgrim VANITY BAG worth 599"
        }
    ]
}
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ARRAY, JSON, create_engine
from easy_com.marketplace_listings import constants
from easy_com.utils import Base


class MarketPlaceListings(Base):
    __tablename__ = constants.MARKETPLACE_LISTINGS_TABLE_NAME
    name = Column(String(255), nullable=False)
    sku = Column(String(255), nullable=False, primary_key=True)
    master_sku = Column(String(255), nullable=True)
    mrp = Column(Float, nullable=True)
    site_uid = Column(String(255), nullable=True)
    listing_ref_number = Column(String(255), nullable=True)
    uid = Column(String(255), nullable=True)
    identifier = Column(String(255), nullable=True)
    title = Column(String, nullable=True)
    ee_extracted_at = Column(DateTime(True))


