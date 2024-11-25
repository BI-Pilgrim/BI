# this is my sample data 
"""
{
    "code": 200,
    "states": [
        {
            "id": 211,
            "name": "Azad Kashmir",
            "is_union_territory": 0,
            "zip_start_range": null,
            "zip_end_range": null,
            "postal_code": null,
            "country_id": 2,
            "Zone": null
        },
        {
            "id": 212,
            "name": "Balochistan",
            "is_union_territory": 0,
            "zip_start_range": null,
            "zip_end_range": null,
            "postal_code": null,
            "country_id": 2,
            "Zone": null
        },
        {
            "id": 213,
            "name": "Federally Administered Tribal ",
            "is_union_territory": 0,
            "zip_start_range": null,
            "zip_end_range": null,
            "postal_code": null,
            "country_id": 2,
            "Zone": null
        },
        {
            "id": 214,
            "name": "Gilgit-Baltistan",
            "is_union_territory": 0,
            "zip_start_range": null,
            "zip_end_range": null,
            "postal_code": null,
            "country_id": 2,
            "Zone": null
        },
        {
            "id": 215,
            "name": "Islamabad",
            "is_union_territory": 0,
            "zip_start_range": null,
            "zip_end_range": null,
            "postal_code": null,
            "country_id": 2,
            "Zone": null
        },
        {
            "id": 216,
            "name": "Khyber Pakhtunkhwa Province",
            "is_union_territory": 0,
            "zip_start_range": null,
            "zip_end_range": null,
            "postal_code": null,
            "country_id": 2,
            "Zone": null
        },
        {
            "id": 217,
            "name": "Punjab Province",
            "is_union_territory": 0,
            "zip_start_range": null,
            "zip_end_range": null,
            "postal_code": null,
            "country_id": 2,
            "Zone": null
        },
        {
            "id": 218,
            "name": "Sindh",
            "is_union_territory": 0,
            "zip_start_range": null,
            "zip_end_range": null,
            "postal_code": null,
            "country_id": 2,
            "Zone": null
        }
    ]
}
"""
from sqlalchemy import Column, Integer, String, Date, create_engine
from sqlalchemy.ext.declarative import declarative_base
from easy_com.states import constants

Base = declarative_base()

class States(Base):
    __tablename__ = constants.STATES_TABLE_NAME
    state_id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String(255), nullable=False)
    is_union_territory = Column(Integer, nullable=True)
    zip_start_range = Column(Integer, nullable=True)
    zip_end_range = Column(Integer, nullable=True)
    postal_code = Column(String(255), nullable=True)
    country_id = Column(Integer, nullable=True)
    Zone = Column(String(255), nullable=True)