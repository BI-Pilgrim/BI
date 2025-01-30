from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional
from datetime import datetime
import requests
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Table, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from google.cloud import bigquery


class PictureUrls(BaseModel):
    small: Optional[str] = None
    compact: Optional[str] = None
    huge: Optional[str] = None
    original: Optional[str] = None

class Picture(BaseModel):
    hidden: Optional[bool] = None
    urls: Optional[PictureUrls] = None

class Reviewer(BaseModel):
    id: Optional[int] = None
    email: Optional[EmailStr] = None
    name: Optional[str] = None
    phone: Optional[str] = None
    tags: Optional[List[str]] = None
    accepts_marketing: Optional[bool] = None
    unsubscribed_at: Optional[datetime] = None
    external_id: Optional[int] = None

class Review(BaseModel):
    id: Optional[int] = None
    title: Optional[str] = None
    body: Optional[str] = None
    rating: Optional[int] = None
    product_external_id: Optional[int] = None
    product_title: Optional[str] = None
    product_handle: Optional[str] = None
    reviewer: Optional[Reviewer] = None
    source: Optional[str] = None
    curated: Optional[str] = None
    hidden: Optional[bool] = None
    verified: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    ip_address: Optional[str] = None
    has_published_pictures: Optional[bool] = None
    has_published_videos: Optional[bool] = None
    pictures: Optional[List[Picture]] = None
    

class ReviewsResponse(BaseModel):
    current_page: Optional[int] = None
    per_page: Optional[int] = None
    reviews: Optional[List[Review]] = None


BASE_URL = "https://api.judge.me/api/v1/reviews"

class JudgeMeScraper:
    def __init__(self, api_key: str, shop_domain: str):
        self.api_key = api_key
        self.shop_domain = shop_domain

    def get_reviews(self, page: int = 1, per_page: int = 10) -> ReviewsResponse:
        url = f"{BASE_URL}"
        response = requests.get(url, params={"api_token": self.api_key, "shop_domain": self.shop_domain, 
                                        "page": page, "per_page": per_page})
        if response.status_code != 200:
            raise Exception(f"Failed to fetch reviews. Status code: {response.status_code}")
        return ReviewsResponse(**response.json())
    
    


def create_bigquery_table(dataset_id: str, table_id: str):
        client = bigquery.Client()
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("body", "STRING"),
            bigquery.SchemaField("rating", "INTEGER"),
            bigquery.SchemaField("product_external_id", "INTEGER"),
            bigquery.SchemaField("product_title", "STRING"),
            bigquery.SchemaField("product_handle", "STRING"),
            bigquery.SchemaField("reviewer", "RECORD", fields=[
                bigquery.SchemaField("id", "INTEGER"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("phone", "STRING"),
                bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
                bigquery.SchemaField("accepts_marketing", "BOOLEAN"),
                bigquery.SchemaField("unsubscribed_at", "TIMESTAMP"),
                bigquery.SchemaField("external_id", "INTEGER"),
            ]),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("curated", "STRING"),
            bigquery.SchemaField("hidden", "BOOLEAN"),
            bigquery.SchemaField("verified", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("ip_address", "STRING"),
            bigquery.SchemaField("has_published_pictures", "BOOLEAN"),
            bigquery.SchemaField("has_published_videos", "BOOLEAN"),
            bigquery.SchemaField("pictures", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("hidden", "BOOLEAN"),
                bigquery.SchemaField("urls", "RECORD", fields=[
                    bigquery.SchemaField("small", "STRING"),
                    bigquery.SchemaField("compact", "STRING"),
                    bigquery.SchemaField("huge", "STRING"),
                    bigquery.SchemaField("original", "STRING"),
                ]),
            ]),
        ]

        table = bigquery.Table(table_ref, schema=schema)
        try:
            client.get_table(table_ref)
            print(f"Table {table.project}.{table.dataset_id}.{table.table_id} already exists.")
        except Exception:
            table = client.create_table(table)
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")