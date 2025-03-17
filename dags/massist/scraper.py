import json
from typing import Optional, List
from requests import Session
from datetime import datetime
from pydantic import BaseModel
from airflow.models import Variable
import pandas as pd
from io import BytesIO
import logging

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded',
}

class AuthException(Exception):
    pass

class OrderData(BaseModel):
    IncrementId: Optional[int]
    ClientZone: Optional[str]
    ClientCity: Optional[str]
    Client_Name: Optional[str]
    EmpName: Optional[str]
    ProductCategory: Optional[str]
    Product_Name: Optional[str]
    Quantity: Optional[int]
    Order_Amt: Optional[float]
    Unit_Price: Optional[float]
    VSKUCode: Optional[str]
    AddressPhoto: Optional[str]

class Label(BaseModel):
    lableId: Optional[str]
    DisplayName: Optional[str]
    DbFeild: Optional[str]
    DisplayPosition: Optional[int]
    GroupName: Optional[str]
    FormPart: Optional[str]

class MassistResponse(BaseModel):
    AllData: List[OrderData]
    AllCounting: List[Label]

class OrderDataPayload(BaseModel):
    EmpList: Optional[str] = ""
    ClientList: Optional[str] = ""
    StartDate: str
    EndDate: str
    Category: str = "C1"
    DataFor: str = "SaleSKUDateWise"
    Division: Optional[str] = ""
    DataState: Optional[str] = ""
    DataCity: Optional[str] = ""
    filter: str = "Sale"
    DataZone: Optional[str] = ""
    ClientType: Optional[str] = ""
    SubType: Optional[str] = ""
    DataFilter: Optional[str] = ""
    FromClientType: Optional[str] = ""
    FromSubType: Optional[str] = ""
    FromClientList: Optional[str] = ""
    ClientTypeGroup: Optional[str] = ""
    FromClientTypeGroup: Optional[str] = ""
    ExcelName: str = "Date Wise SKU"
    ProductId: Optional[str] = ""
    VarientId: Optional[str] = ""
    Manufacturer: Optional[str] = ""
    ProductScheme: Optional[str] = ""
    TotalOrderScheme: Optional[str] = ""
    IsActive: Optional[str] = ""
    IsDMS: Optional[str] = ""
    
class MassistAPI:
    base_url = "https://api.massistcrm.com"
    def __init__(self):
        self.session = Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.token = self.login()
        self.session.headers.update({'Authorization': f'Bearer {self.token}'})

    def login(self) -> str:
        url = f'{self.base_url}/token'
        data = {
            'username': Variable.get("MASSIST_API_USERNAME"),
            'password': Variable.get("MASSIST_API_PASSWORD"),
            'grant_type': 'password'
        }
        response = self.session.post(url, data=data)
        if not response.ok:
            raise AuthException("Unable to login")
        return response.json()['access_token']

    def fetch_order_data(self, start_date: datetime, end_date: datetime) -> MassistResponse:
        
        start_date_str = start_date.strftime("%m/%d/%Y")
        end_date_str = end_date.strftime("%m/%d/%Y")
        payload = OrderDataPayload(StartDate=start_date_str, EndDate=end_date_str).model_dump()
        response = self.session.post(f"{self.base_url}/api/v2/Employee/GetAllOrderSKUData", data=payload)
        if not response.ok:
            raise AuthException("Unable to fetch order data")
        response_data = response.json()
        massist_response = MassistResponse(**response_data)
        return massist_response
