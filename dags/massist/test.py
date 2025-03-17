from decimal import Decimal
from typing import Optional
import requests
from pydantic import BaseModel

url = 'https://api.massistcrm.com/token'
headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
}
data = {
    'username': 'dummy',
    'password': 'dummy',
    'grant_type': 'password'
}


response = requests.post(url, headers=headers, data=data)
token = response.json()['access_token']

payload = {
"EmpList": "",
"ClientList": "",
"StartDate": "02/24/2025",
"EndDate": "02/24/2025",
"Category": "C1",
"DataFor": "SaleSKUDateWise",
"Division": "",
"DataState": "",
"DataCity": "",
"filter": "Sale",
"DataZone": "",
"ClientType": "",
"SubType": "",
"DataFilter": "",
"FromClientType": "",
"FromSubType": "",
"FromClientList": "",

"ClientTypeGroup": "",
"FromClientTypeGroup": "",
"ExcelName": "Date Wise SKU",
"ProductId": "",
"VarientId": "",
"Manufacturer": "",
"ProductScheme": "",
"TotalOrderScheme": "",
"IsActive": "",
"IsDMS": ""
}
resp = requests.post("https://api.massistcrm.com/api/v2/Employee/GetAllOrderSKUData", headers={'Authorization': f'Bearer {token}'},
              data=payload)
class Label(BaseModel):
    lableId: str = None
    DisplayName: str = None
    DbFeild: str = None
    DisplayPosition: int = None
    GroupName: str = None
    FormPart: str = None

label_data = Label(
    lableId="lblClientZone",
    DisplayName="Zone",
    DbFeild="ClientZone",
    DisplayPosition=0,
    GroupName="",
    FormPart=""
)

print(label_data.json())
class OrderData(BaseModel):
    IncrementId: Optional[int] = None
    ClientZone: Optional[str] = None
    ClientCity: Optional[str] = None
    Client_Name: Optional[str] = None
    EmpName: Optional[str] = None
    ProductCategory: Optional[str] = None
    Product_Name: Optional[str] = None
    Quantity: Optional[int] = None
    Order_Amt: Optional[Decimal] = None
    Unit_Price: Optional[Decimal] = None
    VSKUCode: Optional[str] = None
    AddressPhoto: Optional[str] = None

order_data = OrderData(
    IncrementId=-10,
    ClientZone="Total",
    ClientCity=None,
    Client_Name=None,
    EmpName=None,
    ProductCategory=None,
    Product_Name=None,
    Quantity=29327,
    Order_Amt=11562872,
    Unit_Price=None,
    VSKUCode=None,
    AddressPhoto=None
)

print(order_data.json())
        

