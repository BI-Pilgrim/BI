from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta

@dag("sync_vendors", schedule='0 4 * * 2', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "weekly"])
def sync_vendors():
    # vendor data sync
    @task.python
    def sync():
        from easy_com.vendors.get_vendors import easyEComVendorsAPI
        easyEComVendorsAPI().sync_data()
    resp = sync()

@dag("sync_countries_and_states", schedule='0 5 * * 2', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "weekly"])
def sync_countries_and_states():
    # states and countries data sync
    @task.python
    def sync():
        from easy_com.countries.get_countries import easyEComCountriesAPI
        easyEComCountriesAPI().sync_data()
    resp = sync()

@dag("sync_locations", schedule='0 4 * * 2', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "weekly"])
def sync_locations():
    # locations data sync
    @task.python
    def sync():
        from easy_com.locations.get_locations import easyEComLocationsAPI
        easyEComLocationsAPI().sync_data()
    resp = sync()

@dag("sync_kits", schedule='0 4 * * 2', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "weekly"])
def sync_kits():
    # kit data sync
    @task.python
    def sync():
        from easy_com.kits.get_kits import easyEComKitsAPI
        easyEComKitsAPI().sync_data()
    resp = sync()

@dag("sync_customers", schedule='0 4 * * 2', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "weekly"])
def sync_customers():
    # customer data sync
    @task.python
    def sync():
        from easy_com.customers.get_customers import easyEComCustomersAPI
        easyEComCustomersAPI().sync_data()
    resp = sync()

@dag("sync_master_products", schedule='0 4 * * 2', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "weekly"])
def sync_master_products():
    # master product data sync
    @task.python
    def sync():
        from easy_com.master_products.get_master_products import easyEComMasterProductAPI
        easyEComMasterProductAPI().sync_data()
    resp = sync()

@dag("sync_marketplaces", schedule='0 4 * * 2', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "weekly"])
def sync_marketplaces():
    # marketplaces data sync
    @task.python
    def sync():
        from easy_com.marketplaces.get_marketplaces import easyEComMarketPlacesAPI
        easyEComMarketPlacesAPI().sync_data()
    resp = sync()

sync_vendors()
sync_countries_and_states()
sync_locations()
sync_kits()
sync_customers()
sync_master_products()
sync_marketplaces()


