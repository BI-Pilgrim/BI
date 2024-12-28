from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta

@dag("sync_orders", schedule='0 1 * * *', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "daily"])
def sync_orders():
    # order data sync
    @task.python
    def sync():
        from easy_com.orders.get_orders import easyEComOrdersAPI
        easyEComOrdersAPI().sync_data()
    resp = sync()

@dag("sync_grn_details", schedule='0 2 * * *', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "daily"])
def sync_grn_details():
    # grn details data sync
    @task.python
    def sync():
        from easy_com.grn_details.get_grn_details import easyEComGrnDetailsAPI
        easyEComGrnDetailsAPI().sync_data()
    resp = sync()

@dag("sync_return_orders", schedule='0 2 * * *', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "daily"])
def sync_return_orders():
    # Return Orders data sync
    @task.python
    def sync():
        from easy_com.return_orders.get_all_return_orders import easyEComAllReturnOrdersAPI
        easyEComAllReturnOrdersAPI().sync_data()

        from easy_com.return_orders.get_pending_return_orders import easyEComPendingReturnOrdersAPI
        easyEComPendingReturnOrdersAPI().sync_data()
    resp = sync()

@dag("sync_inventory_details", schedule='0 2 * * *', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "daily"])
def sync_inventory_details():
    # inventory data sync
    @task.python
    def sync():
        from easy_com.inventory_details.get_inventory_details import easyEComInventoryDetailsAPI
        easyEComInventoryDetailsAPI().sync_data()

        from easy_com.inventory_snapshot.get_inventory_snapshot_details import easyEComInventorySnapshotDetailsAPI
        easyEComInventorySnapshotDetailsAPI().sync_data()
    resp = sync()


@dag("sync_purchase_orders", schedule='0 2 * * *', start_date=datetime(year=2024,month=1,day=1), tags=["easyecom", "daily"])
def sync_purchase_orders():
    @task.python
    def sync():
        from easy_com.purchase_order.get_purchase_orders import easyEComPurchaseOrdersAPI
        easyEComPurchaseOrdersAPI().sync_data()
    resp = sync()

orders = sync_orders()
grn_details = sync_grn_details()
return_orders = sync_return_orders()
inventory_details = sync_inventory_details()
purchase_orders = sync_purchase_orders()