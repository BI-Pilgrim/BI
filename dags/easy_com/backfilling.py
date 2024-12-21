# this is a script to backfill data to bigquery month wise where we take the input of the month and year and sync all the data for that month
# in chunks of 5 days

import sys
from datetime import datetime, timedelta
import calendar

# reports data sync
from easy_com.reports.get_reports import easyEComReportsAPI
from easy_com.orders.get_orders import easyEComOrdersAPI
import time

def backfill_reports(month, year):

    start_date = datetime(year, month, 1)

    # Get the last day of the month 
    last_day = calendar.monthrange(year, month)[1] 
    # Create the end_date 
    end_date = datetime(year, month, last_day)

    
    easyEComReportsAPI().sync_data(start_date=start_date, end_date=end_date)


def back_fill_orders(month, year):
    start_date = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, last_day)

    # sync it in chunks of 6 days make sure teh end date should not cross the end of month
    while start_date < end_date:
        chunk_start_date = start_date
        chunk_end_date = start_date + timedelta(days=5)
        if chunk_end_date > end_date:
            chunk_end_date = end_date

        data = easyEComOrdersAPI().sync_data(start_date=chunk_start_date, end_date=chunk_end_date)
        if data and data == "No data found":
            print("No data found between the dates {} to {}".format(chunk_start_date, chunk_end_date))
        start_date = chunk_end_date + timedelta(days=1)
        time.sleep(30)