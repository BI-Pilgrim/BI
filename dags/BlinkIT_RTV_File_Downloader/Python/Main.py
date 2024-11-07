from playwright.sync_api import sync_playwright
import time
from datetime import datetime, timedelta
import os
import random
import shutil

class BlinkitPODownloader:
    def __init__(self, start_date, end_date):
        # Initialize with the start date, end date, and download path for PO files
        self.start_date = start_date
        self.end_date = end_date
        self.Base_Path =os.path.join(os.path.dirname(__file__),"RTV_FOLDER")
        self.po_orders = []  # List to store PO Numbers
        self.dates = self.get_date_range(start_date, end_date)  # Generate a range of dates to check
        os.makedirs(self.Base_Path, exist_ok=True)  # Ensure download path exists
        # self.backup_folder = os.path.join(self.Base_Path)
        start_date_try = datetime.strptime(start_date, '%d/%m/%y').strftime('%d_%m_%y')
        end_date_try = datetime.strptime(end_date, '%d/%m/%y').strftime('%d_%m_%y')

        self.download_path = self.Create_Folder_(Start_Date=start_date_try,End_Date=end_date_try)  # Backup existing files before starting the process
        

    def get_date_range(self, start_date, end_date):
        # Convert start and end dates from string to datetime, then generate list of dates in 'dd Mon yy' format
        start = datetime.strptime(start_date, '%d/%m/%y')
        end = datetime.strptime(end_date, '%d/%m/%y')

        delta = end - start
        return [(start + timedelta(days=i)).strftime('%d %b %y') for i in range(delta.days + 1)]
    
    def Create_Folder_(self,Start_Date , End_Date):
        # Create backup folder with a timestamp to store previous file
        Folder_path = os.path.join(self.Base_Path, f"RTV_Data_{str(Start_Date)}_TO_{str(End_Date)}")
        os.makedirs(Folder_path, exist_ok=True)
        return Folder_path
        


    def download_po_orders(self):
        # Launch Playwright browser with a persistent context to maintain session
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch_persistent_context(
                "Blinkit_Session", headless=False, args=["--start-maximized"], no_viewport=True
            )
            # Generating the list of PO numbers
            page = browser.new_page()
            page.goto("https://www.partnersbiz.com/")
            time.sleep(random.randint(1, 3))
            page.wait_for_load_state('load')
            page.wait_for_selector('text="Expired PO with GRN"')
            page.click('text="Expired PO with GRN"')
            time.sleep(random.randint(1, 3))

            # Sorting the orders by Order Date
            page.wait_for_selector('text="Order Date"')
            page.click('text="Order Date"')
            time.sleep(random.randint(1, 2))
            page.click('text="Order Date"')
            
            """
            State management for PO extraction process:
            - "Start": The initial state where it searches for the row with the first date in the specified date range.
            - "Extract": Once the starting date is found, it switches to this state and begins extracting PO numbers, appending them to the list.
            - "Stop": The final state, triggered when it encounters a date outside the specified range, indicating the end of the extraction process.
            """
            state = "Start"
            while state != "Stop":
                rows = page.query_selector_all('.ant-table-row.ant-table-row-level-0')
                for row_data in rows:
                    cells = row_data.query_selector_all('.ant-table-cell')
                    fifth_cell_text = cells[4].text_content().strip()
                    oneth_cell_text = cells[1].text_content().strip()
                    
                    if state == "Start" and fifth_cell_text in self.dates:
                        state = "Extract"
                        self.po_orders.append(oneth_cell_text)
                    elif state == "Extract":
                        if fifth_cell_text not in self.dates:
                            state = "Stop"
                            break
                        else:
                            self.po_orders.append(oneth_cell_text)

                if state != "Stop":
                    page.wait_for_selector('li[title="Next Page"]')
                    page.click('li[title="Next Page"]')
                    time.sleep(random.randint(2, 6))
                    page.wait_for_selector('.ant-table-row.ant-table-row-level-0', timeout=60000)

            # Function downloading RTV PDF for all those PO  
            self.process_po_orders(page)
            browser.close()

    def process_po_orders(self, page):
        
        for po_number in self.po_orders:
            # time.sleep(6)
            print(f"Processing PO: {po_number}")
            page.goto(f"https://www.partnersbiz.com/app?po_number={po_number}")
            time.sleep(random.randint(2, 5))
            page.wait_for_load_state('load')
           
            try:
                with page.expect_download(timeout=10000) as download_info:
                    page.click('text="Discrepancy Note"')
                download = download_info.value

                if download:
                    download_path = os.path.join(self.download_path, f"{po_number}_discrepancy_note.pdf")
                    download.save_as(download_path)
                    print(f'Downloaded to: {download_path}')
                else:
                    print("No file was downloaded.")

            except Exception as e:
                print("Error during download:", str(e))

# Usage example
start_date = '01/10/24'
end_date = '28/10/24'

downloader = BlinkitPODownloader(start_date, end_date)
downloader.download_po_orders()
