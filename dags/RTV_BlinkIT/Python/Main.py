from playwright.sync_api import sync_playwright
import time
from datetime import datetime, timedelta
import os
import random
import shutil
from Excel_To_PDF import *
class BlinkitPODownloader:
    def __init__(self, start_date, end_date , end_date_plus_one):
        # Initialize with the start date, end date, and download path for PO files
        self.start_date = start_date
        self.end_date = end_date
        self.Base_Path =os.path.join(os.path.dirname(__file__),"RTV_FOLDER")
        self.po_orders = []  # List to store PO Numbers
        self.dates = self.get_date_range(start_date, end_date)  # Generate a range of dates to check
        self.end_date_plus_one = end_date_plus_one
        self.end_date_obj = datetime.strptime(self.end_date_plus_one, '%d %b %y')
        
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
            time.sleep(8)
            page.wait_for_selector(".ant-table-row.ant-table-row-level-0", timeout=20000)  # Timeout is optional, default is 30s
            
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
                    #Logic if the start - 1 se lower date is there then break it

                    comparison_date = datetime.strptime(fifth_cell_text,'%d %b %y')
                    
                    if self.end_date_obj  > comparison_date:
                        state="Stop"
                        break
                    
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
        return self.download_path 

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
start_date = '20/10/24'
end_date = '30/10/24'

#End_Date+1
end_date_obj = datetime.strptime(start_date, '%d/%m/%y')
end_date_plus_one = (end_date_obj - timedelta(days=1)).strftime('%d %b %y')


downloader = BlinkitPODownloader(start_date, end_date , end_date_plus_one)
download_path = downloader.download_po_orders()
folder_path = download_path  # Change this to the path of your PDF folder
output_excel = os.path.join(download_path,"Blinkit_discrepancy.csv")  # Name of the output Excel file

all_in_one=[]
for filename in os.listdir(folder_path):
    if filename.endswith(".pdf"):  # Case-insensitive check for PDF files
        pdf_path = os.path.join(folder_path, filename)
        sheet_name = filename[:-4]  # Use the PDF file name as the sheet name
        print(f"Processing: {pdf_path}")
        text_col = []
        text_col = extract_text_from_pdf(pdf_path)
        all_table = extract_table_from_pdf(pdf_path)
        header = []
        i=0
        f=0
        for element in all_table:
            temp = [t.replace('\n',' ') if t else t for t in element]
            
            if f==1:
                if(temp[0].split(" ")[0]!='Number'):
                    all_in_one.append(text_col + temp)
            
            if temp[0] == 'Sr. no':
                header.append(['Buyers ID','Purchase No','Invoice No','HOT','Sr. No','UPC','Product Name','Qty',
                               'Unit Landing price','Subtotal','GST','Net Amount','Reason','Remark'])
                f=1
    else:
        print("It is not a pdf file")


table_op = pd.DataFrame(header + all_in_one)  # Add column names
table_op.to_csv(output_excel ,index = False, header = False)
print(f"Data written to sheet: {sheet_name}")
print(f"All data written to {output_excel}")
