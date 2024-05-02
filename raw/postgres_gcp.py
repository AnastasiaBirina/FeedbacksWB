import psycopg2
from datetime import date, timedelta
from airflow.hooks.base import BaseHook  
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import logging
import gspread


log = logging.getLogger(__name__)

class google_sheets_loader():
    def __init__(self, ds):
        self.hook = GoogleBaseHook(gcp_conn_id="GOOGLE_CONNECTION")
        self.credentials = self.hook.get_credentials()
        self.google_credentials = gspread.Client(auth=self.credentials)
        # Reading a spreadsheet by its title
        self.sheet = self.google_credentials.open("WB_feedbacks_top_7")
        # Defining the worksheet to manipulate
        self.worksheet = self.sheet.worksheet("WB_feedbacks_top_7")
        self.ds = ds
  
    def delete_data(self):
        cells = self.worksheet.range("A1:C7")
        # Очищаем значения в ячейках
        for cell in cells:
            cell.value = ""
        self.worksheet.update_cells(cells)