import gspread
import pandas as pd

def read_google_sheet(sheet_id, range_name, index, credentials_path):
    """
    Read data from Google Sheet and return as pandas.DataFrame.
    """
    gc = gspread.service_account(filename=credentials_path)
    sh = gc.open_by_key(sheet_id)
    worksheet = sh.worksheet(range_name)
    data = worksheet.get_all_records()
    data = data[index:]  # Skip the first 'index' rows if needed
    return pd.DataFrame(data)