import gspread
import pandas as pd
import re
from datetime import datetime

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

def normalize_date_columns(df, columns):
    """
    Normalize date strings in multiple pandas DataFrame columns from formats like
    '12h00\n14/06/2025' to '2025-06-14'.
    Args:
        df (pd.DataFrame): The DataFrame to process.
        columns (list): List of column names to normalize.
    Returns:
        pd.DataFrame: DataFrame with normalized date columns.
    """
    def extract_date(val):
        if isinstance(val, str):
            match = re.search(r'(\d{2})/(\d{2})/(\d{4})', val)
            if match:
                day, month, year = match.groups()
                try:
                    return datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d").date().isoformat()
                except Exception:
                    return None
        return None
    for column in columns:
        if column in df.columns:
            df[column] = df[column].apply(extract_date)
    return df

