import pyarrow as pa
import pandas as pd
from utils.google.sheets import read_google_sheet, normalize_date_columns
from utils.iceberg.providers import write_table_to_iceberg, load_lakekeeper_catalog
from utils.crm.api import get_crm_token, push_data_to_crm

class SheetToIcebergJob:
    def __init__(self, sheet_config, credentials_path):
        self.sheet_config = sheet_config
        self.credentials_path = credentials_path

    def run(self):
        # 1. Read from Google Sheet
        df = read_google_sheet(
            sheet_id=self.sheet_config['sheet_id'],
            range_name=self.sheet_config['range_name'],
            index=self.sheet_config.get('index', 0),
            credentials_path=self.credentials_path
        )
        df['rsa_type'] = 'add'
        df['processing_date'] = pd.to_datetime('today').normalize()
        # Rename all columns in df with sheet_config['columns']
        columns = [col['name'] for col in self.sheet_config['columns']]
        if 'columns' in self.sheet_config:
            df.columns = columns
        # Drop rows with too many missing values (including empty strings)
        threshold = len(df.columns) // 2
        df = df.replace('', pd.NA)
        df = df.dropna(thresh=threshold)
        df = normalize_date_columns(df, ['valid_from', 'valid_to'])
        # Convert all columns to string type
        df = df.astype(str)
        df = df.reset_index(drop=True)
        # Write to Iceberg
        catalog = load_lakekeeper_catalog(
            catalog_name=self.sheet_config['iceberg_table']['catalog_name'],
            warehouse=self.sheet_config['iceberg_table']['warehouse']
        )
        pa_table = pa.Table.from_pandas(df)
        iceberg_table = self.sheet_config['iceberg_table']
        key_columns = iceberg_table.get('primary_key') or []
        partition_columns = iceberg_table.get('partition_by') or []
        sort_columns = iceberg_table.get('order_by') or []
        write_table_to_iceberg(
            table_name=iceberg_table['name'],
            df=pa_table,
            namespace=iceberg_table['namespace'],
            catalog=catalog,
            config_schema=self.sheet_config['columns'],
            key_columns=key_columns,
            partition_columns=partition_columns,
            sort_columns=sort_columns,
            write_mode="overwrite"
        )