import logging
from utils.iceberg.providers import read_table_from_iceberg, load_lakekeeper_catalog
from utils.crm.api import push_data_to_crm

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class IcebergToCRMJob:
    def __init__(self, iceberg_config, crm_config):
        self.iceberg_config = iceberg_config
        self.crm_config = crm_config

    def load_iceberg_catalog(self):
        self.catalog = load_lakekeeper_catalog(
            catalog_name=self.iceberg_config['catalog_name'],
            warehouse=self.iceberg_config['warehouse']
        )

    def read_iceberg(self):
        return read_table_from_iceberg(
            table_name=self.iceberg_config['name'],
            catalog=self.catalog,
            namespace=self.iceberg_config['namespace'],
            columns=self.iceberg_config.get('columns'),
            filters=self.iceberg_config.get('filters')
        )

    def push_to_crm(self, df):
        # Chuyển đổi df (pyarrow.Table) sang list[dict] hoặc json phù hợp
        data = df.to_pandas().to_dict(orient='records')
        return push_data_to_crm(
            data=data,
            endpoint=self.crm_config['endpoint'],
            token=self.crm_config['token']
        )

    def run(self):
        self.load_iceberg_catalog()
        df = self.read_iceberg()
        result = self.push_to_crm(df)
        logger.info("Push to CRM completed: %s", result)