import logging
from utils.iceberg.providers import read_table_from_iceberg, load_lakekeeper_catalog
from utils.crm.api import push_data_to_crm, get_crm_token, convert_data_to_payload_format

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
            warehouse=self.iceberg_config['warehouse'],
            namespace=self.iceberg_config['namespace'],
            columns=self.iceberg_config['columns'],
            filters=self.iceberg_config['filters']
        )

    def push_to_crm(self, df):
        # Chuyển đổi df (pyarrow.Table) sang list[dict] hoặc json phù hợp
        data = df.to_pandas().to_dict(orient='records')
        api_token = get_crm_token(auth_url=self.crm_config['CRM_API_TOKEN_URL'],
                                  client_id=self.crm_config['CRM_API_CLIENT_ID'],
                                  client_secret=self.crm_config['CRM_API_CLIENT_SECRET'])
        crm_data = convert_data_to_payload_format(data)
        for data in crm_data:
            logger.info("Prepared CRM data: %s", data)
            response = push_data_to_crm(
                data=data,
                crm_endpoint=self.crm_config['CRM_API_PUSH_DATA_URL'],
                token=api_token
            )

    def run(self):
        self.load_iceberg_catalog()
        df = self.read_iceberg()
        self.push_to_crm(df)
        logger.info("Push to CRM completed successfully.")