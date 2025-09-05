import logging
import pyarrow as pa

from utils.iceberg.helper import set_nullable_false_for_primary
from utils.iceberg.providers import write_table_to_iceberg, load_lakekeeper_catalog
from utils.sources.clickhouse.providers import clickhouse_execute_query

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
YELLOW = "\033[93m"
GREEN = "\033[92m"
RESET = "\033[0m"


class ClickHouseToIcebergJob:
    """
    A pipeline job with 3 clear stages:
      1. Load Iceberg catalog
      2. Read data from ClickHouse
      3. Write result to Iceberg
    """

    def __init__(self, clickhouse_config: dict, model_config: dict, query: str):
        """
        Initialize the job.

        Args:
            clickhouse_config (dict): ClickHouse connection info.
            yml_model_config (dict): YAML model config already parsed.
            query (str): SQL query to run on ClickHouse.
        """
        self.clickhouse_config = clickhouse_config
        self.model_config = model_config
        self.query = query

        logger.info(GREEN + "Job initialized for model: %s",
                    self.model_config['models'][0]['name'] + RESET)

    # -------------------------
    # Stage 1: Load Iceberg catalog
    # -------------------------
    def load_iceberg_catalog(self):
        logger.info(YELLOW + "Loading Iceberg catalog..." + RESET)
        self.iceberg_catalog = load_lakekeeper_catalog(catalog_name="rest", warehouse=self.model_config['models'][0]['config']['run']['database'])
        logger.info(GREEN + "Iceberg catalog loaded successfully." + RESET)

    # -------------------------
    # Stage 2: Read from ClickHouse
    # -------------------------
    def read_clickhouse(self) -> pa.Table:
        logger.info(YELLOW + "Executing query on ClickHouse..." + RESET)
        df = clickhouse_execute_query(
            self.query,
            output_format='pyarrow'
        )
        logger.info(GREEN + "Fetched %d rows from ClickHouse." % len(df) + RESET)

        primary_keys = (
            self.model_config['models'][0]
            ['config']['key']['primary_key']
        )
        df = set_nullable_false_for_primary(df, primary_keys)
        logger.info(GREEN + "Primary keys %s set as NOT NULL." % primary_keys + RESET)
        return df

    # -------------------------
    # Stage 3: Write to Iceberg
    # -------------------------
    def write_to_iceberg(self, df: pa.Table):
        model = self.model_config['models'][0]
        output_cfg = model['config']['key']

        logger.info(YELLOW + "Writing to Iceberg table: %s" % model['name'] + RESET)
        write_table_to_iceberg(
            table_name=model['name'],
            df=df,
            namespace=model['config']['run']['namespace'],
            catalog=self.iceberg_catalog,
            config_schema=model['columns'],
            key_columns=output_cfg['primary_key'],
            partition_columns=output_cfg['partition_by'],
            sort_columns=output_cfg['order_by'],
            write_mode=model['config']['run']['mode'],
        )
        logger.info(GREEN + "Write completed for table %s" % model['name'] + RESET)

    # -------------------------
    # Run pipeline
    # -------------------------
    def run(self):
        self.load_iceberg_catalog()
        df = self.read_clickhouse()
        self.write_to_iceberg(df)
        logger.info(GREEN + "Job finished successfully." + RESET)
