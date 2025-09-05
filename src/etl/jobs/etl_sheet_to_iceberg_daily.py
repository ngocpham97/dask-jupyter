from utils.common.yaml_providers import parse_yaml_file_to_json
import logging
from variables.crm import CRMConfig
from src.etl.transform.sheet2iceberg import SheetToIcebergJob

logger = logging.getLogger(__name__)

crm_config = CRMConfig.load()

def run_etl_sheet_to_iceberg_daily():
    try:
        sheet_config = parse_yaml_file_to_json('configs/sheet_config.yml')
        for sheet_cfg in sheet_config['sheets']:
            credentials_path = sheet_cfg['credentials_path']
            job = SheetToIcebergJob(
                sheet_config=sheet_cfg,
                credentials_path=credentials_path
            )
            job.run()
            logger.info("ETL from Google Sheet to Iceberg completed successfully.")
    except Exception as e:
        logger.error("Error during ETL from Google Sheet to Iceberg: %s", e)
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_etl_sheet_to_iceberg_daily()