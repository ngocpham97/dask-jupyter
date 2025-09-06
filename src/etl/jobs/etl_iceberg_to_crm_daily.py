from utils.common.yaml_providers import parse_yaml_file_to_json
import logging
from datetime import date
from variables.crm import CRMConfig
from src.etl.transform.iceberg2crm import IcebergToCRMJob

logger = logging.getLogger(__name__)

crm_config = CRMConfig.load()

def run_etl_iceberg_to_crm_daily():
    try:
        iceberg_config = parse_yaml_file_to_json('configs/iceberg_config.yml')
        for iceberg_cfg in iceberg_config['iceberg']['tables']:
            iceberg_cfg['filter'] = {
                "processing_date": date.today().isoformat()
            }
            job = IcebergToCRMJob(
                crm_config=crm_config,
                iceberg_config=iceberg_cfg
            )
            job.run()
            logger.info("ETL from Iceberg to CRM completed successfully.")
    except Exception as e:
        logger.error("Error during ETL from Iceberg to CRM: %s", e)
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_etl_iceberg_to_crm_daily()
