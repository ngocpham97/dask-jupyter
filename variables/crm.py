from variables.helper import BaseConfig

class CRMConfig(BaseConfig):
    """
    Configuration class for CRM integration.

    This class extends `BaseConfig` and manages the configuration
    required to connect to a CRM system for data integration.

    Variables:
        CRM_API_URL (str): The base URL of the CRM API.
        CRM_API_KEY (str): The API key for authenticating with the CRM API.
        CRM_TIMEOUT (int): The timeout duration for CRM API requests.
    """

    VARIABLES = [
        "CRM_API_TOKEN_URL",
        "CRM_API_CLIENT_ID",
        "CRM_API_CLIENT_SECRET",
        "CRM_API_TIMEOUT",
        "CRM_API_PUSH_DATA_URL",
        "CRM_API_GRANT_TYPE",
    ]