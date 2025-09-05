from variables.helper import BaseConfig

class LakekeeperConfig(BaseConfig):
    """
    Configuration class for Lakekeeper integration.

    This class extends `BaseConfig` and manages the configuration
    variables required to connect to a Lakekeeper catalog.

    Variables:
        LAKEKEEPER_CATALOG_URI (str):
            The URI of the Lakekeeper catalog to connect to.
        LAKEKEEPER_SCOPE (str):
            The authorization or access scope used when authenticating
            with the Lakekeeper service.
        LAKEKEEPER_CREDENTIAL (str):
            The credential (e.g., API key or token) used for
            authentication with the Lakekeeper catalog.
    """

    VARIABLES = [
        "LAKEKEEPER_CATALOG_URI",
        "LAKEKEEPER_SCOPE",
        "LAKEKEEPER_CREDENTIAL"
    ]
