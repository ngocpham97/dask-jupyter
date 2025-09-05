from variables.helper import BaseConfig

class ClickHouseConfig(BaseConfig):
    """
    Configuration class for Clickhouse environment variables.

    Inherits from BaseConfig and sets a predefined list of variables specific to Clickhouse.

    Class Attributes:
        VARIABLES (list): Contains Clickhouse-specific environment variable names such as:
            - 'CDP_PROD_CREDENTIALS_CONFIG'
            - 'CLICKHOUSE_PORT'
            - 'CLICKHOUSE_USER'
            - 'CLICKHOUSE_PASSWORD'
    """

    VARIABLES = [
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_PORT",
        "CLICKHOUSE_USER",
        "CLICKHOUSE_PASSWORD"
    ]
