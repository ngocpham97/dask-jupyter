from variables.helper import BaseConfig

class ETLConfig(BaseConfig):
    """
    Configuration class for ELT variables.

    Inherits from BaseConfig and sets a predefined list of variables specific to ELT.

    Class Attributes:
        VARIABLES (list): Contains etl-specific environment variable names such as:
            - 'ETL_INIT_START_DATE'
            - 'ETL_INIT_END_DATE'
    """

    VARIABLES = [
        "ETL_INIT_START_DATE",
        "ETL_INIT_END_DATE"
    ]
