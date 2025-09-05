"""
helper.py

This module provides configuration helpers to load environment variables
"""

import os
import json
from dotenv import load_dotenv
from pathlib import Path
from typing import Type, Dict, Any, List

# Load .env file from project root
base_folder = Path(__file__).parent.parent.absolute()  # noqa
load_dotenv(os.path.join(str(base_folder), ".env"))  # noqa


class BaseConfig:
    """
    A base configuration class for loading environment variables.

    Class Attributes:
        VARIABLES (list): List of environment variable names to load.

    Class Methods:
        - load(mode='basic'): Load environment variables defined in VARIABLES.

    Static Methods:
        - get_variable(name, default_value=None, deserialize_json=False): Retrieve individual variable with optional JSON parsing.
    """

    VARIABLES = []  # List of environment variables to be loaded

    @classmethod
    def load(cls, mode='basic') -> dict:
        """
        Load the specified environment variables.

        Args:
            mode (str): The loading mode. Currently only 'basic' is supported.

        Returns:
            dict: Dictionary containing the variable names and their corresponding values.

        Raises:
            ValueError: If an unsupported mode is provided.
        """
        if mode == 'basic':
            config = {var: cls.get_variable(var) for var in cls.VARIABLES}
        else:
            raise ValueError(
                "Invalid 'mode' value. It must be one of the following: 'basic'.")
        return config

    @staticmethod
    def get_variable(name: str, default_value=None, deserialize_json=False):
        """
        Retrieve the value of an environment variable.

        Args:
            name (str): The name of the environment variable.
            default_value (any, optional): Default value to return if the variable is not found. Defaults to None.
            deserialize_json (bool, optional): If True, attempts to parse the value as JSON. Defaults to False.

        Returns:
            str | dict | None: The environment variable value, parsed as JSON if required.

        Raises:
            ValueError: If deserialization is enabled but the value is not a valid JSON string.
        """
        value = os.getenv(name, default_value)
        if deserialize_json and value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                raise ValueError(f"Environment variable {name} contains invalid JSON.")
        return value

class ConfigLoader:
    """
    Utility class for loading environment variables from one or multiple configs.

    Example
    -------
    >> ConfigLoader.load_single(DaskJupyterConfig)
    {'DASK_JUPYTER_IMAGE': 'ghcr.io/my-dask:latest'}

    >> ConfigLoader.load_multiple([DaskJupyterConfig, TrinoDbtConfig])
    {
        'DASK_JUPYTER_IMAGE': 'ghcr.io/my-dask:latest',
        'TRINO_DBT_IMAGE': 'ghcr.io/my-dbt:1.0',
        'TRINO_USER': 'admin',
        'TRINO_HOST': 'trino.example.com',
        'TRINO_PASS': 'xxx'
    }
    """

    @staticmethod
    def load_single(config_cls: Type[BaseConfig]) -> Dict[str, Any]:
        """
        Load variables from a single config class.

        Args:
            config_cls (Type[BaseConfig]): The configuration class to load.

        Returns:
            dict: A dictionary of environment variables defined in the config.
        """
        return config_cls.load()

    @staticmethod
    def load_multiple(config_classes: List[Type[BaseConfig]]) -> Dict[str, Any]:
        """
        Load and merge variables from multiple config classes.

        Args:
            config_classes (list[Type[BaseConfig]]): A list of configuration classes.

        Returns:
            dict: A merged dictionary containing all environment variables.
                  In case of duplicate keys, later classes in the list override earlier ones.
        """
        merged_config = {}
        for config_cls in config_classes:
            merged_config.update(config_cls.load())
        return merged_config