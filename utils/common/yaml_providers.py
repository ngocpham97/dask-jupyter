import yaml


def parse_yaml_file_to_json(yaml_file_path: str) -> dict:
    """
    Parse a YAML configuration file and return its contents as a Python dictionary.

    Parameters:
        yaml_file_path (str): The file path to the YAML file.

    Returns:
        dict: The parsed contents of the YAML file represented as a dictionary.

    Raises:
        FileNotFoundError: If the specified YAML file does not exist.
        yaml.YAMLError: If the file is not a valid YAML format.
    """
    with open(yaml_file_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config
