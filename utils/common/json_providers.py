import json
import pandas as pd
from typing import List

def parse_json_column(df: pd.DataFrame, parse_column: str, expected_columns: List[str]) -> pd.DataFrame:
    """
    Parses a DataFrame column containing JSON strings into structured columns.

    Args:
        df (pd.DataFrame): Input DataFrame with a JSON string column.
        parse_column (str): Name of the column containing JSON strings.
        expected_columns (List[str]): List of expected keys to extract from each JSON object.

    Returns:
        pd.DataFrame: A new DataFrame with columns corresponding to the expected JSON keys.

    Notes:
        - All keys are converted to lowercase for consistency.
        - Malformed JSON or missing values are handled gracefully by filling None.
        - Suitable for use with Dask's map_partitions by providing appropriate `meta`.

    Example:
        >>> df = pd.DataFrame({'payload': ['{"name": "Alice"}', '{"name": "Bob"}']})
        >>> parse_json_column(df, 'payload', ['name'])
            name
        0  Alice
        1    Bob
    """
    parsed_records = []

    for json_str in df[parse_column]:
        row_data = {}

        # Handle null / NA values explicitly
        if pd.isna(json_str):
            for col in expected_columns:
                row_data[col.lower()] = None
            parsed_records.append(row_data)
            continue

        try:
            raw_data = json.loads(json_str)
            raw_data = {k.lower(): v for k, v in raw_data.items()}
            for col in expected_columns:
                row_data[col.lower()] = raw_data.get(col.lower(), None)
        except json.JSONDecodeError:
            print(f"[Warning] Malformed JSON. Data snippet: {str(json_str)[:100]}...")
            for col in expected_columns:
                row_data[col.lower()] = None
        except Exception as e:
            print(f"[Error] Unexpected error: {e}. Data snippet: {str(json_str)[:100]}...")
            for col in expected_columns:
                row_data[col.lower()] = None

        parsed_records.append(row_data)

    return pd.DataFrame(parsed_records)
