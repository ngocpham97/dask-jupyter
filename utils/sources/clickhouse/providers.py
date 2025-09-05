import pandas as pd
from utils.sources.clickhouse.helper import connect_to_clickhouse
from variables.clickhouse import ClickHouseConfig
from utils.common.data_type.pandas_dtype_helper import cast_dataframe_dtypes
from typing import Optional, Union, List
import pyarrow as pa
import dask.dataframe as dd



def clickhouse_execute_query(
    query: str,
    output_format: Optional[str] = None,
    dtype_mapping: Optional[dict] = None
) -> Union[List[tuple], pd.DataFrame, pa.Table, dd.DataFrame]:
    """
    Execute a SQL query on ClickHouse and return the result in multiple formats.

    Args:
        query (str): SQL query string to be executed.
        output_format (str, optional): One of ['pandas', 'pyarrow', 'dask'].
        dtype_mapping (dict, optional): Mapping column names to dtypes. Used if applicable.

    Returns:
        Union[List[tuple], pd.DataFrame, pa.Table, dd.DataFrame]: Query result.

    Raises:
        RuntimeError: If query fails.
    """
    try:
        # Connect to ClickHouse
        clickhouse_config = ClickHouseConfig.load()
        conn = connect_to_clickhouse(clickhouse_config)
        client = conn.connect_client()

        # Execute query
        result = client.query(query)
        rows = result.result_rows
        columns = result.column_names

        if output_format == 'pandas':
            df = pd.DataFrame(rows, columns=columns)
            if dtype_mapping:
                df = cast_dataframe_dtypes(df, dtype_mapping)
            return df

        elif output_format == 'pyarrow':
            table = pa.Table.from_pylist([dict(zip(columns, row)) for row in rows])
            if dtype_mapping:
                # Cast to Arrow schema if mapping provided
                arrow_schema = pa.schema([
                    (col, dtype_mapping[col])
                    for col in columns if col in dtype_mapping
                ])
                table = table.cast(arrow_schema)
            return table

        elif output_format == 'dask':
            # Convert to Pandas first, then Dask
            df = pd.DataFrame(rows, columns=columns)
            if dtype_mapping:
                df = cast_dataframe_dtypes(df, dtype_mapping)
            ddf = dd.from_pandas(df, npartitions=1)
            return ddf

        else:
            # Return raw tuples
            return rows

    except Exception as err:
        raise RuntimeError(f"ClickHouse query failed: {err}") from err
