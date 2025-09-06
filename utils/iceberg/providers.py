# Basic Libraries
from pyiceberg.catalog import load_catalog
import dask.dataframe as dd
import logging
from pyiceberg.exceptions import NoSuchNamespaceError
import pyiceberg.catalog.rest
from variables.keycloak import KeycloakConfig
from variables.lakekeeper import LakekeeperConfig
from variables.helper import ConfigLoader
import pyarrow as pa
from typing import List, Optional
from pyiceberg.catalog import Catalog

from utils.iceberg.helper import build_iceberg_schema, namespace_exists_in_catalog, iceberg_write_mode

YELLOW = "\033[93m"
GREEN = "\033[92m"
RESET = "\033[0m"


def load_table_from_iceberg(catalog_config, schema_name, table_name, output_format='iceberg', npartitions=1):
    """
    Load a table from an Iceberg catalog and return it in the specified format.

    Args:
        catalog_config (dict): Configuration dictionary for connecting to the Iceberg catalog.
                               Expected key: 'catalog' (contains connection parameters).
        schema_name (str): Name of the schema where the table is located.
        table_name (str): Name of the Iceberg table to be loaded.
        output_format (str, optional): Desired output format. Options include:
            - 'iceberg': Returns the Iceberg table object.
            - 'pyarrow': Returns a PyArrow Table.
            - 'pandas': Returns a Pandas DataFrame.
            - 'dask': Returns a Dask DataFrame.
          Default is 'iceberg'.
        npartitions (int, optional): Number of partitions when converting to a Dask DataFrame.
                                     Default is 1.

    Returns:
        Union[IcebergTable, pyarrow.Table, pandas.DataFrame, dask.dataframe.DataFrame]:
        The table in the specified format.

    Raises:
        ValueError: If `output_format` is not one of the expected options.
    """

    # Load the catalog
    catalog_instance = load_catalog("rest", **catalog_config['catalog'])
    full_table_name = f"{schema_name}.{table_name}"

    # Load the table from Iceberg
    table = catalog_instance.load_table(full_table_name)

    # Convert to the requested format
    if output_format == 'iceberg':
        return table
    elif output_format == 'pyarrow':
        return table.scan().to_arrow()
    elif output_format == 'pandas':
        return table.scan().to_arrow().to_pandas()
    elif output_format == 'dask':
        return dd.from_pandas(table.scan().to_arrow().to_pandas(), npartitions=npartitions)
    else:
        raise ValueError(
            "Invalid 'output_format'. Expected one of: 'iceberg', 'pyarrow', 'pandas', 'dask'."
        )

def write_table_to_iceberg(
    table_name: str,
    df: pa.Table,
    namespace: str,
    catalog: Catalog,
    config_schema: List[dict],
    key_columns: Optional[List[str]] = None,
    partition_columns: Optional[List[str]] = None,
    sort_columns: Optional[List[str]] = None,
    write_mode: str = "append",
) -> None:
    """
    Write a PyArrow table to an Apache Iceberg catalog with schema validation.

    Args:
        table_name (str): Name of the target Iceberg table (without namespace).
        df (pa.Table): PyArrow table containing the data to write.
        namespace (str): Namespace in the catalog where the table resides.
        catalog (Catalog): Iceberg catalog instance.
        config_schema (List[dict]): Schema definition provided externally.
            Example: [{"name": "id", "data_type": "int"}, {"name": "event_time", "data_type": "timestamp"}]
        key_columns (Optional[List[str]]): List of primary/identifier columns.
        partition_columns (Optional[List[str]]): Columns used for partitioning.
        sort_columns (Optional[List[str]]): Columns used for sorting.
        write_mode (str): One of ["append", "upsert", "overwrite"].

    Raises:
        NoSuchNamespaceError: If the provided namespace does not exist.
        ValueError: If schema mismatch is detected or invalid write_mode is provided.
    """

    key_columns = key_columns or []
    partition_columns = partition_columns or []
    sort_columns = sort_columns or []
    full_table_name = f"{namespace}.{table_name}"

    if not namespace_exists_in_catalog(namespace, catalog):
        raise NoSuchNamespaceError(f"Namespace '{namespace}' does not exist")

    # Case 1: Table already exists
    if catalog.table_exists(full_table_name):
        table = catalog.load_table(full_table_name)
        print("Existing table schema:", table.schema)
        if write_mode in ("append", "upsert"):
            actual_schema = {f.name: str(f.field_type) for f in table.schema().fields}
            expected_schema = {c["name"]: str(c["data_type"]) for c in config_schema}

            schema_diffs = []
            for col, expected_dtype in expected_schema.items():
                if col not in actual_schema:
                    schema_diffs.append((col, expected_dtype, None))
                elif actual_schema[col].lower() != expected_dtype.lower():
                    schema_diffs.append((col, expected_dtype, actual_schema[col]))

            if schema_diffs:
                header = f"{'Column':<25} {'Expected (config)':<25} {'Actual (catalog)':<25}"
                lines = [header, "-" * len(header)]
                for col, expected, actual in schema_diffs:
                    lines.append(f"{col:<25} {expected or '-':<25} {actual or '-':<25}")

                error_msg = "\n".join(lines)
                logging.error("Schema mismatch detected:\n%s" % error_msg)
                raise ValueError(
                    f"Schema mismatch detected when writing to '{full_table_name}':\n\n{error_msg}"
                )

            logging.info("Appending data to existing table '%s'" % full_table_name)
            return iceberg_write_mode(df, full_table_name, catalog, write_mode, join_columns=key_columns)

        elif write_mode == "overwrite":
            logging.warning(YELLOW + "Overwriting existing table '%s'" % full_table_name + RESET)

            iceberg_schema, partition_spec, sort_order_spec = build_iceberg_schema(
                df, config_schema, key_columns, partition_columns, sort_columns
            )

            catalog.drop_table(full_table_name)
            logging.info(GREEN + "Dropped table '%s'. Re-creating..." % full_table_name + RESET)

            catalog.create_table(
                identifier=full_table_name,
                schema=iceberg_schema,
                partition_spec=partition_spec,
                sort_order=sort_order_spec,
            )

            return iceberg_write_mode(df, full_table_name, catalog, write_mode, join_columns=key_columns)

        else:
            raise ValueError(f"Invalid write mode '{write_mode}' for existing table.")

    # Case 2: Table does not exist, create it
    logging.info(YELLOW + "Creating new table '%s'" % full_table_name + RESET)
    iceberg_schema, partition_spec, sort_order_spec = build_iceberg_schema(
        df, config_schema, key_columns, partition_columns, sort_columns
    )

    catalog.create_table(
        identifier=full_table_name,
        schema=iceberg_schema,
        partition_spec=partition_spec,
        sort_order=sort_order_spec,
    )

    return iceberg_write_mode(df, full_table_name, catalog, write_mode, join_columns=key_columns)

def load_lakekeeper_catalog(catalog_name, warehouse):
    """
    Initialize and return a Lakekeeper-backed Iceberg REST catalog.

    This function loads configuration values from both `LakekeeperConfig`
    and `KeycloakConfig` using `ConfigLoader`. It then constructs and
    returns a `RestCatalog` instance from the `pyiceberg` library, which
    can be used to interact with Iceberg tables through the Lakekeeper
    service.

    Args:
        catalog_name (str):
            The logical name to assign to the catalog instance.
        warehouse (str):
            The warehouse location (e.g., base storage path) associated
            with the catalog.

    Returns:
        pyiceberg.catalog.rest.RestCatalog:
            A configured Iceberg REST catalog client authenticated
            against Lakekeeper via Keycloak OAuth2.

    Raises:
        KeyError: If required configuration variables are missing.
        Exception: If the catalog initialization fails.

    Example:
        >> catalog = load_lakekeeper_catalog("analytics", "s3://my-bucket/warehouse")
        >> tables = catalog.list_tables()
    """
    lakekeeper_catalog_config = ConfigLoader.load_multiple([
        LakekeeperConfig,
        KeycloakConfig
    ])

    catalog = pyiceberg.catalog.rest.RestCatalog(
        name=catalog_name,
        uri=lakekeeper_catalog_config['LAKEKEEPER_CATALOG_URI'],
        warehouse=warehouse,
        credential=lakekeeper_catalog_config['LAKEKEEPER_CREDENTIAL'],
        scope="lakekeeper",
        **{
            "oauth2-server-uri": lakekeeper_catalog_config['KEYCLOAK_OAUTH2_SERVER_URI']
        },
    )
    return catalog

def read_table_from_iceberg(table_name, warehouse, namespace, columns=None, filters=None):
    """
    Read data from an Iceberg table using the Lakekeeper catalog and return it as a PyArrow Table.

    Args:
        table_name (str): Name of the Iceberg table.
        warehouse (str): Warehouse path for Lakekeeper.
        namespace (str): Namespace of the table.
        columns (list, optional): List of columns to select. If None, all columns are returned.
        filters (dict, optional): Dictionary of column-value pairs to filter the data.

    Returns:
        pyarrow.Table: Table data as a PyArrow Table.

    Example:
        >>> read_table_from_iceberg(
        ...     table_name="customer",
        ...     warehouse="warehouse",
        ...     namespace="crm",
        ...     columns=["id", "name"],
        ...     filters={"active": True}
        ... )
    """
    # Khởi tạo Lakekeeper catalog
    catalog = load_lakekeeper_catalog(catalog_name="rest", warehouse=warehouse)
    full_table_name = f"{namespace}.{table_name}"
    # Load bảng từ Iceberg
    table = catalog.load_table(full_table_name)

    # Áp dụng filters nếu có
    scan = table.scan()
    # if filters:
    #     for column, value in filters.items():
    #         scan = scan.filter(f"{column} == '{value}'")

    # # Chọn các cột cần thiết
    # print("Columns selected:", columns)
    # if columns:
    #     scan = scan.select(columns)

    # Trả về pyarrow.Table
    return scan.to_arrow()
