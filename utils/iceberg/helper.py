import yaml
from pyarrow import Table as ArrowTable
from pyiceberg.catalog import Catalog
from typing import Optional, List, Any, Dict
import pyarrow as pa
import re
from pyiceberg.schema import Schema, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform, DayTransform
import logging
GREEN = "\033[92m"
RESET = "\033[0m"

def resolve_pyiceberg_type(type_str: str) -> Any:
    """
    Convert a string like 'LongType()' to a PyIceberg type instance.
    """
    if not type_str.endswith("()"):
        raise ValueError(f"Invalid PyIceberg type string: {type_str}")
    type_name = type_str.replace("()", "")
    try:
        return getattr(__import__("pyiceberg.types", fromlist=[type_name]), type_name)()
    except AttributeError:
        raise ValueError(f"Unknown PyIceberg type: {type_name}")

def resolve_pyarrow_type(type_str: str) -> pa.DataType:
    """
    Convert a string like 'int64', 'timestamp[ns]', or 'timestamp[ns, UTC]' to a PyArrow type instance.
    """
    if type_str.startswith("timestamp"):
        match = re.match(r"timestamp\[(\w+)(?:,\s*(\w+))?\]", type_str)
        if not match:
            raise ValueError(f"Invalid pyarrow timestamp type string: {type_str}")
        unit, tz = match.groups()
        return pa.timestamp(unit, tz=tz) if tz else pa.timestamp(unit)

    try:
        # Try to resolve built-in types (e.g., 'int64', 'float32', 'binary')
        return getattr(pa, type_str)()
    except AttributeError:
        raise ValueError(f"Unknown PyArrow type: {type_str}")

def build_type_mapping_from_yaml(
    yaml_path: str,
    source_field: str = "pyarrow",
    target_field: str = "pyiceberg"
) -> Dict[Any, Any]:
    """
    Build a mapping from source data types to target data types using a YAML configuration.

    Args:
        yaml_path (str): Path to the YAML file.
        source_field (str): Field name representing source types (e.g., 'pyarrow').
        target_field (str): Field name representing target types (e.g., 'pyiceberg').

    Returns:
        dict: Mapping of source types to target type instances or strings.
    """
    SUPPORTED_FIELDS = {'pyarrow', 'pyiceberg', 'pandas'}

    if source_field == target_field:
        raise ValueError("source_field must be different from target_field")
    if source_field not in SUPPORTED_FIELDS:
        raise ValueError(f"Unsupported source_field: {source_field}")
    if target_field not in SUPPORTED_FIELDS:
        raise ValueError(f"Unsupported target_field: {target_field}")

    with open(yaml_path, 'r') as f:
        yaml_data = yaml.safe_load(f)

    type_map = {}
    for entry in yaml_data.get("type", []):
        src = entry[source_field]
        tgt = entry[target_field]

        # Resolve source
        if source_field == 'pyiceberg':
            src = resolve_pyiceberg_type(src)
        elif source_field == 'pyarrow':
            src = resolve_pyarrow_type(src)

        # Resolve target
        if target_field == 'pyiceberg':
            tgt = resolve_pyiceberg_type(tgt)
        elif target_field == 'pyarrow':
            tgt = resolve_pyarrow_type(tgt)

        type_map[src] = tgt

    return type_map


def namespace_exists_in_catalog(namespace: str, catalog) -> bool:
    """
    Check if a namespace exists in the Iceberg catalog.

    Args:
        namespace (str): Target namespace.
        catalog: Iceberg catalog instance.

    Returns:
        bool: True if namespace exists, else False.
    """
    return any(",".join(ns) == namespace for ns in catalog.list_namespaces())


def iceberg_write_mode(
    data: ArrowTable,
    table_identifier: str,
    iceberg_catalog: Catalog,
    write_mode: str,
    join_columns: Optional[List[str]] = None
) -> None:
    """
    Write data to an existing Iceberg table using the specified mode.

    Args:
        data (ArrowTable): PyArrow Table to write.
        table_identifier (str): Fully qualified table name (e.g., 'namespace.table').
        iceberg_catalog (Catalog): Instance of the Iceberg catalog.
        write_mode (str): One of ['append', 'upsert', 'overwrite'].
        join_columns (Optional[List[str]]): List of columns to use for upsert join (required if write_mode is 'upsert').

    Raises:
        ValueError: If write_mode is invalid or join_columns are missing when mode is 'upsert'.
    """
    table = iceberg_catalog.load_table(table_identifier)

    if write_mode == "upsert":
        if not join_columns:
            raise ValueError("Join columns must be provided for 'upsert' mode.")
        table.upsert(data, join_cols=join_columns)
        logging.info(GREEN + "Successfully upsert %d records to Iceberg table '%s'" % (len(data), table_identifier) + RESET)
    elif write_mode == "append":
        table.append(data)
        logging.info(GREEN + "Successfully append %d records to Iceberg table '%s'" % (len(data), table_identifier) + RESET)
    elif write_mode == "overwrite":
        table.overwrite(data)
    else:
        raise ValueError(f"Invalid write mode '{write_mode}'. Choose from ['append', 'upsert', 'overwrite'].")
    logging.info(GREEN + "Successfully overwrite %d records to Iceberg table '%s'" % (len(data), table_identifier) + RESET)

def set_nullable_false_for_primary(table: pa.Table, primary_columns: list[str]) -> pa.Table:
    """
    Update the schema of a PyArrow Table by setting `nullable=False`
    for columns listed in `primary_columns`.

    Parameters:
    ----------
    table : pa.Table
        The original PyArrow Table whose schema needs modification.

    primary_columns : list[str]
        A list of column names that should be marked as non-nullable.

    Returns:
    -------
    pa.Table
        A new PyArrow Table with an updated schema where the specified
        primary columns are marked as `nullable=False`.

    Notes:
    -----
    - If any column listed in `primary_columns` contains null values,
      setting `nullable=False` may cause issues when writing to systems
      that enforce strict schema validation (e.g., Apache Iceberg).
    - This function only modifies the schema metadata. It does not alter
      the actual data or perform any null checks.
    """
    new_fields = [
        pa.field(f.name, f.type, nullable=False if f.name in primary_columns else f.nullable)
        for f in table.schema
    ]
    new_schema = pa.schema(new_fields)
    return table.cast(new_schema)

def normalize_type(dtype: str) -> str:
    """Normalize timestamp types and other Iceberg-supported variations."""
    if dtype.startswith("timestamp"):
        return "timestamp[ms]"
    return dtype

def compare_schema(config_schema: dict, iceberg_schema: Schema):
    """Compare schema from config with schema of an existing Iceberg table."""
    iceberg_fields = {field.name: str(field.field_type).lower() for field in iceberg_schema.fields}

    mismatches = []
    for col, dtype in config_schema.items():
        expected = normalize_type(dtype)
        actual = normalize_type(iceberg_fields.get(col))

        if actual is None:
            mismatches.append((col, f"missing in iceberg table, expected {expected}"))
        elif expected != actual:
            mismatches.append((col, f"expected {expected}, got {actual}"))

    if mismatches:
        raise ValueError(f"Schema mismatch detected:\n" + "\n".join([f"- {c}: {m}" for c, m in mismatches]))

def build_iceberg_schema(
    df: pa.Table,
    config_schema: List[dict],
    key_columns: List[str],
    partition_columns: List[str],
    sort_columns: List[str],
):
    """Helper to build Iceberg schema, partition spec and sort order."""
    schema_fields = []
    identifier_field_ids = []
    partition_fields = []
    sort_fields = []
    for fid, col in enumerate(config_schema, start=1):
        col_name = col.get("name")
        col_type = col.get("data_type")

        if col_name not in df.schema.names:
            raise ValueError(f"Column '{col_name}' not found in incoming DataFrame")

        schema_fields.append(NestedField(fid, col_name, col_type, required=(col_name in key_columns)))

        if col_name in key_columns:
            identifier_field_ids.append(fid)

        if col_name in partition_columns:
            partition_fields.append(
                PartitionField(source_id=fid, field_id=fid, transform=DayTransform(), name=f"partition_{col_name}")
            )

        if col_name in sort_columns:
            sort_fields.append(
                SortField(source_id=fid, field_id=fid, transform=IdentityTransform())
            )

    iceberg_schema = Schema(*schema_fields, identifier_field_ids=identifier_field_ids)
    partition_spec = PartitionSpec(*partition_fields) if partition_fields else PartitionSpec()
    sort_order_spec = SortOrder(*sort_fields) if sort_fields else SortOrder()

    return iceberg_schema, partition_spec, sort_order_spec