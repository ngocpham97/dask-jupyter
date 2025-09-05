from utils.sources.clickhouse.module import ClickhouseConnector

def connect_to_clickhouse(clickhouse_config):
    """
    Establishes a connection to a ClickHouse database.

    Args:
        clickhouse_config (dict): A dictionary containing ClickHouse connection parameters:
            - CLICKHOUSE_HOST (str): The hostname or IP address of the ClickHouse server.
            - CLICKHOUSE_PORT (int): The port number of the ClickHouse server.
            - CLICKHOUSE_USER (str): The username for authentication.
            - CLICKHOUSE_PASSWORD (str): The password for authentication.

    Returns:
        ClickhouseConnector: An instance of the ClickhouseConnector class, representing the connection to the ClickHouse server.
    """

    conn_clickhouse = ClickhouseConnector(
        host=clickhouse_config['CLICKHOUSE_HOST'],
        port=clickhouse_config['CLICKHOUSE_PORT'],
        user=clickhouse_config['CLICKHOUSE_USER'],
        password=clickhouse_config['CLICKHOUSE_PASSWORD']
    )

    return conn_clickhouse