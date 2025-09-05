import clickhouse_connect
class ClickhouseConnector:
    """
    A connector class for interacting with a ClickHouse database.
    """

    def __init__(self, host, port, user, password):
        """
        Initializes the ClickhouseConnector with connection parameters.

        Args:
            host (str): Hostname or IP address of the ClickHouse server.
            port (int): Port number of the ClickHouse server.
            user (str): Username for authentication.
            password (str): Password for authentication.
        """

        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password

    def __variable__(self):
        """
        Returns the connection details as a string.

        Returns:
            str: A formatted string representing the connection details.
        """

        return f"ClickHouseConnection(host={self.__host}, port={self.__port}, user={self.__user}, password={self.__password}"

    def connect_client(self):
        """
        Creates a ClickHouse client and establishes a connection.

        Returns:
            Client: The ClickHouse client instance.

        Raises:
            Exception: If the connection to ClickHouse fails.
        """

        try:
            client = clickhouse_connect.get_client(
                host=self.__host,
                port=self.__port,
                user=self.__user,
                password=self.__password
            )
            return client
        except Exception as e:
            raise

    def close(self):
        """Closes the ClickHouse client connection."""

        if self.connect_client:
            try:
                self.connect_client.close()
                print("Clickhouse client connection closed successfully.")
            except Exception as e:
                print(f"Failed to close ClickHouse connection: {e}")
            finally:
                self.connect_client = None
