from variables.helper import BaseConfig

class MinIOConfig(BaseConfig):
    """
    Configuration loader for accessing MinIO S3-compatible storage service.

    Inherits from BaseConfig and defines the required environment variables
    for MinIO-based storage access. Provides utility methods to construct
    client connection options for libraries like `fsspec`, `s3fs`, or AWS SDKs.

    Required Environment Variables:
        - MINIO_ENDPOINT_URL: The full URL to the MinIO server (e.g., http://minio.local:9000)
        - MINIO_ACCESS_KEY_ID: Access key for MinIO authentication
        - MINIO_SECRET_ACCESS_KEY: Secret key for MinIO authentication
    """

    VARIABLES = [
        "MINIO_ENDPOINT_URL",
        "MINIO_ACCESS_KEY_ID",
        "MINIO_SECRET_ACCESS_KEY",
    ]

    @classmethod
    def get_storage_options(cls) -> dict:
        """
        Construct a storage options dictionary from the configured MinIO credentials.

        This is commonly used to configure:
            - `fsspec` or `s3fs` for Pandas, PyArrow, or Dask
            - Custom S3 clients for read/write operations to MinIO

        Returns:
            dict: A dictionary with the following structure:
                {
                    "key": <MINIO_ACCESS_KEY_ID>,
                    "secret": <MINIO_SECRET_ACCESS_KEY>,
                    "client_kwargs": {
                        "endpoint_url": <MINIO_ENDPOINT_URL>
                    }
                }

        Raises:
            ValueError: If one or more required environment variables are missing.

        Example:
            >> options = MinIOConfig.get_storage_options()
            >> s3 = s3fs.S3FileSystem(**options)
        """
        # Load all configured MinIO variables
        minio_vars = cls.load()

        access_key = minio_vars.get("MINIO_ACCESS_KEY_ID")
        secret_key = minio_vars.get("MINIO_SECRET_ACCESS_KEY")
        endpoint_url = minio_vars.get("MINIO_ENDPOINT_URL")

        # Validate presence
        missing_vars = []
        if not access_key:
            missing_vars.append("MINIO_ACCESS_KEY_ID")
        if not secret_key:
            missing_vars.append("MINIO_SECRET_ACCESS_KEY")
        if not endpoint_url:
            missing_vars.append("MINIO_ENDPOINT_URL")

        if missing_vars:
            raise ValueError(
                f"Missing required MinIO environment variables: {', '.join(missing_vars)}"
            )

        return {
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {
                "endpoint_url": endpoint_url
            }
        }
