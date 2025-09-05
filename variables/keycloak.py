from variables.helper import BaseConfig

class KeycloakConfig(BaseConfig):
    """
    Configuration class for Keycloak integration.

    This class extends `BaseConfig` and manages the configuration
    required to connect to a Keycloak server for OAuth2 authentication.

    Variables:
        KEYCLOAK_OAUTH2_SERVER_URI (str):
            The base URI of the Keycloak server used for OAuth2 authentication.
    """

    VARIABLES = [
        "KEYCLOAK_OAUTH2_SERVER_URI"
    ]
