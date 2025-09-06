import ssl
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

CRM_API_DATA_FIELDS = ['so_number', 'order_type', 'vehicle_owner_name', 'address', 'phone', 'id_number',
 'vehicle_brand', 'license_plate', 'chassis_number', 'engine_number', 'product_package',
 'product_fee', 'total_amount', 'agent_code', 'agent_name', 'insurance_provider_code',
 'insurance_provider_name', 'sales_staff', 'valid_from', 'valid_to', 'issue_date',
 'certificate_number', 'rsa_type']

class WeakCiphersAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        # Cho phép ciphers yếu, tránh lỗi DH_KEY_TOO_SMALL
        ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

session = requests.Session()
session.mount("https://", WeakCiphersAdapter())


def get_crm_token(auth_url, client_id, client_secret, headers=None):
    """
    Retrieve access token from CRM authentication API.

    Args:
        auth_url (str): The authentication endpoint URL.
        payload (dict): The authentication payload (e.g., username, password).
        headers (dict, optional): Additional headers for the request.

    Returns:
        str: Access token string.

    Raises:
        requests.HTTPError: If the request fails.
    """
    headers = headers or {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    response = session.post(auth_url, headers=headers, data=data)
    response.raise_for_status()
    return response.json().get("access_token")

def push_data_to_crm(data, crm_endpoint, token):
    """
    Push data to CRM via API.

    Args:
        data (dict or list): Data to send.
        endpoint (str): CRM API endpoint.
        token (str): Bearer token for authentication.

    Returns:
        dict: Response from CRM API.

    Raises:
        requests.HTTPError: If the request fails.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = session.post(crm_endpoint, headers=headers, data=data)
    response.raise_for_status()
    return response.json()

def convert_data_to_payload_format(df):
    """
    Convert DataFrame to CRM-compatible format based on a mapping.

    Args:
        df (pd.DataFrame): Input DataFrame.
        mapping (dict): Mapping of DataFrame columns to CRM fields.

    Returns:
        list: List of dictionaries formatted for CRM.
    """
    crm_data = []
    for row in df:
        record = {crm_field: row[crm_field] if crm_field in row.keys() else None for crm_field in CRM_API_DATA_FIELDS}
        crm_data.append(record)
    print(crm_data)
    return crm_data