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

def normalize_payload(data: dict) -> dict:
    """Chuẩn hóa dữ liệu CRM để không còn None/null."""
    normalized = {}
    for k, v in data.items():
        if v is None:
            normalized[k] = ""   # thay None bằng chuỗi rỗng
        else:
            normalized[k] = v
    return normalized


def push_data_to_crm(data, crm_endpoint, token):
    """
    Push data to CRM via API.

    Args:
        data (dict or list): Data to send.
        endpoint (str): CRM API endpoint.
        token (str): Bearer token for authentication.

    Returns:
        dict: Response from CRM API or error info.

    Raises:
        requests.HTTPError: If the request fails.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Cookie": f"session_id={token}"
    }
    payload = normalize_payload(data)
    try:
        response = session.post(crm_endpoint, headers=headers, json=payload)
        print(response.status_code, response.text)
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as e:
        print(f"HTTPError: {e}")
        print(f"Response: {getattr(e.response, 'text', None)}")
        return {"error": str(e), "response": getattr(e.response, 'text', None)}
    except Exception as ex:
        print(f"Exception: {ex}")
        return {"error": str(ex)}

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
    return crm_data

if __name__ == "__main__":
    push_data_to_crm(data={}, crm_endpoint="https://dev-dvkh.vetc.com.vn/api/partner_rsa", token="")