import requests

def get_crm_token(auth_url, payload, headers=None):
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
    headers = headers or {"Content-Type": "application/json"}
    response = requests.post(auth_url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json().get("access_token")

def push_data_to_crm(data, endpoint, token):
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
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(endpoint, json=data, headers=headers)
    response.raise_for_status()
    return response.json()

def convert_data_to_payload_format(df, mapping):
    """
    Convert DataFrame to CRM-compatible format based on a mapping.

    Args:
        df (pd.DataFrame): Input DataFrame.
        mapping (dict): Mapping of DataFrame columns to CRM fields.

    Returns:
        list: List of dictionaries formatted for CRM.
    """
    crm_data = []
    for _, row in df.iterrows():
        record = {crm_field: row[df_col] for df_col, crm_field in mapping.items() if df_col in row}
        crm_data.append(record)
    return crm_data