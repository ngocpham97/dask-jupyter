import ssl
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

class WeakCiphersAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        # Cho phép ciphers yếu, tránh lỗi DH_KEY_TOO_SMALL
        ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

session = requests.Session()
session.mount("https://", WeakCiphersAdapter())

def get_access_token():
    url = "https://dvkh.vetc.com.vn/api/v2/authentication/oauth2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": "w3quP6QAGENf9To8YYLNPutRJ5qfVOm85S24qazz",
        "client_secret": "VCCOtfa7sXKNPgDkNpONPhmsHrbFcfnjtwx64tst"
    }

    response = session.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    print(get_access_token())
