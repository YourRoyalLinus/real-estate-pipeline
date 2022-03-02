from urllib.error import HTTPError
import requests
from requests.adapters import HTTPAdapter, Retry


_BASE_URL = "" #Get from Airflow
_HEADERS = {
    'x-rapidapi-host' : "", #Get from Airflow
    'x-rapidapi-key' : "" #Get from Airflow
}

_RETRIES = Retry(total=5, backoff_factor=2, status_forcelist=[500, 501, 502,
                                                                503, 504])
class APIExcpetion(Exception):
    pass


def fetch_v2_sold_properties(**params) -> dict:
    try:
        params["city"]
        params["state_code"]
        params["limit"]
        params["offset"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")

    endpoint = "/list-sold"
    with requests.session as session:
        session.mount("https://", HTTPAdapter(max_retries=_RETRIES))
        try:
            resp = session.get(url=_BASE_URL+endpoint, headers=_HEADERS,
                                params=params)
            resp.raise_for_status()
        except HTTPError as e:
            raise APIExcpetion(str(e))
        
    return resp.json()

def fetch_v2_for_rent_properties(**params) -> dict:
    try:
        params["city"]
        params["state_code"]
        params["limit"]
        params["offset"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")

    endpoint = "/list-for-rent"
    with requests.session as session:
        session.mount("https://", HTTPAdapter(max_retries=_RETRIES))
        try:
            resp = session.get(url=_BASE_URL+endpoint, headers=_HEADERS,
                                params=params)
            resp.raise_for_status()
        except HTTPError as e:
            raise APIExcpetion(str(e)) 
        
    return resp.json()

def fetch_v2_for_sale_properties(**params) -> dict:
    try:
        params["city"]
        params["state_code"]
        params["limit"]
        params["offset"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")

    endpoint = "/list-for-sale"
    with requests.session as session:
        session.mount("https://", HTTPAdapter(max_retries=_RETRIES))
        try:
            resp = session.get(url=_BASE_URL+endpoint, headers=_HEADERS,
                                params=params)
            resp.raise_for_status()
        except HTTPError as e:
            raise APIExcpetion(str(e)) 
            
    return resp.json()

def fetch_v2_mls_properties(**params) -> dict:
    try:
        params["mls_id"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")

    endpoint = "/list-by-mls"
    with requests.session as session:
        session.mount("https://", HTTPAdapter(max_retries=_RETRIES))
        try:
            resp = session.get(url=_BASE_URL+endpoint, headers=_HEADERS,
                                params=params)
            resp.raise_for_status()
        except HTTPError as e:
            raise APIExcpetion(str(e)) 

    return resp.json()

def fetch_v2_property_details(**params) -> dict:
    try:
        params["property_id"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")

    endpoint = "/detail"
    with requests.session as session:
        session.mount("https://", HTTPAdapter(max_retries=_RETRIES))
        try:
            resp = session.get(url=_BASE_URL+endpoint, headers=_HEADERS,
                                params=params)
            resp.raise_for_status()
        except HTTPError as e:
            raise APIExcpetion(str(e)) 

    return resp.json()
