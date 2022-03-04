import requests
from requests import HTTPError
from requests.adapters import HTTPAdapter, Retry
from airflow.decorators import task
from airflow.models import variable

class APIExcpetion(Exception):
    pass

@task
def start_session() -> dict:
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 501, 502,
                                                                503, 504])
    session = requests.session()
    session.mount("https://", HTTPAdapter(max_retries=retries))

    return {"http_session":session}

@task
def cleanup_session(http_session :requests.Session) -> dict:
    try:
        http_session.close()
    except Exception:
        #Log?
        pass

@task
def fetch_v2_sold_properties(http_session :requests.Session, **params) -> dict:
    base_url = variable.get('API_URL')
    headers = {
        'x-rapidapi-host' : variable.get('API_HOST'), 
        'x-rapidapi-key' : variable.get('API_KEY') 
    }
    try:
        params["city"]
        params["state_code"]
        params["limit"]
        params["offset"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")
    
    endpoint = "/list-sold"
    try:
        resp = http_session.get(url=base_url+endpoint, headers=headers,
                            params=params)
        resp.raise_for_status()
    except HTTPError as e:
        raise APIExcpetion(str(e))

    return resp.json() #doesnt handle JSON error

@task
def fetch_v2_for_rent_properties(http_session :requests.Session, **params) \
        -> dict:
    base_url = variable.get('API_URL')
    headers = {
        'x-rapidapi-host' : variable.get('API_HOST'), 
        'x-rapidapi-key' : variable.get('API_KEY') 
    }
    try:
        params["city"]
        params["state_code"]
        params["limit"]
        params["offset"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")

    endpoint = "/list-for-rent"
    try:
        resp = http_session.get(url=base_url+endpoint, headers=headers,
                            params=params)
        resp.raise_for_status()
    except HTTPError as e:
        raise APIExcpetion(str(e))
        
    return resp.json()

@task
def fetch_v2_for_sale_properties(http_session :requests.Session, **params) \
        -> dict:
    base_url = variable.get('API_URL')
    headers = {
        'x-rapidapi-host' : variable.get('API_HOST'), 
        'x-rapidapi-key' : variable.get('API_KEY') 
    }
    try:
        params["city"]
        params["state_code"]
        params["limit"]
        params["offset"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")

    endpoint = "/list-for-sale"
    try:
        resp = http_session.get(url=base_url+endpoint, headers=headers,
                            params=params)
        resp.raise_for_status()
    except HTTPError as e:
        raise APIExcpetion(str(e))
            
    return resp.json()

@task
def fetch_v2_mls_properties(http_session :requests.Session, **params) -> dict:
    base_url = variable.get('API_URL')
    headers = {
        'x-rapidapi-host' : variable.get('API_HOST'), 
        'x-rapidapi-key' : variable.get('API_KEY') 
    }
    try:
        params["mls_id"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")

    endpoint = "/list-by-mls"
    try:
        resp = http_session.get(url=base_url+endpoint, headers=headers,
                            params=params)
        resp.raise_for_status()
    except HTTPError as e:
        raise APIExcpetion(str(e))

    return resp.json()

@task
def fetch_v2_property_details(http_session :requests.Session, **params) \
        -> dict:
    base_url = variable.get('API_URL')
    headers = {
        'x-rapidapi-host' : variable.get('API_HOST'), 
        'x-rapidapi-key' : variable.get('API_KEY') 
    }
    try:
        params["property_id"]
    except KeyError as e:
        raise APIExcpetion(f"{e} is a required param")

    endpoint = "/detail"
    try:
        resp = http_session.get(url=base_url+endpoint, headers=headers,
                            params=params)
        resp.raise_for_status()
    except HTTPError as e:
        raise APIExcpetion(str(e))

    return resp.json()