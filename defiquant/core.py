import time
import pandas as pd
import json
import requests
from requests.exceptions import RequestException
from dune_client.client import DuneClient

from defiquant.env import (COINGECKO_API_KEY, VAULTSFYI_KEY, FLIPSIDE_API_KEY, FRED_API_KEY, DUNE_API_KEY)
from defiquant.utils import api_cache

@api_cache
def flipside_api_results(query=None, query_run_id=None, attempts=10, delay=30):
    if not FLIPSIDE_API_KEY:
        raise ValueError("FLIPSIDE_API_KEY is missing or empty.")
    if query is None and query_run_id is None:
        raise ValueError("You must provide either a SQL query or an existing query_run_id.")

    url = "https://api-v2.flipsidecrypto.xyz/json-rpc"
    headers = {"Content-Type": "application/json", "x-api-key": FLIPSIDE_API_KEY}

    if query_run_id is None:
        payload = {
            "jsonrpc": "2.0",
            "method": "createQueryRun",
            "params": [{"resultTTLHours": 1, "maxAgeMinutes": 0, "sql": query,
                        "tags": {"source": "python-script", "env": "production"},
                        "dataSource": "snowflake-default", "dataProvider": "flipside"}],
            "id": 1
        }
        response = requests.post(url, headers=headers, json=payload)
        response_data = response.json()
        if 'error' in response_data:
            raise Exception(f"Error creating query: {response_data['error']['message']}")
        query_run_id = response_data.get('result', {}).get('queryRun', {}).get('id')
        if not query_run_id:
            raise KeyError(f"Query creation failed. Response: {response_data}")

    for attempt in range(attempts):
        status_payload = {
            "jsonrpc": "2.0",
            "method": "getQueryRunResults",
            "params": [{"queryRunId": query_run_id, "format": "json", "page": {"number": 1, "size": 10000}}],
            "id": 1
        }
        response = requests.post(url, headers=headers, json=status_payload)
        resp_json = response.json()

        if 'result' in resp_json and 'rows' in resp_json['result']:
            all_rows, page_number = [], 1
            while True:
                status_payload["params"][0]["page"]["number"] = page_number
                response = requests.post(url, headers=headers, json=status_payload)
                resp_json = response.json()
                rows = resp_json.get('result', {}).get('rows', [])
                if not rows:
                    break
                all_rows.extend(rows)
                page_number += 1
            return pd.DataFrame(all_rows)

        if 'error' in resp_json and 'not yet completed' in resp_json['error'].get('message', '').lower():
            time.sleep(delay)
        else:
            raise Exception(f"Error fetching query results: {resp_json}")

    raise TimeoutError(f"Query did not complete after {attempts} attempts.")

@api_cache
def defillama_pool_data():
    try:
        response = requests.get('https://yields.llama.fi/pools', timeout=10)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data.get('data', []))
    except Exception as e:
        print(f"❌ Error fetching DefiLlama pool data: {e}")
        return pd.DataFrame()

@api_cache
def defillama_pool_yield(pools):
    base_url = 'https://yields.llama.fi/chart/'
    all_data = []
    for pool in pools:
        url = f'{base_url}{pool}'
        retries = 0
        while retries < 5:
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                for entry in data.get('data', []):
                    entry['pool'] = pool
                    all_data.append(entry)
                print(f"Data retrieved successfully for pool: {pool}")
                time.sleep(2)
                break
            except RequestException as e:
                retries += 1
                wait_time = 2 ** retries
                print(f"Error: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
    df = pd.DataFrame(all_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@api_cache
def get_token_price(network='arbitrum-one', token='0x5979d7b546e38e414f7e9822514be443a4800529', quote_currency='usd'):
    if not COINGECKO_API_KEY:
        raise ValueError("COINGECKO_API_KEY is missing or empty.")
    try:
        url = f"https://api.coingecko.com/api/v3/simple/token_price/{network}?contract_addresses={token}&vs_currencies={quote_currency}"
        headers = {"accept": "application/json", "x-cg-demo-api-key": COINGECKO_API_KEY}
        response = requests.get(url, headers=headers)
        token_data = response.json()
        token_df = pd.DataFrame(token_data)
        return token_df[f'{token}'].values[0]
    except Exception as e:
        print(f'e: {e}')
        return None

@api_cache
def coingecko_prices(start, end, coin, quote_currency='usd'):
    if not COINGECKO_API_KEY:
        raise ValueError("COINGECKO_API_KEY is missing or empty.")
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range?vs_currency={quote_currency}&from={start}&to={end}"
        headers = {"accept": "application/json", "x-cg-demo-api-key": COINGECKO_API_KEY}
        response = requests.get(url, headers=headers)
        data = json.loads(response.text)
        df_prices = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
        df_prices["timestamp"] = pd.to_datetime(df_prices["timestamp"], unit='ms')
        df_prices.set_index('timestamp', inplace=True)
        return df_prices
    except Exception as e:
        print(f'e: {e}')
        return pd.DataFrame()

@api_cache
def coingecko_api(url_type, coins=None, max_retries=5):
    url_type = url_type.upper()
    if not COINGECKO_API_KEY:
        raise ValueError("COINGECKO_API_KEY is missing or empty.")
    if url_type not in ['FDV', 'LIST', 'PLATFORM']:
        raise ValueError(f"Invalid url_type: {url_type}. Must be one of ['FDV', 'LIST', 'PLATFORM'].")

    headers = {"accept": "application/json", "x-cg-demo-api-key": COINGECKO_API_KEY}

    def make_request(request_url, retries=max_retries):
        delay = 2
        for attempt in range(retries):
            response = requests.get(request_url, headers=headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"Rate limit hit. Retrying after {delay} seconds...")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"Error: {response.status_code}, {response.text}")
                return None
        print("Max retries exceeded.")
        return None

    if url_type == 'FDV':
        if coins is None:
            base_url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=250"
        elif isinstance(coins, list):
            coin_ids = ",".join(coins)
            base_url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids={coin_ids}&per_page=250"
        else:
            base_url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category={coins}&per_page=250"

        all_data, page = [], 1
        while True:
            paginated_url = f"{base_url}&page={page}"
            data = make_request(paginated_url)
            if not data:
                break
            all_data.extend(data)
            print(f"Retrieved page {page} with {len(data)} records.")
            page += 1
            time.sleep(2)
        df = pd.DataFrame(all_data)

    elif url_type == 'LIST':
        base_url = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
        data = make_request(base_url)
        if data:
            df = pd.DataFrame(data)

    elif url_type == 'PLATFORM':
        base_url = "https://api.coingecko.com/api/v3/asset_platforms"
        data = make_request(base_url)
        if data:
            df = pd.DataFrame(data)

    return df

@api_cache
def dune_api_results(query_num, csv_path=None):
    if not DUNE_API_KEY:
        raise ValueError("DUNE_API_KEY is missing or empty.")
    dune = DuneClient(DUNE_API_KEY)
    results = dune.get_latest_result(query_num)
    df = pd.DataFrame(results.result.rows)
    if csv_path:
        try:
            df.to_csv(csv_path, index=False)
            print(f"✅ Dune query results saved to: {csv_path}")
        except Exception as e:
            print(f'Unable to save to {csv_path}: {e}')
    return df

@api_cache
def vaultsfyi_pool_yield(address, network, from_dt, to_dt, 
                       base_url='http://api.vaults.fyi/v1/vaults', interval='1day',
                       granularity=1):
    if not VAULTSFYI_KEY:
        raise ValueError("VAULTSFYI_KEY is missing or empty.")
    page, all_data = 0, []
    while True:
        url = (
            f"{base_url}/{network}/{address}/historical-apy"
            f"?interval={interval}&from_timestamp={from_dt}&to_timestamp={to_dt}"
            f"&granularity={granularity}&page={page}"
        )
        response = requests.get(url, headers={"x-api-key": VAULTSFYI_KEY})
        data = response.json()
        print(f"Fetched page {page}: {data}")
        all_data.extend(data.get("data", []))
        next_page = data.get("next_page")
        if next_page:
            page = next_page
        else:
            break
    return all_data

@api_cache
def fetch_fred_data(api_url, start_date=None, end_date=None):
    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY is missing or empty.")
    if start_date and end_date:
        api_url += f"&observation_start={start_date}&observation_end={end_date}"
    api_url_with_key = f"{api_url}&api_key={FRED_API_KEY}"
    try:
        response = requests.get(api_url_with_key)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error fetching FRED data: {e}")
        return {}
    
def parse_fred_data(data, data_key, date_column, value_column):
    """
    Parse FRED API JSON data into a clean DataFrame.
    """
    try:
        df = pd.DataFrame(data[data_key])
        df[date_column] = pd.to_datetime(df[date_column])
        df[value_column] = pd.to_numeric(df[value_column], errors='coerce')
        df.set_index(date_column, inplace=True)
        return df
    except Exception as e:
        print(f"❌ Error parsing FRED data: {e}")
        return pd.DataFrame()

