import requests
import urllib.parse
from database.services import add_data_on_sales, get_data
import json
import concurrent.futures
import datetime


def fetch_data_on_sale(data_on_sales: dict, key, hash_chunk):
    tail = ''
    for block in hash_chunk:
        tail += '&list_hash_name[]=' + urllib.parse.quote(block[0])
    url = f"https://market.csgo.com/api/v2/search-list-items-by-hash-name-all/?key={key}{tail}"

    try:
        response = requests.get(url, timeout=120)
        data = response.json()

    except json.decoder.JSONDecodeError:
        pass
    except Exception as ex:
        print(f"Error with resource available - ConnectTimeOut. Skip {len(hash_chunk)} chunks in ON_SALE stage")

    finally:
        if response.status_code in range(100, 400):
            for name, items in data['data'].items():
                for item in items:
                    try:
                        data_on_sales.update({str(item['id']): {"market_hash_name": name,
                                                                "price": str(int(item['price']) / 100),
                                                                "asset": str(
                                                                    item['extra'][
                                                                        'asset']) if 'extra' in item and 'asset' in
                                                                                     item['extra'] else None,
                                                                "class_id": str(item['class']),
                                                                "instance_id": str(item['instance']),
                                                                "status": "on_sale",
                                                                "time": datetime.datetime.now()}})
                    except Exception as e:
                        print(f"Error while inserting data into added to general ON_SALE list: {e}")


def main_on_sale(hash_chunks: list, data_on_sales: dict):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for index, hash_chunk in enumerate(hash_chunks):
            key = hash_chunk[0]
            hash_chunk = hash_chunk[1]
            executor.submit(fetch_data_on_sale, data_on_sales=data_on_sales, key=key,
                            hash_chunk=hash_chunk)

    print(f"Amount elements on all iterations: {len(data_on_sales)}")

    add_data_on_sales(table="on_sale", data=data_on_sales)
    print(f"Now ON_SALE DB is storing: {len(get_data(table='on_sale', db=1))} records")
    print("--------------------------------------------")
