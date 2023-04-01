import requests
import urllib.parse
from database.services import add_data_history, get_data
# import hashlib
import concurrent.futures
import datetime
import json


# hash_object = hashlib.sha256()


def fetch_data_history(data_history: dict, key, hash_chunk):
    tail = ''
    for block in hash_chunk:
        tail += '&list_hash_name[]=' + urllib.parse.quote(block[0])
    url = f"https://market.csgo.com/api/v2/get-list-items-info?key={key}{tail}"

    try:
        response = requests.get(url, timeout=120)
        data = response.json()

    except json.decoder.JSONDecodeError:
        pass
    except Exception as ex:
        print(f"Error with resource available - ConnectTimeOut. Skip {len(hash_chunk)} chunks in HISTORY stage")

    finally:
        if response.status_code in range(100, 400):
            for name, items in data['data'].items():
                try:
                    prices = []
                    for block in items['history']:
                        if "." not in str(block[1]):
                            prices.append(str(block[1]) + ".0")
                        else:
                            prices.append(str(block[1]))

                    # hash_object.update(json.dumps({name: items['history']}).encode())
                    data_history.update({json.dumps({name: items['history']}): {"market_hash_name": name,
                                                                                "times": [
                                                                                    str(datetime.datetime.fromtimestamp(
                                                                                        int(block[0])))
                                                                                    for block in
                                                                                    items['history']],
                                                                                "price": prices,
                                                                                "time": datetime.datetime.now(),
                                                                                "status": "need_check"
                                                                                }})
                except Exception as e:
                    print(f"Error while inserting data into added to general HISTORY list: {e}")


def main_history(hash_chunks: list, data_history: dict):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for index, hash_chunk in enumerate(hash_chunks):
            key = hash_chunk[0]
            hash_chunk = hash_chunk[1]

            executor.submit(fetch_data_history, data_history=data_history, key=key,
                            hash_chunk=hash_chunk)

    print(f"Amount elements on all iterations: {len(data_history)}")

    add_data_history(table="history", data=data_history)
    print(f"Now HISTORY DB is storing: {len(get_data(table='history', db=2))} records")
