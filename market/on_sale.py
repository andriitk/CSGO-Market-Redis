# import requests
import urllib.parse
from database.services import add_data_on_sales, get_data
# import concurrent.futures
import datetime
import logging
import asyncio
import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', filename='market_hash_names.log',
    filemode='a')


async def fetch_data_on_sale(data_on_sales: dict, key, hash_chunk, session):
    tail = ''
    for block in hash_chunk:
        tail += '&list_hash_name[]=' + urllib.parse.quote(block[0])
    url = f"https://market.csgo.com/api/v2/search-list-items-by-hash-name-all/?key={key}{tail}"

    try:
        async with session.get(url) as response:
            status_code = response.status
            data = await response.json()

            for name, items in data['data'].items():
                for item in items:
                    data_on_sales[str(item['id'])] = {"market_hash_name": name,
                                                      "asset": str(item['extra'][
                                                                       'asset']) if 'extra' in item and 'asset' in
                                                                                    item[
                                                                                        'extra'] else None,
                                                      "class_id": str(item['class']),
                                                      "instance_id": str(item['instance']),
                                                      "price": str(int(item['price']) / 100),
                                                      "status": "on_sale",
                                                      "created_at": datetime.datetime.now()}

    except aiohttp.client_exceptions.ContentTypeError as ex:
        print(f"[!] Exception - ON_SALE: ConnectError. Status code: {status_code}")
        logging.error(
            f"[!] Exception - ON_SALE - ConnectError. "
            f"Skip {len(hash_chunk)} chunks in ON_SALE stage. Status code: {status_code}")

    except Exception as ex:
        print(f"[!] Exception - ON_SALE: {ex}. Status code: {status_code}")
        logging.error(
            f"[!] Exception - ON_SALE - {ex}. "
            f"Skip {len(hash_chunk)} chunks in ON_SALE stage. Status code: {status_code}")


# ASYNCIO
async def main_on_sale(hash_chunks: list, data_on_sales: dict):
    print(asyncio.get_event_loop())
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(
            fetch_data_on_sale(data_on_sales=data_on_sales, key=hash_chunk[0], hash_chunk=hash_chunk[1],
                               session=session)) for index, hash_chunk in enumerate(hash_chunks)]

        await asyncio.gather(*tasks)

    logging.info("Start inserted data to ON_SALE DB")
    print("Start inserted data to ON_SALE DB")

    add_data_on_sales(table="on_sale", data=data_on_sales)

    logging.info(f"Amount elements on all iterations in ON_SALE stage: {len(data_on_sales)}")
    logging.info(f"Now ON_SALE DB is storing: {len(get_data(table='on_sale', db=1))} records")

    print(f"Amount elements on all iterations in ON_SALE stage: {len(data_on_sales)}")
    print(f"Now ON_SALE DB is storing: {len(get_data(table='on_sale', db=1))} records")

# THREADING
# def main_on_sale(hash_chunks: list, data_on_sales: dict):
#     with concurrent.futures.ThreadPoolExecutor(max_workers=110) as executor:
#         for index, hash_chunk in enumerate(hash_chunks):
#             key = hash_chunk[0]
#             hash_chunk = hash_chunk[1]
#             executor.submit(fetch_data_on_sale, data_on_sales=data_on_sales, key=key,
#                             hash_chunk=hash_chunk)
#
#             logging.info(
#                 f"Submitting chunk {index} with key {key} and {len(hash_chunk)} hash names to executor ON_SALE")
#
#     logging.info("Start inserted data to ON_SALE DB")
#     print("Start inserted data to ON_SALE DB")
#
#     add_data_on_sales(table="on_sale", data=data_on_sales)
#
#     logging.info(f"Amount elements on all iterations in ON_SALE stage: {len(data_on_sales)}")
#     logging.info(f"Now ON_SALE DB is storing: {len(get_data(table='on_sale', db=1))} records")
#
#     print(f"Amount elements on all iterations in ON_SALE stage: {len(data_on_sales)}")
#     print(f"Now ON_SALE DB is storing: {len(get_data(table='on_sale', db=1))} records")
