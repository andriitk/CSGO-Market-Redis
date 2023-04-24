import datetime
from database.redis_con import get_data, add_data_to_redis
import logging

print("\n\n-------------------4 STAGE---------------------")
on_sale_data = get_data(table="on_sale", db=1)
history_data = get_data(table="history", db=2)
fourth_stage = get_data(table="fourth_stage", db=3)

dict_on_sale = {(value['market_hash_name'], value['price'], value['status']): {"market_id": key,
                                                                               'asset': value['asset'],
                                                                               'class_id': value['class_id'],
                                                                               'instance_id': value['instance_id']} for
                key, value in on_sale_data.items()}
dict_history = {(key, i, value['status']): {"market_hash_name": key} for key, value in history_data.items() for i in
                value['price'][0]}

list_of_sales = [i for i in dict_history.keys() if i in dict_on_sale.keys()]

count = 0
for i in list_of_sales:
    if dict_on_sale[i]['market_id'] not in fourth_stage.keys():
        fourth_stage[dict_on_sale[i]['market_id']] = {
            'market_hash_name': i[0],
            'price': i[1],
            'status': 'found',
            'asset': dict_on_sale[i]['asset'],
            'class_id': dict_on_sale[i]['class_id'],
            'instance_id': dict_on_sale[i]['instance_id'],
            'created_at': datetime.datetime.now()
        }
        count += 1
        logging.info(f"[+] Found match element with name: '{i[0]}'; price: {i[1]}.")
        print(f"[+] Found match element with name: '{i[0]}'; price: {i[1]}.")

if count:
    add_data_to_redis(table="fourth_stage", data=fourth_stage, db=3)
    logging.info(f"Found {count} elements will be on sales. Relevant column added to History table.")
    print(f"\n[!] Found {count} elements will be on sales. Relevant column added to History table.\n")
else:
    logging.info("Not found matching elements. Stage 4 finished without changes.")
    print("[-] Not found matching elements. Stage 4 finished without changes.\n")
