from database.services import get_data, add_data_to_redis

print("-------------------4 STAGE-----------------")
on_sale_data = get_data(table="on_sale", db=1)
history_data = get_data(table="history", db=2)

dict_on_sale = {}
dict_history = {}

for key, value in on_sale_data.items():
    market_data = {"market_id": key, 'asset': value['asset'], 'class_id': value['class_id'],
                   'instance_id': value['instance_id']}
    dict_on_sale[(value['market_hash_name'], value['price'], value['status'])] = market_data

for key, value in history_data.items():
    hash_key = {"hash_key": key}
    for i in value['price']:
        dict_history[(value['market_hash_name'], i, value['status'])] = hash_key

list_of_sales = [i for i in dict_history.keys() if i in dict_on_sale.keys()]

count = 0
for i in list_of_sales:
    on_sale_data[dict_on_sale[i]['market_id']]['status'] = "found"
    history_data[dict_history[i]['hash_key']]['status'] = "found"
    history_data[dict_history[i]['hash_key']]['market_id'] = dict_on_sale[i]['market_id']
    history_data[dict_history[i]['hash_key']]['asset'] = on_sale_data[dict_on_sale[i]['market_id']]['asset']
    history_data[dict_history[i]['hash_key']]['class_id'] = on_sale_data[dict_on_sale[i]['market_id']]['class_id']
    history_data[dict_history[i]['hash_key']]['instance_id'] = on_sale_data[dict_on_sale[i]['market_id']]['instance_id']

    count += 1

    if count:
        print(f"[+] Search {count} will be on sales.")
        add_data_to_redis(table="history", data=history_data, db=2)
        add_data_to_redis(table="on_sale", data=on_sale_data, db=1)
    else:
        print("[-] Stage 4 finished without changes.")
