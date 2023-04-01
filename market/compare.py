from database.services import get_data, add_data_to_redis

print("-------------------4 STAGE-----------------")
on_sale_data = get_data(table="on_sale", db=1)
history_data = get_data(table="history", db=2)

os_sales_process = []
history_process = []

for key, value in on_sale_data.items():
    market_id = {"market_id": key}
    os_sales_process.append({**value, **market_id})

for key, value in history_data.items():
    hash_key = {"hash_key": key}
    history_process.append({**value, **hash_key})

count = 0
for sale, his in zip(os_sales_process, history_process):
    if sale['market_hash_name'] == his['market_hash_name'] and sale['status'] == his['status'] and sale['price'] in his['price']:
        on_sale_data[sale['market_id']]['status'] = "found"
        history_data[his['hash_key']]['status'] = "found"
        history_data[his['hash_key']]['market_id'] = sale['market_id']
        history_data[his['hash_key']]['asset'] = on_sale_data[sale['market_id']]['asset']
        history_data[his['hash_key']]['class_id'] = on_sale_data[sale['market_id']]['class_id']
        history_data[his['hash_key']]['instance_id'] = on_sale_data[sale['market_id']]['instance_id']

        count += 1

if count:
    print(f"[+] Search {count} will be on sales.")

    add_data_to_redis(table="history", data=history_data, db=2)
    add_data_to_redis(table="on_sale", data=on_sale_data, db=1)
else:
    print("[-] Stage 4 finished without changes.")
