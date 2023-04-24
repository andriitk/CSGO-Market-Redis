from database.redis_con import add_data_to_redis, get_data
import datetime
import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', filename='market_hash_names.log',
    filemode='a')


# Func for insert of add market hash names ro redis DB
def insert_market_hash_names():
    response = requests.get(
        "https://api.steamapis.com/market/items/730?api_key=r39OfExGTYD64Bf8MGntG8oaJgQ&format=comact", timeout=30)
    data = response.json()

    market_hash_names = get_data(table='market_hash_names', db=0)

    if not market_hash_names:
        market_hash_names = {"created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                             'market_hash_names': {}}
        for key, value in data.items():
            market_hash_names['market_hash_names'][key] = str(value)
        add_data_to_redis(table='market_hash_names', data=market_hash_names, db=0)
    else:
        for key, value in data.items():
            market_hash_names['market_hash_names'][key] = value

    return market_hash_names


# Func for insert data to ON_SALE table according to logic
def add_data_on_sales(data: dict, table="on_sales", db=1):
    data_on_sales = get_data(table=table, db=db)

    if not data_on_sales:
        add_data_to_redis(table=table, data=data, db=db)
    else:
        diff_keys = set(data_on_sales.keys()) - set(data.keys())

        not_founds = 0
        need_checks = 0

        for i in diff_keys:
            time_db = data_on_sales[i]['created_at'].timestamp()
            time_now = datetime.datetime.now().timestamp()

            if time_now - time_db > 3600:
                data_on_sales[i]['status'] = "not_found"
                not_founds += 1
            else:
                data_on_sales[i]['status'] = "need_check"
                need_checks += 1

        if not_founds:
            logging.info(f"{not_founds} elements got 'not_found' status in ON_SALE DB.")
            print(f"{not_founds} elements got 'not_found' status in ON_SALE DB\n")
        if need_checks:
            logging.info(f"{need_checks} elements got 'need_check' status in ON_SALE DB.")
            print(f"{need_checks} elements got 'need_check' status in ON_SALE DB\n")

        for key, value in data.items():
            on_sale_status = 0

            if key not in data_on_sales:
                data_on_sales[key] = value
                logging.info(f"Insert new record to ON_SALE DB '{value['market_hash_name']} : {key}'.")
            else:
                if data_on_sales[key]['status'] == 'not_found':
                    data_on_sales[key]['status'] = 'on_sale'
                    on_sale_status += 1

                data_on_sales[key]['time'] = datetime.datetime.now()
                data_on_sales[key]['asset'] = value['asset']
                data_on_sales[key]['price'] = value['price']
                logging.info(f"Update existing record in the ON_SALE DB with market_id '{key}'.")

            if on_sale_status:
                logging.info(f"{on_sale_status} elements got 'on_sale' status in ON_SALE DB.")

        add_data_to_redis(table=table, data=data_on_sales, db=db)


def add_data_history(data: dict, table="history", db=2):
    data_history = get_data(table=table, db=db)

    if not data_history:
        add_data_to_redis(table=table, data=data, db=db)
    else:
        diff_keys = set(data_history.keys()) - set(data.keys())

        not_founds = 0
        for i in diff_keys:
            time_db = data_history[i]['created_at'].timestamp()
            time_now = datetime.datetime.now().timestamp()

            if time_now - time_db > 3600:
                data_history[i]['status'] = "not_found"
                not_founds += 1

        if not_founds:
            logging.info(f"{not_founds} elements got 'not_found' status in HISTORY DB.")
            print(f"{not_founds} elements got 'not_found' status in HISTORY DB\n")

        for key, value in data.items():
            if key not in data_history:
                data_history[key] = value
                logging.info(f"Insert new record to HISTORY DB '{key}'.")
            else:
                prices = data_history[key]['price'][0]
                times = data_history[key]['time']

                data_history[key]['price'] = list(set(prices) | set(value['price'])),
                data_history[key]['time'] = list(set(times) | set(value['time']))

                logging.info(f"Update existing record in the HISTORY DB with name '{key}'.")

        add_data_to_redis(table=table, data=data_history, db=db)
