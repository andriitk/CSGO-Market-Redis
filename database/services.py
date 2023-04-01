from database.redis_con import add_data_to_redis, get_data
import datetime
import requests


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


def add_data_on_sales(data: dict, table="on_sales", db=1):
    data_on_sales = get_data(table=table, db=db)

    if not data_on_sales:
        add_data_to_redis(table=table, data=data, db=db)
    else:
        diff_keys = set(data_on_sales.keys()) - set(data.keys())

        not_founds = 0
        need_checks = 0
        for i in diff_keys:
            time_db = data_on_sales[i]['time'].timestamp()
            time_now = datetime.datetime.now().timestamp()

            if time_now - time_db > 3600:
                data_on_sales[i]['status'] = "not_found"
                not_founds += 1
            else:
                data_on_sales[i]['status'] = "need_check"
                need_checks += 1

        if not_founds:
            print(f"Elements got {not_founds} 'not_found' status")
        if need_checks:
            print(f"Elements got {need_checks} 'need_check' status")

        for key, value in data.items():
            if key not in data_on_sales:
                data_on_sales[key] = value
            else:
                if data_on_sales[key]['status'] == 'not_found':
                    data_on_sales[key]['status'] = 'on_sale'

                data_on_sales[key]['time'] = datetime.datetime.now()
                data_on_sales[key]['asset'] = value['asset']
                data_on_sales[key]['price'] = value['price']

        add_data_to_redis(table=table, data=data_on_sales, db=db)


def add_data_history(data: dict, table="history", db=2):
    data_history = get_data(table=table, db=db)

    if not data_history:
        add_data_to_redis(table=table, data=data, db=db)
    else:
        diff_keys = set(data_history.keys()) - set(data.keys())

        not_founds = 0
        for i in diff_keys:
            time_db = data_history[i]['time'].timestamp()
            time_now = datetime.datetime.now().timestamp()

            if time_now - time_db > 3600:
                data_history[i]['status'] = "not_found"
                not_founds += 1
        if not_founds:
            print(f"Elements got {not_founds} 'not_found' status")

        for key, value in data.items():
            if key not in data_history:
                data_history[key] = value
            else:
                prices = data_history[key]['price']
                times = data_history[key]['times']

                data_history[key]['price'] = list(set(prices).symmetric_difference(set(value['price']))) + prices
                data_history[key]['times'] = list(set(times).symmetric_difference(set(value['times']))) + times

        add_data_to_redis(table=table, data=data_history, db=db)