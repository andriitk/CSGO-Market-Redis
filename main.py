from market.on_sale import main_on_sale
from market.history import main_history
from database.services import insert_market_hash_names
import multiprocessing as mp
import time

START = time.time()

KEYS = ['vCkVaFAV7rrFGOX5xTE6zsu27t36g9N', '4DsygAaKoCRDAs2jX9o3Gcx68gq34oX', 'vXJ7sFz4nsuc7fT81RNqIkrj7y06m14',
        'UAYvjQAX5U7Lu0GjHcovE3f5Yf8lsEA', '00Evwqok5v459W3CsG1uwBnOV7vG7I6', 'yREZeeZN2pZAKnhDKEUi2gtY5xxtDoU',
        'NDJSqIbxf0zCNE9yYSh35iHHxxMFzz7', '1KAEe664V41vorlgd6kG9hx17g5y6F0', 'w522SCV09kFqLyH02RCRK18kbauVhCL',
        'jwqZ69qF3UMfcfRdyj3cBpqT403WZ36']

# Размер пачки хэшей
CHUNK_SIZE = 50

# Создаем список для хранения пачек хэшей и имен подсписков
HASH_CHUNKS = []

# Индекс текущего ключа в списке keys
KEY_INDEX = 0

DATA_ON_SALES = dict()
DATA_HISTORY = dict()

data = insert_market_hash_names()
market_hash_names = data['market_hash_names']

for i in range(0, len(market_hash_names), CHUNK_SIZE):
    key = KEYS[KEY_INDEX]
    hash_chunk = list(market_hash_names.items())[i:i + CHUNK_SIZE]
    HASH_CHUNKS.append((key, hash_chunk))
    KEY_INDEX = (KEY_INDEX + 1) % len(KEYS)


def run_main_on_sale():
    main_on_sale(hash_chunks=HASH_CHUNKS, data_on_sales=DATA_ON_SALES)


def run_main_on_history():
    main_history(hash_chunks=HASH_CHUNKS, data_history=DATA_HISTORY)


if __name__ == '__main__':
    print("-------------------1 STAGE-----------------")
    print(f"From API got {len(market_hash_names)} market_hash_names")
    process_one = mp.Process(target=run_main_on_sale)
    process_two = mp.Process(target=run_main_on_history)

    print("--------------------------------------------")
    process_one.start()
    print("Start process ON_SALE")
    process_two.start()
    print("Start process HISTORY")

    print("-------------------2 and 3 STAGE-----------------")

    process_one.join()
    process_two.join()

    from market import compare

    elapsed_script_time = time.time() - START
    print("--------------------------------------------")
    print("Total time elapsed:", elapsed_script_time, "seconds")
    print("=============================================")
