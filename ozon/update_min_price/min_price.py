#!/usr/bin/python3
import json
import urllib3
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import time
import pytz
import os
import pandas as pd



urllib3.disable_warnings()
moscow_tz = pytz.timezone('Europe/Moscow')

# Load configurations
env_path = os.getenv("AIRFLOW_ENV_PATH", default="/home/ritter/airflow-monitoring-and-alerting")
oz_config_path = f'{env_path}/files/config/ozon_token.json'
db_config_path = f'{env_path}/files/config/ritter_db.json'

with open(oz_config_path) as oz_config_path:
    oz_config = json.load(oz_config_path)    

with open(db_config_path) as db_config_path:
    db_config = json.load(db_config_path)

# Initialize HTTP connection
http = urllib3.PoolManager()
headers = {'client-id': oz_config['client-id'], 'api-key': oz_config['api-key'], 'Content-Type': 'application/json'}

# API method url
prices_url = 'https://api-seller.ozon.ru/v5/product/info/prices'
API_URL = 'https://api-seller.ozon.ru/v1/product/import/prices'

# Main function
def f_ozon_prices_extractor():

    values = []

    json_prices = make_request(method='POST', url=prices_url, body=json.dumps(request_body()), headers=headers)
    cursor_json = json_prices['cursor']
    total = json_prices['total']

    while True:

        for mini_dict in json_prices["items"]:
            values.append(
                (
                mini_dict["price"]["min_price"],
                mini_dict["price"]["price"],
                mini_dict["offer_id"],
                mini_dict["product_id"],
               
                )
            )

        total -=1000
        if total > 0:
            json_prices = make_request(method='POST', url=prices_url, body=json.dumps(request_body(cursor_json)), headers=headers)
            cursor_json = json_prices['cursor']
        else: 
            break

    df = pd.DataFrame(values, columns=['МинЦен', 'Цена', 'offer_id', "product_id"])

    return values, df


def make_request(method, url, body, headers):
    response = http.request(method, url, body=body, headers=headers)
    attempt = 1
    while response.status not in [200, 204] and attempt <= 10:
        print(f'Ошибка при запросе:{response.status}, {response.data.decode("utf-8")}. Повторный запрос через 15 секунд. Попытка {attempt}/10')
        time.sleep(15)
        response = http.request(method, url, body=body, headers=headers)
        attempt += 1
    json_data = json.loads(response.data.decode("utf-8"))
    return json_data


def request_body(cursor_json=None):
    json_body = {
        "filter": {
            "visibility": "ALL",
        },
        "limit": 1000,
    }
    if cursor_json is not None:
        json_body["cursor"] = f"{cursor_json}"

    return json_body


def post_offers(min_price, price, offer_id, product_id):
        payload = {
            "prices": [
                {
                    "min_price": str(min_price),
                    "price": str(price),
                    "offer_id": str(offer_id),
                    "product_id": int(product_id)
                }
            ]
        }

        response_data = make_request('POST', API_URL, body=json.dumps(payload).encode('utf-8'), headers=headers)
        if response_data is None:
            return False, response_data
        print(f"Цена {price} для товара {product_id} обновлена")
        return True


def update_min_price(new_data):
    for elem in new_data:
        min_price, price, offer_id, product_id = elem
        result = post_offers(min_price, price, offer_id, product_id)


def check(df_1, df_2):

    df_2 = df_2.rename(columns={
        "МинЦен": "МинЦенПОСЛЕ", "Цена": "ЦенаПОСЛЕ", "offer_id": "offer_id_ПОСЛЕ"
    })
    
    # df_1 = df_1.drop_duplicates(subset=['offer_id', 'product_id'])
    # df_2 = df_2.drop_duplicates(subset=['offer_id_ПОСЛЕ', 'product_id'])

    merged_df = pd.merge(
            df_1, df_2, 
            left_on=['offer_id', 'product_id'],  # Ключи из первого DataFrame
            right_on=['offer_id_ПОСЛЕ', 'product_id'],  # Ключи из второго DataFrame
            how='left'
        )
    merged_df['Equal_min_price'] = merged_df['МинЦен'] == merged_df['МинЦенПОСЛЕ']
    merged_df['Equal_price'] = merged_df['Цена'] == merged_df['ЦенаПОСЛЕ']

    if merged_df['Equal_min_price'].all() and merged_df['Equal_price'].all():
        print("Все значения True")
    else:
        print("Есть хотя бы одно значение False")
    
    merged_df.to_csv("Сравнение цен.scv", index=False)


if __name__ == '__main__':
    print("Получаем данные о текущих ценах")
    get_result_1, df_1 = f_ozon_prices_extractor()
    print(len(get_result_1), len(df_1))

    print("Обновляем все цены")
    update_min_price(get_result_1)

    print("Получаем СНОВА данные о текущих ценах, которые обновлили")
    get_result_2, df_2 = f_ozon_prices_extractor()
    print(len(get_result_2), len(df_2))

    print("Сравниваем DF и формируем отчет")
    check(df_1, df_2)
    
