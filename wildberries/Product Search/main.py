import json
import urllib3
import psycopg2
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple
from datetime import timedelta
import time 
import json
import pandas as pd
import logging
import psycopg2
from typing import List, Dict, Any
import pytz
import os


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

urllib3.disable_warnings()

moscow_tz = pytz.timezone('Europe/Moscow')

def load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, 'r', encoding='utf-8') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        logging.error(f"Конфигурационный файл не найден: {path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Ошибка парсинга конфигурационного файла {path}: {e}")
        raise

def gen_unickue_sku(db_config):
    query = f"SELECT DISTINCT sku FROM wildberries.wildberries_sku;"
    try:
        # Устанавливаем соединение с базой данных
        with psycopg2.connect(**db_config) as connection:
            with connection.cursor() as cursor:
                # Выполняем SQL-запрос
                cursor.execute(query)
                # Получаем результат запроса
                results = cursor.fetchall()

                # Возвращаем вложенный список уникальных значений
                original_list = [int(row[0]) for row in results]
                max_length = 50
                sub_lists = split_into_equal_parts(original_list, max_length)
                return sub_lists

    except psycopg2.Error as e:
        logging.error(f"Ошибка при выполнении запроса: {e}")
        return []

import math

def split_into_equal_parts(original_list, max_length):

    # Вычисляем количество частей
    total_length = len(original_list)
    num_parts = math.ceil(total_length / max_length)

    # Расчёт длины каждой части для равномерного распределения
    part_size = math.ceil(total_length / num_parts)

    # Разбиение списка
    sub_lists = [original_list[i:i + part_size] for i in range(0, total_length, part_size)]

    return sub_lists


def extract_and_transform_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []

    for item in data:
        transformed_item = {
            "sku": item.get("nmId"),
            "keyword": item.get("text"),
            "date": (datetime.now(moscow_tz) - timedelta(days=1)).strftime('%Y-%m-%d'),
            "frequency": item.get("frequency", {}).get("current", 0),
            "rank": item.get("avgPosition", {}).get("current", 0),
            "clicks_from_search": item.get("openCard", {}).get("current", 0),
            "baskets_from_search": item.get("addToCart", {}).get("current", 0),
            "orders_from_search": item.get("orders", {}).get("current", 0),
            "visibility_in_search": item.get("visibility", {}).get("current", 0)
        }
        result.append(transformed_item)
    return result


def insert_data_into_db(data: List[Dict[str, Any]], db_config: Dict[str, Any]) -> None:

    query = """
    INSERT INTO wildberries.wildberries_product_queries
    (sku, keyword, day, frequency, rank, clicks, baskets, orders, visibility_in_search) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (sku, keyword, day) DO UPDATE
    SET frequency = EXCLUDED.frequency,
        rank = EXCLUDED.rank,
        clicks = EXCLUDED.clicks,
        baskets = EXCLUDED.baskets,
        orders = EXCLUDED.orders,
        visibility_in_search = EXCLUDED.visibility_in_search;"""

    try:
        # Преобразование данных в кортежи
        records = [
            (
                item.get("sku"),  # sku
                item.get("keyword"),  # keyword
                item.get("date"),  # day
                item.get("frequency"),  # frequency (должно быть примитивным типом, например int)
                item.get("rank"),  # rank
                item.get("clicks_from_search"),  # clicks
                item.get("baskets_from_search"),  # baskets
                item.get("orders_from_search", None),  # orders, с дефолтным значением None
                item.get("visibility_in_search", None)  # visibility_in_search, с дефолтным значением None
            )
            for item in data
        ]
        # Вставка данных
        with psycopg2.connect(**db_config) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, records)
            connection.commit()
        logging.info(f"Успешно вставлено {len(records)} записей.")
    except Exception as e:
        logging.error(f"Ошибка при вставке данных: {e}")
        raise


def fetch_api_data(url: str, headers: Dict[str, str], lst_sky, db_config) -> List[Dict[str, Any]]:
    http = urllib3.PoolManager()
    today = datetime.now(moscow_tz)

    current_period_duration = 1  # Длительность текущего периода 
    date_from = (today - timedelta(days=current_period_duration)).strftime('%Y-%m-%d')
    date_to = today.strftime('%Y-%m-%d')

    # Период для сравнения: на 1 день длиннее текущего периода
    past_period_duration = current_period_duration + 1
    past_start = (today - timedelta(days=current_period_duration + past_period_duration)).strftime('%Y-%m-%d')
    past_end = (today - timedelta(days=current_period_duration + 1)).strftime('%Y-%m-%d')

    for i in lst_sky:
        # Формируем тело запроса
        body = {
            "currentPeriod": {
                "start": date_from,
                "end": date_to
            },
            "pastPeriod": {
                "start": past_start,
                "end": past_end
            },
            "nmIds": i,  # Пример артикулов WB для фильтрации
            "topOrderBy": "openToCart",
            "orderBy": {
                "field": "avgPosition",  # Поле для сортировки
                "mode": "asc"           # Режим сортировки: "asc" или "desc"
            },
            "limit": 30,  # Количество групп товаров в ответе (максимум 1000)
        }

        # Логируем тело запроса
        logging.info(f"Сформированное тело запроса")

        max_attempts = 3
        attempt = 1
        success = False

        while attempt <= max_attempts:
            try:
                response = http.request('POST', url, headers=headers, body=json.dumps(body).encode('utf-8'))

                if response.status == 200:
                    data = json.loads(response.data.decode('utf-8'))
                    products = data["data"]["items"]
                    update = extract_and_transform_data(products)
                    insert_data_into_db(update, db_config)
                    success = True
                    time.sleep(30)
                    break  # выход из цикла while, данные обработаны успешно

                elif response.status == 504:
                    logging.warning(f"[{i}] API вернул 504 Gateway Timeout. Повтор через 60 сек... (попытка {attempt}/{max_attempts})")
                    time.sleep(60)
                    attempt += 1

                else:
                    raise ValueError(f"API вернул статус {response.status}")

            except json.JSONDecodeError as e:
                logging.error(f"[{i}] Ошибка декодирования JSON на попытке {attempt}: {e}")
                time.sleep(10)
                attempt += 1

            except Exception as e:
                logging.error(f"[{i}] Неожиданная ошибка при запросе на попытке {attempt}: {e}")
                time.sleep(10)
                attempt += 1

        if not success:
            logging.error(f"[{i}] Не удалось получить данные от API после {max_attempts} попыток. Пропуск SKU-группы.")


def f_wb_product_queries():
    try:
        # Загрузка конфигурационных файлов
        env_path = os.getenv("AIRFLOW_ENV_PATH", default="/home/ritter/airflow-monitoring-and-alerting")
        wb_config_path = f'{env_path}/files/config/wb_token.json'
        db_config_path = f'{env_path}/files/config/ritter_db.json'

        wb_config = load_config(wb_config_path)
        db_config = load_config(db_config_path)
        logging.info("Конфигурационные файлы успешно загружены.")

        # Запрос данных из API
        api_url = f"https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts"
        headers = {
            "Content-Type": "application/json",
            'Authorization': wb_config['WB Statistics and Analytics']
            }

        lst_sky = gen_unickue_sku(db_config)
        records = fetch_api_data(api_url, headers, lst_sky, db_config)

    except Exception as e:
        logging.error(f"Общая ошибка: {e}")
        raise
    
    finally:
        logging.info("Скрипт завершён.")


if __name__ == "__main__":
    f_wb_product_queries()
