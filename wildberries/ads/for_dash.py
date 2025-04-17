#!/usr/bin/python3
import json
import urllib3
import os
from typing import Any, Dict, Optional, List, Tuple 
import json
import urllib3
import psycopg2
import time
import pandas as pd
from datetime import date, datetime, timedelta
import pytz


# Отключение предупреждений от urllib3
urllib3.disable_warnings()
http = urllib3.PoolManager()

# Определение пути к конфигурационным файлам
ENV_PATH: str = os.getenv("AIRFLOW_ENV_PATH", default="/home/ritter/airflow-monitoring-and-alerting")


class LoadConfig:
    def load_config(self, path: str) -> Dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8") as config_file:
                return json.load(config_file)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Конфигурационный файл не найден: {path}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка декодирования JSON в файле {path}: {e}") from e


class GetStatistic(LoadConfig):

    def __init__(self, API_url: str, body: str, method: str) -> None:
        self.wb_config_path: str = f"{ENV_PATH}/files/config/wb_token.json"
        
        self.wb_config: Dict[str, Any] = self.load_config(self.wb_config_path)

        self.url: str = API_url
        self.body: bytes = body.encode("utf-8")
        self.headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Authorization": self.wb_config["WB Ads Info"],
        }
        self.method: str = method

    def make_request(self) -> Optional[Dict[str, Any]]:
        response = http.request(self.method, self.url, body=self.body, headers=self.headers)

        try:
            response_json: Dict[str, Any] = json.loads(response.data.decode("utf-8"))
            if response.status == 200:
                return response_json
            return None

        except json.JSONDecodeError:
            print(f"Ошибка декодирования JSON: {response.data.decode('utf-8')}")
            raise


class GetID(LoadConfig):

    def __init__(self, API_url: str, method: str) -> None:

        self.wb_config_path: str = f"{ENV_PATH}/files/config/wb_token.json"
        self.wb_config: Dict[str, Any] = self.load_config(self.wb_config_path)

        self.url: str = API_url
        self.headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Authorization": self.wb_config["WB Ads Info"],
        }
        self.method: str = method


    def process_report(self, json_stats: Optional[List[Dict[str, Any]]]) -> Optional[List[int]]:
        
        if not json_stats:
            return None  

        data: List[int] = []
        for campaign in json_stats:
            try:
                advert_id = campaign.get("advertId")
                if advert_id is not None:
                    data.append(advert_id)
            except Exception as e:
                print(f"Ошибка обработки кампании: {e} -> {campaign}")

        return data


    def make_request(self, url_campaigns: str) -> Optional[List[Dict[str, Any]]]:

        response = http.request(method=self.method, url=url_campaigns, headers=self.headers)
        try:
            response_json: List[Dict[str, Any]] = json.loads(response.data.decode("utf-8"))
            if response.status == 200:
                return response_json
            return None
        except json.JSONDecodeError:
            print(f"Ошибка декодирования JSON: {response.data.decode('utf-8')}")
            return None

    def activate(self) -> Optional[List[int]]:
 
        url_campaigns = f"{self.url}?status=9"
        json_campaigns = self.make_request(url_campaigns)
        return self.process_report(json_campaigns)


class UpdateDB(LoadConfig):

    def __init__(self, data_for_DB: List[Tuple[str, int]]):
        self.db_config_path: str = f"{ENV_PATH}/files/config/ritter_db.json"
        self.db_config: Dict[str, Any] = self.load_config(self.db_config_path)

        self.data_for_DB: List[Tuple[str, int]] = data_for_DB
        
        self.adjust_new_data()

    def get_old_data(self, day: str) -> pd.DataFrame:

        try:
            with psycopg2.connect(**self.db_config) as connection:
                query = f"""
                    SELECT campaign_id, 
                        SUM(views) AS views, 
                        SUM(clicks) AS clicks, 
                        SUM(spent) AS spent, 
                        SUM(baskets) AS baskets, 
                        SUM(orders) AS orders, 
                        SUM(revenue) AS revenue
                    FROM wildberries.wildberries_ads_for_dash 
                    WHERE day = '{day}'
                    GROUP BY campaign_id;
                """
                df_existing = pd.read_sql(query, connection)
                return df_existing
        except Exception as e:
            raise Exception(f"Ошибка при получении данных из БД: {e}")


    def adjust_new_data(self) -> List[Tuple]:
 
        cur_day = self.data_for_DB[0][0]
        df_existing = self.get_old_data(cur_day)
        
        if not df_existing.empty:
            existing_dict = df_existing.set_index("campaign_id").to_dict(orient="index")
            adjusted_data = []
            for row in self.data_for_DB:
                campaing_id = row[4]  # ID кампании

                if campaing_id in existing_dict:
                    existing_values = existing_dict[campaing_id]
                    # Вычитание существующих значений
                    adjusted_row = (
                        row[0],  # day
                        row[1],  # api_received_at
                        row[2] - existing_values.get("views"),  
                        row[3] - existing_values.get("clicks"),  
                        row[4],  # campaing_id (не изменяется)
                        row[5] - existing_values.get("spent"),  
                        row[6] - existing_values.get("baskets"),  
                        row[7] - existing_values.get("orders"),  
                        row[8] - existing_values.get("revenue")
                    )
                else:
                    adjusted_row = row  # Если данных нет, вставляем как есть
                adjusted_data.append(adjusted_row)

            self.update_db(adjusted_data)

        else:
            self.update_db(self.data_for_DB)

    def update_db(self, data) -> None:

        with psycopg2.connect(**self.db_config) as connection:
            with connection.cursor() as cursor:
                query = """
                INSERT INTO wildberries.wildberries_ads_for_dash (
                    day, api_received_at, views, clicks, campaign_id, spent, baskets, orders, revenue)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (api_received_at, campaign_id) DO NOTHING;
                """
                cursor.executemany(query, data)
                connection.commit()
                print("Данные успешно записаны в базу данных.")


def main() -> Optional[List[Dict[str, Any]]]:

    result_get_id = GetID(
        API_url="https://advert-api.wb.ru/adv/v1/promotion/adverts",
        method="POST"
    )

    current_id: List[int] = result_get_id.activate()

    moscow_tz = pytz.timezone('Europe/Moscow')
    current_date: str = datetime.now(moscow_tz).strftime("%Y-%m-%d")
    lst_data: List[Dict[str, Any]] = [{"id": campaign_id, "dates": [current_date, current_date]} for campaign_id in current_id]

    result_get_statistic = GetStatistic(
        API_url="https://advert-api.wb.ru/adv/v2/fullstats",
        body=json.dumps(lst_data),
        method="POST"
    )

    req_stat = result_get_statistic.make_request()
    result_data: List[Tuple[str, int, int, int, int, float, float, float, int, float, int, float]] = []

    if req_stat:
        api_received_at = datetime.now(moscow_tz).strftime("%Y-%m-%d %H:%M:%S")
        for elem in req_stat:
            data_tuple = (
                elem.get("dates")[0],
                api_received_at,
                elem.get("views", 0),
                elem.get("clicks", 0),
                elem.get("advertId", 0),
                elem.get("sum", 0.0),
                elem.get("atbs", 0),
                elem.get("shks", 0),
                elem.get("sum_price", 0.0)
            )
            result_data.append(data_tuple)

    db_object = UpdateDB(result_data)


if __name__ == "__main__":
    response = main()


