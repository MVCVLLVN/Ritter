import datetime
import urllib3
import json
import re
import time
import psycopg2
import pandas as pd
from playwright.sync_api import sync_playwright
from typing import List, Tuple, Dict, Any
import json
import psycopg2


# Отключаем предупреждения библиотеки urllib3
urllib3.disable_warnings()

db_config_path = r""

with open(db_config_path) as db_config_path:
    db_config = json.load(db_config_path)

new_name = {
    'In Home - официальный магазин производителя': "In Home",
    'REV Ritter GmbH': "Ritter",
    'ЭРА, официальный магазин': "ЭРА",
    'LAMPLANDIA OFFICIAL STORE': "LAMPLANDIA",
    '"GAUSS" ОФИЦИАЛЬНЫЙ МАГАЗИН': "GAUSS",
    'Feron, официальный магазин': "Feron",
    'Центр профессиональной электрики "Электромир"': "Электромир",
    'ООО «АЛИОТ"': "АЛИОТ",
    'MEGALIGHTMARKET официальный онлайн магазин производителя MEGALIGHT RUS': "MEGALIGHTMARKET",
    "ООО «Стул Груп»": 'Стул Груп'
}

my_category = {
    "CAT971434442": "Люстра",
    "CAT504866302": "Спот",
    "CAT970715573": "Фитосветильник",
    "CAT91309": "Лампочка",
    "CAT504866303": "Трековый светильник",
    # "CAT91654": "Прожектор",
    "CAT91653": "Потолочный светильник",
    "CAT91670": "Уличный светильник",
    "CAT94808": "Клемма",
    "CAT970943336": "Розетка с таймером",
    # "CAT970743127": "Садово-парковый светильник",
}

all_data = {
    "CAT17028941": ["CAT504866302", "CAT971434442", "CAT970715573",
                    "CAT504866303", "CAT91653", "CAT91670"],
    "CAT17028609": ["CAT91309"],
    "CAT17028654": ["CAT94808", "CAT970943336"]
 }

user_data_path = r"C:\Users\m.troshin\PycharmProjects\Pp\get_ozon_competition_position\data_user"
url = "https://seller.ozon.ru/app/analytics/what-to-sell/competitive-position"


def update_db(data: List[Tuple]) -> None:

    def load_config(path: str) -> Dict[str, Any]:
        try:
            with open(path, 'r', encoding='utf-8') as config_file:
                return json.load(config_file)
        except Exception as e:
            print(f"Ошибка при загрузке конфигурации базы данных: {e}")
            raise

    # Путь к файлу конфигурации базы данных
    db_config_path: str = r""

    # Загрузка конфигурации базы данных
    db_config: Dict[str, Any] = load_config(db_config_path)

    # SQL-запрос для вставки данных
    query: str = """
    INSERT INTO ozon.ozon_competitive_positionseller (share, revenue, speed, day, seller, category, position)
    VALUES 
        (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (seller, day, category) DO NOTHING;
    """

    print(data)  # Вывод данных для отладки

    try:
        # Установка соединения с базой данных
        with psycopg2.connect(**db_config) as connection:
            with connection.cursor() as cursor:
                # Вставка данных
                cursor.executemany(query, data)
                connection.commit()
                print("Данные в БД вставлены")

    except psycopg2.Error as db_err:
        # Обработка ошибок базы данных
        print(db_err)
        raise


def get_previous_dates():
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime("%d.%m.%Y")
    bd_data = yesterday.strftime("%Y-%m-%d")
    return yesterday_str, bd_data


def transform_row(row, day, category):
    if len(row) != 1:
        row[0] = row[0].split("\n")
        position = row[0][0]
        seller = row[0][1]
        match = re.search(r'([\d\s\xa0]+)₽', row[2])

        if match:
            revenue = int(match.group(1).replace('\xa0', '').replace(' ', ''))

        match = re.search(r'[\d,]+', row[1])
        if match:
            share = float(match.group(0).replace(",", "."))

        match = re.search(r'[\d,]+', row[-1])
        if match:
            speed = float(match.group(0).replace(",", "."))

        if seller in new_name:
            seller = new_name.get(seller)
        seller = seller.upper()

        print("Отработала функция трансвормайции данных", (share, revenue, speed, day, seller, category, position))
        return (share, revenue, speed, day, seller, category, position)


def login_with_saved_session():
    # Укажите директорию с сохраненными данными профиля

    with sync_playwright() as p:
        # Запускаем браузер с сохраненными данными профиля
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_path,
            headless=False,  # Открываем браузер с интерфейсом
            channel="chrome"  # Используем реальный браузер Chrome
        )

        # Берем первую страницу или создаем новую, если страниц нет
        page = browser.pages[0] if browser.pages else browser.new_page()

        # Переходим на нужный сайт

        print(f"Открываю сайт: {url}")
        page.goto(url, wait_until="load")

        transformed_rows = list()

        # Ждем для проверки результата
        page.wait_for_timeout(2000)  # Задержка, чтобы вы могли проверить работу

        # Нажимаем на вторую кнопку с указанным классом
        print("Ищу и нажимаю на вторую кнопку с заданным классом...")
        page.locator("button.link-module_linkBase_R4YRK").nth(1).click()  # Выбираем вторую кнопку (индекс 1)

        # page.locator(f"button.calendar-day-module_day_3tyvM.calendar-day-module_calendarCell_1mCgU").nth(1).click()
        # aria_label_value = "14.01.2025"  # Замените на значение вашего `aria-label`

        start, bd_data = get_previous_dates()

        # start = "20.03.2025"
        # bd_data = "2025-03-20"

        print(f"Ищу кнопку сaria-label: {start}...")
        page.click(f"[aria-label='{start}']")  # Кликаем по кнопке

        print(f"Ищу кнопку с aria-label: {start}...")
        page.click(f"[aria-label='{start}']")  # Кликаем по кнопке

        time.sleep(2)

        for title_2, lst in all_data.items():
            for title_3 in lst:
                print(title_2, title_3)
                # Нажимаем на вторую кнопку с указанным классом
                page.locator("button.link-module_linkBase_R4YRK").nth(0).click()  # Выбираем вторую кнопку (индекс 1)
                page.wait_for_timeout(1000)
                page.click("#CAT17027482")
                page.click(f"#{title_2}")
                page.click(f"#{title_3}")
                page.locator("button.custom-button_button_1Vyeq").click() # Выбираем вторую кнопку (индекс 1)
                page.wait_for_timeout(2000)

                transformed_rows = []  # Список для хранения обработанных строк

                try:
                    # Ожидаем загрузки таблицы (увеличьте timeout при необходимости)
                    page.wait_for_selector("tr.table-row-module_row_tR-2M.table-row-module_hoverable_1BGOb",
                                           timeout=5000)

                    # Локатор строк таблицы (новый селектор)
                    row_locator = page.locator("tr.table-row-module_row_tR-2M.table-row-module_hoverable_1BGOb")

                    # Подсчёт строк
                    row_count = row_locator.count()
                    print(f"🔍 Найдено строк: {row_count}")

                    # Проход по строкам таблицы
                    for i in range(row_count):
                        # Локатор ячеек в текущей строке
                        cells = row_locator.nth(i).locator("td")
                        cell_count = cells.count()

                        # Извлечение данных из ячеек
                        row_data = [cells.nth(j).inner_text().strip() for j in range(cell_count)]

                        # Проверка на пустые строки
                        if not any(row_data):
                            continue

                        print(f" Данные строки {i + 1}: {row_data}")

                        # Трансформация данных (если у вас есть функция обработки `transform_row()`)
                        result = transform_row(row_data, bd_data, my_category.get(title_3))

                        if result:
                            transformed_rows.append(result)

                except Exception as e:
                    print(f" Ошибка при парсинге таблицы: {e}")
                update_db(transformed_rows)

        browser.close()
        print(" Браузер закрыт.")


if __name__ == "__main__":
    login_with_saved_session()
