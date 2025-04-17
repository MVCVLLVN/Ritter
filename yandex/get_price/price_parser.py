import time
from playwright.sync_api import sync_playwright
import datetime
import pytz
import psycopg2
import json
from datetime import datetime
from bs4 import BeautifulSoup
import re
import traceback
import urllib3

db_config_path = r""
TG_CONFIG_PATH = r""

with open(db_config_path) as db_config_path:
    db_config = json.load(db_config_path)

with open(TG_CONFIG_PATH) as tg_config_path:
    tg_config = json.load(tg_config_path)

moscow_tz = pytz.timezone("Europe/Moscow")
current_date = datetime.now(moscow_tz)

http = urllib3.PoolManager()
approved_list = [, ]
credentials = tg_config['TG credentials']

statistic = [0, 0]
fail_page = list()

def make_request(method, url, body=None, headers=None):
    response = http.request(method, url, body=body, headers=headers)
    attempt = 1
    while response.status not in [200, 204] and attempt <= 10:
        print(response.status, response.data.decode('utf-8'))
        time.sleep(10)
        response = http.request(method, url, body=body, headers=headers)
        attempt += 1
    return json.loads(response.data.decode('utf-8'))

def send_telegram_notification(text):
    for chat_id in approved_list:
        text_url = f'https://api.telegram.org/bot{credentials}/sendMessage?chat_id={chat_id}&text={text}&parse_mode=HTML'
        texting = make_request('POST', url=text_url)

def insert_search_queries(data):
    try:
        insert_query = """
            INSERT INTO yandexmarket.yandex_price_monitoring
            (sku_seller, my_price, price_in_display, cofinancing, day, link)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (sku_seller, day) 
            DO UPDATE SET 
                my_price = EXCLUDED.my_price,
                price_in_display = EXCLUDED.price_in_display,
                cofinancing = EXCLUDED.cofinancing,
                link = EXCLUDED.link;
            """
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_query, data)
            conn.commit()
    except Exception as e:
        print(f"Ошибка при вставке данных: {e}")


def get_max_page(html):
    soup = BeautifulSoup(html, 'html.parser')

    # Ищем только нужный блок с пагинацией
    pagination_div = soup.find('div', class_='___pages___n7lSe')

    buttons = pagination_div.find_all('button')
    page_numbers = []
    for btn in buttons:
        title = btn.get('title')
        if title and title.isdigit():
            page_numbers.append(int(title))
    if page_numbers:
        max_page = max(page_numbers)
        return max_page
    return None

def clean_price(text):
    return re.sub(r"[^\d\-]", "", text)

def get_my_price(row):
    # Ваша цена
    your_price_cell = row.select_one('[data-e2e="price-cell"]')
    your_price_raw = your_price_cell.get_text(strip=True) if your_price_cell else None
    return int(clean_price(your_price_raw))

def get_price_on_display(row):
    # На витрине
    showcase_cell = row.select_one('[data-e2e="showcase-cell"]')
    showcase_price_raw = showcase_cell.get_text(strip=True) if showcase_cell else None
    return int(clean_price(showcase_price_raw))

def get_sku(row):
    sku_tag = row.select_one('[data-e2e="offer-id"]')
    return sku_tag.get_text(strip=True) if sku_tag else None

def get_product_url(row) -> str:
    a_tags = row.select('a[href]')
    for a in a_tags:
        href = a.get('href')
        if href and 'market.yandex.ru/product' in href:
            return 'https:' + href if href.startswith('//') else href
    return None

def get_count_page():
    # Укажите директорию с сохраненными данными профиля
    user_data_path = r"C:\Users\m.troshin\PycharmProjects\Pp\yandex_parsers\data_user"

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_path,
            headless=False,
            channel="chrome"
        )
        page = browser.pages[0] if browser.pages else browser.new_page()

        url = f"https://partner.market.yandex.ru/business/923477/prices?campaignId=22096514&activeTab=offers&businessOfferStatus=1&offerNameOrSku=&page=1"  # Замените на ваш URL
        page.goto(url, wait_until="load")
        time.sleep(6)

        html = page.content()
        page.wait_for_selector('table.___table___AgVjd', timeout=20000)
        count_page = get_max_page(html)
        print("Количество страниц", count_page)
        browser.close()
        return count_page

def login_with_saved_session(count):
    # Укажите директорию с сохраненными данными профиля
    user_data_path = r"C:\Users\m.troshin\PycharmProjects\Pp\yandex_parsers\data_user"

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_path,
            headless=False,
            channel="chrome"
        )
        page = browser.pages[0] if browser.pages else browser.new_page()

        for i in range(1, count + 1):
            url = f"https://partner.market.yandex.ru/business/923477/prices?campaignId=22096514&activeTab=offers&businessOfferStatus=1&offerNameOrSku=&page={i}"  # Замените на ваш URL
            print(f"Открываю сайт: {url}")
            page.goto(url, wait_until="load")
            time.sleep(6)

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")

            # count_page = get_max_page(html)
            # print("Количество страниц", count_page)

            # ищем тег таблицы
            page.wait_for_selector('table.___table___AgVjd',  timeout=20000)

            try:
                day = datetime.now().strftime("%Y-%m-%d")
                rows = soup.select("tr[data-e2e-row-offer-id]")
                print(f"Найдено строк: {len(rows)}")
                for row in rows:
                    # page.wait_for_selector(".style-priceLink___57y8a")
                    sku = get_sku(row)
                    your_price = get_my_price(row)
                    showcase_price = get_price_on_display(row)
                    sum_co_financing = round((your_price - showcase_price) / your_price,  4)
                    product_url = get_product_url(row)
                    # 💬 Вывод
                    print(f"🔸Артикул: {sku}")
                    print(f"💰Ваша цена: {your_price}")
                    print(f"🛍 На витрине: {showcase_price}")
                    print(f" % Cофинансирования : {sum_co_financing}")
                    print(f" Ссылка на товар : {product_url}")
                    print("-" * 50)
                    insert_search_queries(
                        (sku, your_price, showcase_price, sum_co_financing, day, product_url)
                    )


            except TypeError:
                print(traceback.print_exc())
                fail_page.append(i)
                statistic[1] += 1
            else:
                statistic[0] += 1
        # Закрываем браузер
        browser.close()
        print(f"Не спарислись страницы - {fail_page}")

if __name__ == "__main__":
    text_start = "Парсер <b>Yandex Market Price Parser</b> запущен."
    send_telegram_notification(text_start)
    count = get_count_page()
    print("Запуск скрипта...")
    login_with_saved_session(count)
    success_count, fail_count = statistic
    print("Скрипт успешно завершен!")
    text_finish = (f'Парсер <b>Yandex Market Price Parser</b> завершил работу. '
                   f'\nУспешно: {success_count}.\nОшибок: {fail_count}.\n'
                   f'Не получены данные со страниц {fail_page}')
    send_telegram_notification(text_finish)
