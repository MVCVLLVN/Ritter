import time
import pandas as pd
import psycopg2
import json
import os
from playwright.sync_api import sync_playwright
from datetime import date

# URL and paths
url_report = r'https://seller.ozon.ru/app/dashboard/main'
download_path = r''
filename = 'ozon_rating.xlsx'
firefox_user_data_dir = r""
auth_state_path = r''
db_config_path = r''
today_date = date.today().strftime("%d.%m.%Y")


def process_and_upload_data(file_path, date_today):
    with open(db_config_path) as db_config_file:
        db_config = json.load(db_config_file)
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

    # Load data from Excel file
    df = pd.read_excel(file_path)

    # Fill missing ratings with the last non-null value above
    df['Рейтинг товара'] = df['Рейтинг товара'].ffill()

    # Add today's date as a new column
    df['day'] = date_today

    # Select only relevant columns and rename them
    df = df[['SKU', 'Кол-во отзывов', 'Рейтинг товара', 'day']]
    df.columns = ['sku', 'reviews_qty', 'rating', 'day']

    # Filter out any rows where SKU might be empty
    df = df.dropna(subset=['sku'])
    df['sku'] = df['sku'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')
    df['sku'] = df['sku'].astype(str)

    # Convert DataFrame to a list of tuples for database insertion
    values = df.values.tolist()

    # SQL query for inserting/updating data
    query = '''
        INSERT INTO ozon.ozon_rating (sku, reviews_qty, rating, day)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (day, sku) DO UPDATE
        SET reviews_qty = EXCLUDED.reviews_qty,
            rating = EXCLUDED.rating;
    '''

    # Insert data into the database
    cursor.executemany(query, values)
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()

# MAIN SECTION
playwright = sync_playwright().start()
context = playwright.firefox.launch_persistent_context(
            firefox_user_data_dir,
            headless=False,
            channel='firefox'
        )
page = context.new_page()
page.goto(url_report, wait_until = 'load')
input()
time.sleep(10)

page.locator("text=Оценка товаров").click()
time.sleep(10)

page.locator("xpath=/html/body/div[1]/div[4]/div/div/div/div[2]/div/div/div/div[1]/div[2]/button/div/span").click()

# Wait for the download to complete
with page.expect_download() as download_info:
    download = download_info.value
    download.save_as(os.path.join(download_path, filename))
    print(f'File successfully saved at: {os.path.join(download_path, filename)}')

# Process and upload data
process_and_upload_data(os.path.join(download_path, filename), today_date)

playwright.stop()
