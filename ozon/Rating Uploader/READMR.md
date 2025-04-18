# Ozon Product Rating Uploader

Скрипт автоматически авторизуется в личном кабинете Ozon Seller, скачивает отчёт с рейтингами товаров, обрабатывает Excel-файл и загружает данные в PostgreSQL.

## Возможности

- Авторизация через сохранённую сессию Firefox
- Загрузка отчёта с вкладки "Оценка товаров"
- Обработка Excel-файла:
  - Заполнение пустых рейтингов предыдущими значениями
  - Преобразование SKU и фильтрация пустых строк
- Загрузка в таблицу PostgreSQL `ozon.ozon_rating` с обновлением (`ON CONFLICT`)

## Зависимости

- Python 3.10+
- pandas
- psycopg2-binary

Установка зависимостей:

```bash
pip install pandas psycopg2-binary playwright
playwright install
