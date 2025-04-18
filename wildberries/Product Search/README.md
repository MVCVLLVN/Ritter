# Wildberries Product Search Analytics

Скрипт автоматически получает данные по поисковым запросам и пользовательским метрикам из API Wildberries и загружает их в базу данных PostgreSQL.

## Возможности

- Подключение к API Wildberries с авторизацией
- Получение данных по метрикам: частотность, позиция, клики, корзины, заказы, видимость
- Поддержка разбивки SKU по пакетам (по 50 элементов)
- Обработка и преобразование структуры ответа
- Загрузка в PostgreSQL с обновлением (`ON CONFLICT`)
- Логирование ошибок и повторных попыток при сбоях

## Метрики, получаемые от Wildberries

- `keyword`: поисковый запрос
- `sku`: артикул товара
- `frequency`: частотность запроса
- `rank`: средняя позиция товара
- `clicks_from_search`: количество переходов в карточку
- `baskets_from_search`: добавлений в корзину
- `orders_from_search`: заказов
- `visibility_in_search`: видимость в поисковой выдаче

## Структура таблицы в PostgreSQL

```sql
CREATE TABLE wildberries.wildberries_product_queries (
    sku BIGINT NOT NULL,
    keyword TEXT NOT NULL,
    day DATE NOT NULL,
    frequency INTEGER,
    rank FLOAT,
    clicks INTEGER,
    baskets INTEGER,
    orders INTEGER,
    visibility_in_search FLOAT,
    PRIMARY KEY (sku, keyword, day)
);
