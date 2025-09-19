# Миграция symbol_name -> symbol_id

Этот скрипт мигрирует данные из таблиц с `symbol_name` (строка) на `symbol_id` (целое число).

## Описание

Скрипт создает новые таблицы с постфиксом `_2` и мигрирует данные, преобразуя строковые имена символов в целочисленные идентификаторы согласно перечислению `SymbolId`:

- `BTC-USDT` -> `1` (SymbolId.BTC_USDT)
- `ETH-USDT` -> `2` (SymbolId.ETH_USDT)  
- `SOL-USDT` -> `3` (SymbolId.SOL_USDT)

## Установка зависимостей

```bash
pip install -r requirements_migration.txt
```

## Использование

### 1. Настройка файла .env

Создайте файл `.env` в корневой папке проекта со следующими переменными:

```env
POSTGRES_DB_HOST_NAME=localhost
POSTGRES_DB_NAME=your_database_name
POSTGRES_DB_PORT=5432
POSTGRES_DB_PASSWORD=your_password
POSTGRES_DB_USER_NAME=your_username
```

**Важно:** Файл `.env` должен находиться в корневой папке проекта (рядом с `settings.py`), а не в папке `ai_code/`.

### 2. Запуск миграции

```bash
python migrate_symbol_name_to_symbol_id.py
```

Скрипт автоматически использует настройки из `settings.py`, которые читают переменные из файла `.env`.

## Что делает скрипт

1. **Создает новые таблицы:**
   - `okx_trade_data_2`
   - `okx_order_book_data_2`
   - `okx_candle_data_15m_2`
   - `okx_candle_data_1H_2`

2. **Мигрирует данные:**
   - Копирует все данные из старых таблиц в новые
   - Преобразует `symbol_name` в `symbol_id`
   - Фильтрует только поддерживаемые символы (BTC-USDT, ETH-USDT, SOL-USDT)

3. **Создает индексы:**
   - `timestamp_ms_idx_2` для таблицы `okx_trade_data_2`

4. **Проверяет результаты:**
   - Выводит количество записей в каждой таблице до и после миграции

## Логирование

Скрипт выводит подробную информацию о процессе миграции в stdout:
- Подключение к базе данных
- Создание таблиц
- Количество мигрированных записей
- Проверка результатов

## Безопасность

- **Старые таблицы НЕ удаляются** - скрипт только создает новые
- Миграция выполняется в транзакции
- При ошибке все изменения откатываются

## Новые схемы

После миграции используйте новые классы в схемах:
- `OKXTradeData2` вместо `OKXTradeData`
- `OKXOrderBookData2` вместо `OKXOrderBookData`
- `OKXCandleData15m2` вместо `OKXCandleData15m`
- `OKXCandleData1H2` вместо `OKXCandleData1H`

## Удаление старых таблиц

После успешной миграции и проверки данных, старые таблицы можно удалить вручную:

```sql
DROP TABLE IF EXISTS okx_trade_data;
DROP TABLE IF EXISTS okx_order_book_data;
DROP TABLE IF EXISTS okx_candle_data_15m;
DROP TABLE IF EXISTS okx_candle_data_1H;
```

