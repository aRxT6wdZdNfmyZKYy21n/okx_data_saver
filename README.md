# OKX Data Saver

Высокопроизводительный Python-сервис для сбора и сохранения данных стакана заявок (order book) с биржи OKX через WebSocket соединения в режиме реального времени.

## 📋 Описание

OKX Data Saver - это асинхронное приложение для получения и сохранения данных order book с криптовалютной биржи OKX. Сервис подключается к публичному WebSocket API OKX, получает обновления стакана заявок в реальном времени и сохраняет их в PostgreSQL базу данных.

### Основные возможности

- **Асинхронная архитектура**: Использует asyncio и uvloop для высокой производительности
- **WebSocket подключения**: Надежное соединение с OKX WebSocket API с автоматическим переподключением
- **Управление состоянием**: Отслеживание состояния order book с проверкой целостности последовательности сообщений
- **Сохранение в БД**: Асинхронное сохранение данных в PostgreSQL с использованием SQLAlchemy
- **Уведомления**: Telegram уведомления о проблемах с подключением
- **Поддержка прокси**: Опциональная поддержка SOCKS5 прокси
- **Обработка ошибок**: Комплексная система обработки ошибок и логирования

## 🏗️ Архитектура

```
okx_data_saver/
├── main/save_order_books/          # Основной модуль сбора данных
│   ├── __main__.py                 # Точка входа приложения
│   ├── okx_web_socket_connection_manager.py  # Менеджер WebSocket соединений
│   ├── schemas.py                  # SQLAlchemy модели данных
│   ├── globals.py                  # Глобальные объекты (БД, очереди)
│   └── constants_.py              # Константы конфигурации
├── constants/                      # Общие константы
├── event/                         # Система асинхронных событий
├── utils/                         # Утилиты
│   ├── async_.py                  # Асинхронные хелперы
│   ├── json.py                    # JSON утилиты
│   ├── proxy.py                   # Работа с прокси
│   ├── telegram.py                # Telegram интеграция
│   └── time.py                    # Работа со временем
├── data/                          # Директория данных
├── settings.py                    # Конфигурация приложения
└── requirements.txt               # Зависимости
```

## 📦 Установка

### Системные требования

- Python 3.12+
- PostgreSQL 12+
- Linux/macOS (рекомендуется)

### Шаги установки

1. **Клонирование репозитория**
```bash
git clone git@github.com:aRxT6wdZdNfmyZKYy21n/okx_data_saver.git
cd okx_data_saver
```

2. **Создание виртуального окружения**
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# или
.venv\Scripts\activate     # Windows
```

3. **Установка зависимостей**
```bash
pip install -r requirements.txt
```

4. **Настройка PostgreSQL**
```sql
-- Создание базы данных
CREATE DATABASE okx;
CREATE USER okx_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE okx TO okx_user;
```

5. **Настройка переменных окружения**

Создайте файл `.env` в корне проекта:
```env
POSTGRES_DB_HOST_NAME=localhost
POSTGRES_DB_NAME=okx
POSTGRES_DB_PORT=5432
POSTGRES_DB_USER_NAME=okx_user
POSTGRES_DB_PASSWORD=your_password

# Опционально: для Telegram уведомлений
TELEGRAM_BOT_TOKEN=your_bot_token
```

## 🚀 Использование

### Запуск сервиса

```bash
python -m main.save_order_books
```

### Базовая конфигурация

В файле `main/save_order_books/constants_.py` можно настроить:

```python
# Отправка уведомлений о проблемах с WebSocket
IS_NEED_SEND_NOTIFICATIONS_ABOUT_WEB_SOCKET_CONNECTION_CLOSED_WITH_ERROR = True

# Использование прокси (требует настройки файлов прокси в data/)
USE_PROXIES = False
```

### Выбор торговой пары

По умолчанию сервис подписывается на данные пары BTC-USDT. Для изменения отредактируйте файл `main/save_order_books/__main__.py`:

```python
await okx_web_socket_connection_manager.subscribe(
    symbol_name='ETH-USDT'  # Замените на нужную пару
)
```

## 📊 Структура данных

Данные order book сохраняются в таблице `okx_order_book_data`:

| Поле | Тип | Описание |
|------|-----|----------|
| symbol_name | TEXT | Название торговой пары (например, BTC-USDT) |
| timestamp_ms | BIGINT | Временная метка сохранения в миллисекундах |
| action | TEXT | Тип операции ('snapshot' или 'update') |
| asks | JSON | Массив заявок на продажу [price, quantity, _, _] |
| bids | JSON | Массив заявок на покупку [price, quantity, _, _] |

Композитный первичный ключ: `(symbol_name, timestamp_ms)`

## 🔧 Настройка

### Конфигурация базы данных

Все настройки БД управляются через переменные окружения в классе `Settings`:

```python
class Settings(BaseSettings):
    POSTGRES_DB_HOST_NAME: str
    POSTGRES_DB_NAME: str
    POSTGRES_DB_PORT: int
    POSTGRES_DB_PASSWORD: SecretStr
    POSTGRES_DB_USER_NAME: str
```

### Система событий

Проект использует кастомную систему асинхронных событий для обработки новых данных order book:

```python
# Подписка на событие новых данных
okx_web_socket_connection_manager_on_new_order_book_data_event += on_new_order_book_data
```

## 📈 Мониторинг

### Логирование

Сервис использует стандартный Python logging с форматом:
```
[LEVEL][TIMESTAMP][MODULE]: message
```

### Telegram уведомления

При настройке `TELEGRAM_BOT_TOKEN` сервис отправляет уведомления о:
- Ошибках WebSocket соединения
- Проблемах с прокси
- Критических исключениях

### Статистика подключений

WebSocket менеджер ведет статистику:
- Количество полученных сообщений
- Количество ping/pong
- Количество подписок/отписок

## 🛠️ Разработка

### Запуск в режиме разработки

Для включения отладочного режима в `globals.py`:
```python
postgres_db_engine = create_async_engine(
    # ... connection string ...
    echo=True,  # Включает SQL логирование
)
```

### Тестирование

Для тестирования отдельных компонентов:

```python
# Тест WebSocket соединения
from main.save_order_books.okx_web_socket_connection_manager import OKXWebSocketConnectionManager

manager = OKXWebSocketConnectionManager(0, 0, 1)
await manager.subscribe('BTC-USDT')
```

## 📚 Зависимости

### Основные

- **asyncpg**: Асинхронный PostgreSQL драйвер
- **SQLAlchemy[asyncio]**: ORM с поддержкой asyncio
- **websockets**: WebSocket клиент
- **pydantic & pydantic-settings**: Валидация и настройки
- **uvloop**: Высокопроизводительный event loop
- **orjson**: Быстрый JSON парсер

### Дополнительные

- **aiogram**: Telegram Bot API
- **PyQt6**: GUI утилиты (опционально)

## 🔒 Безопасность

- Пароли БД хранятся как `SecretStr` в pydantic
- Поддержка переменных окружения для чувствительных данных
- Опциональная поддержка SOCKS5 прокси для анонимности

## 📝 Лицензия

Проект разработан для образовательных и исследовательских целей.

## 🤝 Вклад в проект

1. Fork репозитория
2. Создайте feature branch (`git checkout -b feature/amazing-feature`)
3. Commit изменения (`git commit -m 'Add amazing feature'`)
4. Push в branch (`git push origin feature/amazing-feature`)
5. Создайте Pull Request

## 📞 Поддержка

При возникновении проблем:
1. Проверьте логи приложения
2. Убедитесь в правильности настройки переменных окружения
3. Проверьте доступность PostgreSQL
4. Создайте issue в репозитории с подробным описанием проблемы
