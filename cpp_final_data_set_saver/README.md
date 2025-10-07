# OKX Final Data Set Saver

C++ приложение для обработки и сохранения финальных датасетов OKX данных с использованием PostgreSQL.

## Описание

Это приложение является C++ версией модуля `save_final_data_set` из Python проекта. Оно обрабатывает данные order book и сделок, рассчитывает статистики и сохраняет результаты в базу данных PostgreSQL.

## Основные возможности

- Обработка данных order book и сделок
- Расчет статистик (min/max цены, общий объем, VWAP)
- Сохранение результатов в PostgreSQL
- Конфигурация через .env файлы
- Graceful shutdown при получении сигналов
- Логирование с различными уровнями

## Требования

- C++17 или выше
- CMake 3.16+
- PostgreSQL с libpqxx
- Linux/macOS

## Установка зависимостей

### Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install build-essential cmake libpqxx-dev postgresql-client
```

### CentOS/RHEL:
```bash
sudo yum install gcc-c++ cmake postgresql-devel
```

## Сборка

1. Клонируйте репозиторий
2. Перейдите в директорию проекта
3. Запустите сборку:

```bash
cd cpp_final_data_set_saver
./scripts/build.sh
```

## Конфигурация

1. Скопируйте пример конфигурации:
```bash
cp config/env_example.txt .env
```

2. Отредактируйте .env файл с вашими настройками:
```bash
nano .env
```

### Параметры конфигурации:

- `POSTGRES_DB_HOST_NAME` - хост PostgreSQL
- `POSTGRES_DB_PORT` - порт PostgreSQL (по умолчанию 5432)
- `POSTGRES_DB_NAME` - имя базы данных
- `POSTGRES_DB_USER_NAME` - имя пользователя
- `POSTGRES_DB_PASSWORD` - пароль
- `PROCESSING_INTERVAL_MS` - интервал обработки в миллисекундах
- `MAX_RETRIES` - максимальное количество повторных попыток
- `LOG_LEVEL` - уровень логирования (DEBUG, INFO, WARN, ERROR)

## Запуск

```bash
./scripts/run.sh
```

Или напрямую:
```bash
./build/okx_final_data_set_saver
```

## Остановка

Нажмите `Ctrl+C` для graceful shutdown.

## Структура проекта

```
cpp_final_data_set_saver/
├── src/                    # Исходный код
│   ├── database/          # Работа с базой данных
│   ├── processors/        # Обработка данных
│   └── utils/            # Утилиты
├── include/               # Заголовочные файлы
├── config/               # Конфигурация
├── scripts/              # Скрипты сборки и запуска
├── test/                 # Тесты
└── build/                # Собранные файлы
```

## Тестирование

```bash
cd build
make test
```

## Логирование

Приложение выводит логи в консоль с временными метками:
- `[INFO]` - информационные сообщения
- `[ERROR]` - ошибки
- `[DEBUG]` - отладочная информация (только при LOG_LEVEL=DEBUG)

## Производительность

C++ версия обеспечивает:
- Увеличение скорости обработки в 2-5 раз по сравнению с Python
- Более эффективное использование памяти
- Более стабильную работу под нагрузкой

## Устранение неполадок

1. **Ошибка подключения к базе данных**: Проверьте настройки в .env файле
2. **Ошибка сборки**: Убедитесь, что установлены все зависимости
3. **Ошибка разрешений**: Проверьте права доступа к файлам

## Лицензия

Проект использует ту же лицензию, что и основной Python проект.
