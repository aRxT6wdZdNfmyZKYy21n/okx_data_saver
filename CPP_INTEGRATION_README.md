# C++ Data Processor Integration

## Обзор

Этот документ описывает интеграцию высокопроизводительного C++ data processor с существующей Python системой обработки финансовых данных.

## 🚀 Быстрый старт

### 1. Развертывание C++ процессора

```bash
# Автоматическое развертывание
python3 scripts/deploy_cpp_processor.py

# Развертывание без тестов (быстрее)
python3 scripts/deploy_cpp_processor.py --skip-tests

# Откат к предыдущей версии
python3 scripts/deploy_cpp_processor.py --rollback
```

### 2. Использование в коде

```python
# Импорт гибридного процессора
from main.process_data.hybrid_data_processor import g_cpp_data_processor

# Получение процессора
processor = g_cpp_data_processor

# Обработка данных
await processor.process_trades_data(symbol_id, trades_df)
```

## 📊 Архитектура

### Компоненты системы

1. **C++ Data Processor** - высокопроизводительная обработка данных
2. **Python Wrapper** - обертка для интеграции с Python
3. **Hybrid Processor** - комбинированный процессор
4. **Configuration System** - система конфигурации
5. **Performance Monitoring** - мониторинг производительности

### Схема работы

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Python Code   │───▶│  Hybrid Processor │───▶│  C++ Processor  │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │  Python Fallback │
                       │   (if needed)    │
                       └──────────────────┘
```

## 🔧 Конфигурация

### Профили конфигурации

```python
from config.cpp_processor_config import get_config

# Разработка
config = get_config('development')

# Продакшн
config = get_config('production')

# Тестирование
config = get_config('testing')
```

### Настройка параметров

```python
from config.cpp_processor_config import CppProcessorConfig, ProcessorMode

# Создание кастомной конфигурации
config = CppProcessorConfig(
    mode=ProcessorMode.HYBRID,
    enable_cpp=True,
    large_dataset_threshold=1000,
    bollinger_period=20,
    rsi_period=14
)
```

## 📈 Мониторинг производительности

### Запуск бенчмарков

```bash
# Полный бенчмарк
python3 benchmarks/performance_benchmark.py

# Тест C++ модуля
python3 cpp_data_processor/test_build.py
```

### Получение статистики

```python
# Статистика гибридного процессора
stats = processor.get_processing_stats()
print(f"Total processed: {stats['total_processed']}")
print(f"C++ usage: {stats['cpp_usage_percentage']:.1f}%")
print(f"Average time: {stats['average_time_ms']:.2f}ms")
```

## 🛠 Разработка

### Структура проекта

```
cpp_data_processor/
├── include/              # Заголовочные файлы C++
├── src/                  # Исходный код C++
├── build/                # Собранные файлы
├── examples/             # Примеры использования
├── tests/                # Тесты
└── CMakeLists.txt        # Конфигурация сборки

main/process_data/
├── cpp_data_processor_wrapper.py  # Обертка C++ процессора
├── hybrid_data_processor.py       # Гибридный процессор
└── data_processor.py              # Оригинальный Python процессор

config/
└── cpp_processor_config.py        # Конфигурация

benchmarks/
└── performance_benchmark.py       # Бенчмарки производительности

scripts/
└── deploy_cpp_processor.py        # Скрипт развертывания
```

### Сборка C++ модуля

```bash
cd cpp_data_processor
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Добавление новых компонентов

1. Создать заголовочный файл в `include/`
2. Реализовать в `src/`
3. Добавить Python bindings в `src/python_bindings.cpp`
4. Обновить `CMakeLists.txt`
5. Добавить тесты

## 🔍 Отладка

### Проверка доступности C++ модуля

```python
from main.process_data.cpp_data_processor_wrapper import g_cpp_data_processor_wrapper

wrapper = g_cpp_data_processor_wrapper
print(f"C++ available: {wrapper.is_cpp_available()}")
print(f"Processor info: {wrapper.get_processor_info()}")
```

### Логирование

```python
import logging

# Включить подробное логирование
logging.basicConfig(level=logging.DEBUG)

# Логирование только C++ процессора
logger = logging.getLogger('cpp_data_processor')
logger.setLevel(logging.DEBUG)
```

## 📊 Производительность

### Ожидаемые улучшения

- **Bollinger Bands**: ~10-20x быстрее
- **Candles Processing**: ~5-15x быстрее
- **RSI Calculation**: ~8-12x быстрее
- **Smoothing**: ~6-10x быстрее
- **Extreme Lines**: ~4-8x быстрее
- **Общая производительность**: ~8-12x быстрее

### Факторы производительности

1. **Размер данных**: C++ эффективнее для больших датасетов
2. **Тип операций**: Математические вычисления быстрее в C++
3. **Память**: C++ использует память более эффективно
4. **Компиляция**: Оптимизации компилятора

## 🚨 Устранение неполадок

### Проблема: Модуль не импортируется

**Решение:**
```bash
# Проверить наличие .so файла
ls -la *.so

# Пересобрать модуль
python3 scripts/deploy_cpp_processor.py
```

### Проблема: Низкая производительность

**Решение:**
```python
# Проверить конфигурацию
config = get_config('production')
print(config.to_cpp_params())

# Включить C++ для больших датасетов
config.prefer_cpp_for_large_datasets = True
config.large_dataset_threshold = 500
```

### Проблема: Ошибки компиляции

**Решение:**
```bash
# Установить зависимости
sudo apt install build-essential cmake python3-dev

# Установить pybind11
pip install pybind11 --break-system-packages

# Очистить и пересобрать
rm -rf cpp_data_processor/build
python3 scripts/deploy_cpp_processor.py
```

## 🔄 Миграция

### Поэтапная миграция

1. **Этап 1**: Развертывание C++ модуля
2. **Этап 2**: Тестирование на dev окружении
3. **Этап 3**: A/B тестирование на production
4. **Этап 4**: Полный переход на C++

### Откат изменений

```bash
# Откат к Python процессору
python3 scripts/deploy_cpp_processor.py --rollback

# Или программно
config.mode = ProcessorMode.PYTHON_ONLY
```

## 📚 Дополнительные ресурсы

- [pybind11 Documentation](https://pybind11.readthedocs.io/)
- [CMake Documentation](https://cmake.org/documentation/)
- [C++ Performance Best Practices](https://en.cppreference.com/)

## 🤝 Поддержка

Для получения помощи:
1. Проверьте логи системы
2. Запустите диагностические тесты
3. Обратитесь к команде разработки
4. Создайте issue в репозитории

---

**Примечание**: C++ Data Processor предназначен для значительного улучшения производительности обработки финансовых данных. При правильной настройке и использовании он может обеспечить ускорение в 8-12 раз по сравнению с Python версией.
