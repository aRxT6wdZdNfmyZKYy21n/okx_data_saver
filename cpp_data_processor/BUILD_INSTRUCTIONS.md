# Инструкции по сборке и интеграции C++ Data Processor

## Обзор

C++ Data Processor - это высокопроизводительная реализация процессора данных для OKX Data Saver, предназначенная для замены Python-версии с значительным улучшением производительности.

## Требования

### Системные требования
- **ОС**: Linux (Ubuntu 20.04+, Debian 11+), macOS 10.15+, Windows 10+
- **Компилятор**: GCC 7+, Clang 5+, или MSVC 2019+
- **CMake**: 3.15 или выше
- **Python**: 3.7 или выше

### Зависимости
- **pybind11**: 2.10.0 или выше
- **Python dev headers**: python3-dev (Linux) или python3-devel (CentOS/RHEL)
- **NumPy**: 1.21.0 или выше
- **Polars**: 0.20.0 или выше

## Установка зависимостей

### Ubuntu/Debian
```bash
sudo apt update
sudo apt install -y \
    build-essential \
    cmake \
    python3-dev \
    python3-pip \
    libpython3-dev

pip install pybind11 numpy polars
```

### CentOS/RHEL
```bash
sudo yum install -y \
    gcc-c++ \
    cmake \
    python3-devel \
    python3-pip

pip install pybind11 numpy polars
```

### macOS
```bash
# Установка через Homebrew
brew install cmake python3

pip install pybind11 numpy polars
```

## Сборка

### 1. Клонирование и подготовка
```bash
cd /path/to/okx_data_saver/cpp_data_processor
mkdir build
cd build
```

### 2. Конфигурация CMake
```bash
# Базовая конфигурация
cmake .. -DCMAKE_BUILD_TYPE=Release

# Или с указанием Python
cmake .. -DCMAKE_BUILD_TYPE=Release -DPYTHON_EXECUTABLE=$(which python3)
```

### 3. Сборка
```bash
# Сборка проекта
make -j$(nproc)

# Или на Windows
cmake --build . --config Release
```

### 4. Установка (опционально)
```bash
# Установка в систему
sudo make install

# Или установка в виртуальное окружение
pip install .
```

## Интеграция с существующим проектом

### 1. Копирование модуля
```bash
# Скопировать собранный модуль в проект
cp build/cpp_data_processor*.so /path/to/okx_data_saver/
# или на Windows
cp build/Release/cpp_data_processor*.pyd /path/to/okx_data_saver/
```

### 2. Обновление Python кода
Замените импорт в `main/process_data/data_processor.py`:

```python
# Старый код
from main.process_data.data_processor import g_data_processor

# Новый код
from cpp_data_processor.integration import get_cpp_integration

# Получить интеграцию
cpp_integration = get_cpp_integration()

# Использовать в методе process_trades_data
async def process_trades_data(self, symbol_id: SymbolId, trades_df: DataFrame) -> None:
    # Использовать C++ процессор
    result = cpp_integration.process_trades_data(symbol_id, trades_df)
    
    if result['success']:
        logger.info(f"C++ processing completed in {result['processing_time_seconds']:.3f}s")
    else:
        # Fallback к Python процессору
        await self._process_with_python(symbol_id, trades_df)
```

### 3. Настройка параметров
```python
# Настройка параметров C++ процессора
params = {
    'enable_bollinger_bands': True,
    'enable_candles': True,
    'enable_rsi': True,
    'enable_smoothing': True,
    'enable_extreme_lines': True,
    'bollinger_period': 20,
    'rsi_period': 14,
    'candle_intervals': ['1m', '5m', '15m', '1h', '4h', '1d'],
    'smoothing_levels': ['Raw (0)', 'Smoothed (1)']
}

cpp_integration.set_processing_params(params)
```

## Тестирование

### 1. Запуск тестов
```bash
cd build
make test
```

### 2. Запуск примеров
```bash
# Базовый пример
./basic_example

# Пример интеграции с Python
python3 ../examples/integration_example.py
```

### 3. Тестирование производительности
```python
import time
import polars as pl
from cpp_data_processor.integration import process_trades_with_cpp
from enumerations import SymbolId

# Создать тестовые данные
trades_df = create_test_data(10000)  # 10k сделок

# Тест C++ процессора
start_time = time.time()
result = process_trades_with_cpp(SymbolId.BTC_USDT, trades_df)
cpp_time = time.time() - start_time

print(f"C++ processing time: {cpp_time:.3f}s")
print(f"Success: {result['success']}")
```

## Отладка

### 1. Включение отладочной информации
```bash
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_VERBOSE_MAKEFILE=ON
make -j$(nproc)
```

### 2. Проверка зависимостей
```bash
# Проверить, что модуль может быть импортирован
python3 -c "import cpp_data_processor; print('C++ module loaded successfully')"
```

### 3. Логирование
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Включить подробное логирование
cpp_integration = get_cpp_integration()
logger = logging.getLogger(__name__)
logger.info(f"Processor info: {cpp_integration.get_processor_info()}")
```

## Производительность

### Ожидаемые улучшения
- **Bollinger Bands**: ~10-20x быстрее
- **Candles Processing**: ~5-15x быстрее
- **RSI Calculation**: ~8-12x быстрее
- **Smoothing**: ~6-10x быстрее
- **Extreme Lines**: ~4-8x быстрее
- **Общая производительность**: ~8-12x быстрее

### Мониторинг производительности
```python
# Получить статистику обработки
stats = cpp_integration.get_processing_stats()
print(f"Total trades processed: {stats['total_trades_processed']}")
print(f"Average processing time: {stats['average_processing_time_ms']}ms")
print(f"Successful operations: {stats['successful_operations']}")
```

## Устранение неполадок

### Проблема: Модуль не компилируется
**Решение**: Проверьте версии зависимостей и убедитесь, что все заголовочные файлы установлены.

### Проблема: Импорт модуля не работает
**Решение**: Убедитесь, что модуль находится в PYTHONPATH или в той же директории.

### Проблема: Низкая производительность
**Решение**: Убедитесь, что сборка выполнена в Release режиме и включены оптимизации.

### Проблема: Ошибки времени выполнения
**Решение**: Проверьте логи и убедитесь, что входные данные в правильном формате.

## Дальнейшее развитие

### Планируемые улучшения
- [ ] Многопоточность
- [ ] SIMD оптимизации
- [ ] GPU ускорение
- [ ] Дополнительные индикаторы
- [ ] Кэширование результатов

### Вклад в проект
1. Форкните репозиторий
2. Создайте feature branch
3. Внесите изменения
4. Добавьте тесты
5. Создайте pull request

## Поддержка

Для получения помощи:
- Создайте issue в репозитории
- Обратитесь к команде разработки
- Проверьте документацию и примеры
