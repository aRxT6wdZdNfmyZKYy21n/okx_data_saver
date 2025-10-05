#!/usr/bin/env python3
"""
Простой тест C++ модуля без зависимостей от настроек проекта.
"""

import os
import sys
from datetime import UTC, datetime

import polars as pl

# Добавляем путь к модулю
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_basic_functionality():
    """Тестирует базовую функциональность C++ модуля."""
    print('🚀 Тестируем базовую функциональность C++ модуля...')

    try:
        import cpp_data_processor

        print('✅ C++ модуль успешно импортирован')
    except ImportError as e:
        print(f'❌ Ошибка импорта C++ модуля: {e}')
        return False

    # Создаем процессор
    processor = cpp_data_processor.DataProcessor()
    print('✅ Процессор создан')

    # Проверяем подключение к Redis
    redis_connected = processor.is_redis_connected()
    print(
        f'📡 Redis подключение: {"✅ Подключен" if redis_connected else "❌ Не подключен"}'
    )

    # Создаем тестовые данные
    data = {
        'trade_id': [1, 2, 3, 4, 5],
        'price': [50000.0, 50100.0, 50050.0, 50200.0, 50150.0],
        'quantity': [0.1, 0.2, 0.15, 0.3, 0.25],
        'is_buy': [True, False, True, False, True],
        'datetime': [
            datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 1, 10, 1, 0, tzinfo=UTC),
            datetime(2024, 1, 1, 10, 2, 0, tzinfo=UTC),
            datetime(2024, 1, 1, 10, 3, 0, tzinfo=UTC),
            datetime(2024, 1, 1, 10, 4, 0, tzinfo=UTC),
        ],
    }

    df = pl.DataFrame(data)
    print(f'✅ Создан тестовый DataFrame: {df.shape}')

    # Тестируем обработку данных
    print('\n📊 Тестируем обработку данных...')
    try:
        result = processor.process_trades_data(cpp_data_processor.SymbolId.BTC_USDT, df)
        print(f'✅ Обработка данных завершена: {result.success}')
        if result.success:
            print(f'📈 Сообщение: {result.error_message}')
            print(f'⏱️ Время обработки: {result.processing_time_seconds:.2f} сек')
    except Exception as e:
        print(f'❌ Ошибка обработки данных: {e}')
        return False

    # Тестируем статистику
    print('\n📊 Тестируем получение статистики...')
    try:
        stats = processor.get_processing_stats()
        print(f'✅ Статистика получена: {stats}')
    except Exception as e:
        print(f'❌ Ошибка получения статистики: {e}')
        return False

    print('\n🎉 Все базовые тесты прошли успешно!')
    return True


if __name__ == '__main__':
    success = test_basic_functionality()
    if success:
        print('\n✅ C++ модуль работает корректно!')
        sys.exit(0)
    else:
        print('\n❌ Обнаружены проблемы с C++ модулем')
        sys.exit(1)
