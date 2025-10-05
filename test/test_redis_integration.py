#!/usr/bin/env python3
"""
Тест интеграции Redis с C++ процессором данных.
Теперь использует Python RedisDataService для сохранения DataFrame'ов.
"""

import os
import sys
from datetime import UTC, datetime

import polars as pl

# Добавляем путь к модулю
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import cpp_data_processor

    print('✅ C++ модуль успешно импортирован')
except ImportError as e:
    print(f'❌ Ошибка импорта C++ модуля: {e}')
    sys.exit(1)


def create_test_data():
    """Создает тестовые данные для проверки."""
    # Создаем тестовый DataFrame с данными о сделках
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
    return df


def create_bollinger_data():
    """Создает тестовые данные для полос Боллинджера."""
    data = {
        'upper_band': [50100.0, 50150.0, 50200.0, 50250.0, 50300.0],
        'middle_band': [50000.0, 50050.0, 50100.0, 50150.0, 50200.0],
        'lower_band': [49900.0, 49950.0, 50000.0, 50050.0, 50100.0],
    }

    df = pl.DataFrame(data)
    print(f'✅ Создан DataFrame полос Боллинджера: {df.shape}')
    return df


def create_rsi_data():
    """Создает тестовые данные для RSI."""
    data = {
        'rsi': [45.5, 52.3, 48.7, 55.2, 49.8],
    }

    df = pl.DataFrame(data)
    print(f'✅ Создан DataFrame RSI: {df.shape}')
    return df


def test_redis_integration():
    """Тестирует интеграцию с Redis."""
    print('\n🚀 Начинаем тестирование Redis интеграции...')

    # Создаем процессор
    processor = cpp_data_processor.DataProcessor()
    print('✅ Процессор создан')

    # Проверяем подключение к Redis
    if not processor.is_redis_connected():
        print('❌ Redis клиент не подключен')
        return False

    print('✅ Redis клиент подключен')

    # Тестируем сохранение данных о сделках
    print('\n📊 Тестируем сохранение данных о сделках...')
    trades_df = create_test_data()

    try:
        processor.save_results_to_redis(
            symbol_id=cpp_data_processor.SymbolId.BTC_USDT,
            data_type='trades',
            dataframe=trades_df,
        )
        print('✅ Данные о сделках сохранены')
    except Exception as e:
        print(f'❌ Ошибка сохранения данных о сделках: {e}')
        return False

    # Тестируем загрузку данных о сделках
    print('\n📥 Тестируем загрузку данных о сделках...')
    try:
        loaded_trades = processor.load_data_from_redis(
            symbol_id=cpp_data_processor.SymbolId.BTC_USDT, data_type='trades'
        )

        if loaded_trades is not None:
            print(f'✅ Данные о сделках загружены: {loaded_trades.shape}')
        else:
            print('⚠️ Данные о сделках не найдены')
    except Exception as e:
        print(f'❌ Ошибка загрузки данных о сделках: {e}')
        return False

    # Тестируем сохранение полос Боллинджера
    print('\n📈 Тестируем сохранение полос Боллинджера...')
    bollinger_df = create_bollinger_data()

    try:
        processor.save_results_to_redis(
            symbol_id=cpp_data_processor.SymbolId.BTC_USDT,
            data_type='bollinger',
            dataframe=bollinger_df,
        )
        print('✅ Полосы Боллинджера сохранены')
    except Exception as e:
        print(f'❌ Ошибка сохранения полос Боллинджера: {e}')
        return False

    # Тестируем загрузку полос Боллинджера
    print('\n📥 Тестируем загрузку полос Боллинджера...')
    try:
        loaded_bollinger = processor.load_data_from_redis(
            symbol_id=cpp_data_processor.SymbolId.BTC_USDT, data_type='bollinger'
        )

        if loaded_bollinger is not None:
            print(f'✅ Полосы Боллинджера загружены: {loaded_bollinger.shape}')
        else:
            print('⚠️ Полосы Боллинджера не найдены')
    except Exception as e:
        print(f'❌ Ошибка загрузки полос Боллинджера: {e}')
        return False

    # Тестируем сохранение RSI
    print('\n📊 Тестируем сохранение RSI...')
    rsi_df = create_rsi_data()

    try:
        processor.save_results_to_redis(
            symbol_id=cpp_data_processor.SymbolId.BTC_USDT,
            data_type='rsi',
            dataframe=rsi_df,
        )
        print('✅ RSI данные сохранены')
    except Exception as e:
        print(f'❌ Ошибка сохранения RSI: {e}')
        return False

    # Тестируем загрузку RSI
    print('\n📥 Тестируем загрузку RSI...')
    try:
        loaded_rsi = processor.load_data_from_redis(
            symbol_id=cpp_data_processor.SymbolId.BTC_USDT, data_type='rsi'
        )

        if loaded_rsi is not None:
            print(f'✅ RSI данные загружены: {loaded_rsi.shape}')
        else:
            print('⚠️ RSI данные не найдены')
    except Exception as e:
        print(f'❌ Ошибка загрузки RSI: {e}')
        return False

    print('\n🎉 Все тесты Redis интеграции прошли успешно!')
    return True


if __name__ == '__main__':
    success = test_redis_integration()
    if success:
        print('\n✅ Интеграция Redis работает корректно!')
        sys.exit(0)
    else:
        print('\n❌ Обнаружены проблемы с интеграцией Redis')
        sys.exit(1)
