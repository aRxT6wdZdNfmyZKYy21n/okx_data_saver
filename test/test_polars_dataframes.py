#!/usr/bin/env python3
"""
Тест для проверки работы C++ модуля с Polars DataFrame'ами.
"""

import sys
import os
import polars as pl
from datetime import datetime, timezone

# Добавляем путь к модулю
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_polars_dataframes():
    """Тестирует создание Polars DataFrame'ов из C++ структур."""
    print("🚀 Тестируем создание Polars DataFrame'ов из C++ структур...")
    
    try:
        import cpp_data_processor
        print("✅ C++ модуль успешно импортирован")
    except ImportError as e:
        print(f"❌ Ошибка импорта C++ модуля: {e}")
        return False
    
    # Создаем тестовые данные
    data = {
        'trade_id': [1, 2, 3, 4, 5],
        'price': [50000.0, 50100.0, 50050.0, 50200.0, 50150.0],
        'quantity': [0.1, 0.2, 0.15, 0.3, 0.25],
        'is_buy': [True, False, True, False, True],
        'datetime': [
            datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 10, 1, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 10, 2, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 10, 3, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 10, 4, 0, tzinfo=timezone.utc),
        ]
    }
    
    df = pl.DataFrame(data)
    print(f"✅ Создан тестовый DataFrame: {df.shape}")
    
    # Конвертируем в C++ структуры
    trades = cpp_data_processor.DataConverter.from_polars_trades(df)
    print(f"✅ Конвертировано в C++ структуры: {len(trades)} записей")
    
    # Тестируем создание Polars DataFrame'ов из различных структур
    print("\n📊 Тестируем создание Polars DataFrame'ов...")
    
    # Создаем тестовые C++ структуры
    from cpp_data_processor import CandleData, BollingerBands, RSIData, VelocityData
    
    # Тест 1: Candles DataFrame
    candles = [
        CandleData(50000.0, 50100.0, 49900.0, 50050.0, 1.5, 1, 5, 1640995200000, 1640995260000, 5),
        CandleData(50050.0, 50200.0, 50000.0, 50150.0, 2.0, 6, 10, 1640995260000, 1640995320000, 5)
    ]
    
    candles_df = cpp_data_processor.DataConverter.to_polars_candles(candles)
    print(f"✅ Candles DataFrame: {candles_df.shape}")
    print(f"   Колонки: {candles_df.columns}")
    
    # Тест 2: Bollinger Bands DataFrame
    bollinger = BollingerBands()
    bollinger.upper_band = [50100.0, 50200.0, 50300.0]
    bollinger.middle_band = [50000.0, 50100.0, 50200.0]
    bollinger.lower_band = [49900.0, 50000.0, 50100.0]
    bollinger.timeperiod = 20
    
    bollinger_df = cpp_data_processor.DataConverter.to_polars_bollinger(bollinger)
    print(f"✅ Bollinger DataFrame: {bollinger_df.shape}")
    print(f"   Колонки: {bollinger_df.columns}")
    
    # Тест 3: RSI DataFrame
    rsi = RSIData()
    rsi.rsi_values = [45.5, 52.3, 48.7, 55.1]
    rsi.timeperiod = 14
    
    rsi_df = cpp_data_processor.DataConverter.to_polars_rsi(rsi)
    print(f"✅ RSI DataFrame: {rsi_df.shape}")
    print(f"   Колонки: {rsi_df.columns}")
    
    # Тест 4: Velocity DataFrame
    velocity = VelocityData()
    velocity.velocity_values = [10.5, 12.3, 8.7, 15.1]
    velocity.interval = "1m"
    
    velocity_df = cpp_data_processor.DataConverter.to_polars_velocity(velocity)
    print(f"✅ Velocity DataFrame: {velocity_df.shape}")
    print(f"   Колонки: {velocity_df.columns}")
    
    # Проверяем, что все DataFrame'ы являются Polars DataFrame'ами
    print("\n🔍 Проверяем типы данных...")
    
    assert isinstance(candles_df, pl.DataFrame), "Candles должен быть Polars DataFrame"
    assert isinstance(bollinger_df, pl.DataFrame), "Bollinger должен быть Polars DataFrame"
    assert isinstance(rsi_df, pl.DataFrame), "RSI должен быть Polars DataFrame"
    assert isinstance(velocity_df, pl.DataFrame), "Velocity должен быть Polars DataFrame"
    
    print("✅ Все DataFrame'ы являются Polars DataFrame'ами")
    
    # Проверяем содержимое DataFrame'ов
    print("\n📋 Проверяем содержимое DataFrame'ов...")
    
    print("Candles DataFrame:")
    print(candles_df.head())
    
    print("\nBollinger DataFrame:")
    print(bollinger_df.head())
    
    print("\nRSI DataFrame:")
    print(rsi_df.head())
    
    print("\nVelocity DataFrame:")
    print(velocity_df.head())
    
    print("\n🎉 Все тесты с Polars DataFrame'ами прошли успешно!")
    return True

if __name__ == "__main__":
    success = test_polars_dataframes()
    if success:
        print("\n✅ C++ модуль корректно работает с Polars DataFrame'ами!")
        sys.exit(0)
    else:
        print("\n❌ Обнаружены проблемы с Polars DataFrame'ами")
        sys.exit(1)
