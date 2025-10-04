"""
Тесты для проверки миграции данных в Redis.
"""

import logging
from datetime import UTC, datetime

import polars
import pytest

from enumerations.compression import CompressionAlgorithm
from main.process_data.data_processor import g_data_processor
from main.process_data.redis_service import g_redis_data_service
from main.show_plot.redis_data_adapter import g_redis_data_adapter
from utils.redis import g_redis_manager
from utils.serialization import (
    deserialize_dataframe,
    merge_dataframe_chunks,
    serialize_dataframe,
    split_dataframe_by_size,
)

logger = logging.getLogger(__name__)


class TestRedisMigration:
    """Тесты для миграции данных в Redis."""

    @pytest.fixture
    async def redis_connection(self):
        """Фикстура для подключения к Redis."""
        await g_redis_manager.connect()
        yield g_redis_manager
        await g_redis_manager.disconnect()

    def test_serialization_roundtrip(self):
        """Тест сериализации и десериализации DataFrame."""
        # Создаем тестовый DataFrame
        df = polars.DataFrame(
            {
                'trade_id': [1, 2, 3, 4, 5],
                'price': [100.0, 101.0, 102.0, 103.0, 104.0],
                'quantity': [1.0, 2.0, 3.0, 4.0, 5.0],
                'datetime': [
                    datetime.now(UTC),
                    datetime.now(UTC),
                    datetime.now(UTC),
                    datetime.now(UTC),
                    datetime.now(UTC),
                ],
            }
        )

        # Тестируем сериализацию
        serialized = serialize_dataframe(df, compression=CompressionAlgorithm.XZ)
        assert isinstance(serialized, bytes)
        assert len(serialized) > 0

        # Тестируем десериализацию
        deserialized = deserialize_dataframe(
            serialized, compression=CompressionAlgorithm.XZ
        )
        assert deserialized.equals(df)

    def test_dataframe_splitting(self):
        """Тест разбивки DataFrame на части."""
        # Создаем большой DataFrame
        large_df = polars.DataFrame(
            {
                'trade_id': list(range(10000)),
                'price': [100.0 + i * 0.01 for i in range(10000)],
                'quantity': [1.0 + i * 0.1 for i in range(10000)],
                'datetime': [datetime.now(UTC) for _ in range(10000)],
            }
        )

        # Разбиваем на части
        chunks = split_dataframe_by_size(
            large_df, max_size_bytes=1000
        )  # Маленький размер для теста
        assert len(chunks) > 1

        # Проверяем, что все части помещаются в лимит
        for chunk in chunks:
            serialized = serialize_dataframe(chunk, compression=CompressionAlgorithm.XZ)
            assert len(serialized) <= 1000

        # Восстанавливаем DataFrame
        merged = merge_dataframe_chunks(chunks)
        assert merged.height == large_df.height
        assert merged.equals(large_df)

    async def test_redis_data_service(self, redis_connection):
        """Тест сервиса Redis."""
        # Создаем тестовый DataFrame
        df = polars.DataFrame(
            {
                'trade_id': [1, 2, 3],
                'price': [100.0, 101.0, 102.0],
                'quantity': [1.0, 2.0, 3.0],
            }
        )

        symbol_id = 'test_symbol'

        # Сохраняем данные
        await g_redis_data_service.save_trades_data(
            symbol_id=symbol_id,
            trades_df=df,
            min_trade_id=1,
            max_trade_id=3,
            min_price=100.0,
            max_price=102.0,
        )

        # Загружаем данные
        loaded_df = await g_redis_data_service.load_trades_data(symbol_id)
        assert loaded_df is not None
        assert loaded_df.equals(df)

    async def test_redis_data_adapter(self, redis_connection):
        """Тест адаптера Redis."""
        # Создаем тестовые данные
        df = polars.DataFrame(
            {
                'trade_id': [1, 2, 3],
                'price': [100.0, 101.0, 102.0],
                'quantity': [1.0, 2.0, 3.0],
            }
        )

        symbol_id = 'test_symbol'

        # Сохраняем через сервис
        await g_redis_data_service.save_trades_data(
            symbol_id=symbol_id,
            trades_df=df,
            min_trade_id=1,
            max_trade_id=3,
            min_price=100.0,
            max_price=102.0,
        )

        # Загружаем через адаптер
        loaded_df = await g_redis_data_adapter.load_trades_dataframe(symbol_id)
        assert loaded_df is not None
        assert loaded_df.equals(df)

    async def test_data_processor(self, redis_connection):
        """Тест процессора данных."""
        # Создаем тестовый DataFrame
        df = polars.DataFrame(
            {
                'trade_id': [1, 2, 3, 4, 5],
                'price': [100.0, 101.0, 102.0, 103.0, 104.0],
                'quantity': [1.0, 2.0, 3.0, 4.0, 5.0],
                'datetime': [datetime.now(UTC) for _ in range(5)],
            }
        )

        symbol_id = 'test_symbol'

        # Обрабатываем данные
        await g_data_processor.process_trades_data(symbol_id, df)

        # Проверяем, что данные сохранились
        trades_df = await g_redis_data_service.load_trades_data(symbol_id)
        assert trades_df is not None
        assert trades_df.height == df.height

    async def test_compression_efficiency(self):
        """Тест эффективности сжатия."""
        # Создаем DataFrame с повторяющимися данными (хорошо сжимается)
        df = polars.DataFrame(
            {
                'trade_id': list(range(1000)),
                'price': [100.0] * 1000,  # Одинаковые значения
                'quantity': [1.0] * 1000,  # Одинаковые значения
                'datetime': [datetime.now(UTC)] * 1000,  # Одинаковые значения
            }
        )

        # Тестируем разные алгоритмы сжатия
        xz_compressed = serialize_dataframe(df, compression=CompressionAlgorithm.XZ)
        lz4_compressed = serialize_dataframe(df, compression=CompressionAlgorithm.LZ4)
        none_compressed = serialize_dataframe(df, compression=CompressionAlgorithm.NONE)

        # xz должен сжимать лучше всего
        assert len(xz_compressed) < len(lz4_compressed)
        assert len(lz4_compressed) < len(none_compressed)

        # Проверяем, что десериализация работает
        xz_df = deserialize_dataframe(
            xz_compressed, compression=CompressionAlgorithm.XZ
        )
        lz4_df = deserialize_dataframe(
            lz4_compressed, compression=CompressionAlgorithm.LZ4
        )
        none_df = deserialize_dataframe(
            none_compressed, compression=CompressionAlgorithm.NONE
        )

        assert xz_df.equals(df)
        assert lz4_df.equals(df)
        assert none_df.equals(df)


if __name__ == '__main__':
    # Запуск тестов
    pytest.main([__file__, '-v'])
