"""
Интеграционные тесты для системы обработки данных.
"""

import asyncio
import logging
from datetime import UTC, datetime

import polars
import pytest
import pytest_asyncio

from enumerations import SymbolId
from main.process_data.data_processor import g_data_processor
from main.process_data.data_validator import g_data_validator
from main.process_data.monitoring import g_error_handler, g_system_monitor
from main.process_data.redis_service import g_redis_data_service
from main.show_plot.redis_data_adapter import g_redis_data_adapter
from utils.redis import g_redis_manager

logger = logging.getLogger(__name__)


class TestIntegration:
    """Интеграционные тесты для всей системы."""

    @pytest_asyncio.fixture
    async def redis_connection(self):
        """Фикстура для подключения к Redis."""
        await g_redis_manager.connect()
        yield g_redis_manager
        await g_redis_manager.disconnect()

    @pytest.mark.asyncio
    async def test_full_data_processing_pipeline(self, redis_connection):
        """Тест полного пайплайна обработки данных."""
        # Создаем тестовые данные
        trades_df = polars.DataFrame(
            {
                'trade_id': list(range(1, 101)),  # 100 сделок
                'price': [100.0 + i * 0.1 for i in range(100)],
                'quantity': [1.0 + i * 0.01 for i in range(100)],
                'datetime': [datetime.now(UTC) for _ in range(100)],
                'is_buy': [
                    i % 2 == 0 for i in range(100)
                ],  # Чередуем покупки и продажи
            }
        )

        symbol_id = SymbolId.BTC_USDT

        try:
            # 1. Сохраняем данные о сделках
            await g_redis_data_service.save_trades_data(
                symbol_id=symbol_id,
                trades_df=trades_df,
                min_trade_id=1,
                max_trade_id=100,
                min_price=100.0,
                max_price=110.0,
            )

            # 2. Обрабатываем все производные данные
            await g_data_processor.process_trades_data(symbol_id, trades_df)

            # 3. Загружаем данные через адаптер
            loaded_trades = await g_redis_data_adapter.load_trades_dataframe(symbol_id)
            assert loaded_trades is not None
            assert loaded_trades.height == trades_df.height

            # 4. Валидируем данные
            is_valid, errors = g_data_validator.validate_trades_data(loaded_trades)
            assert is_valid, f'Validation failed: {errors}'

            # 5. Проверяем, что все типы данных сохранены
            bollinger_data = await g_redis_data_adapter.load_bollinger_bands(symbol_id)
            assert bollinger_data is not None

            candles_data = await g_redis_data_adapter.load_candles_dataframe(
                symbol_id, '1m'
            )
            assert candles_data is not None

            rsi_data = await g_redis_data_adapter.load_rsi_series(symbol_id)
            assert rsi_data is not None

            logger.info('Full data processing pipeline test passed')

        finally:
            # Очищаем тестовые данные
            await g_redis_manager.delete_dataframe(f'trades:{symbol_id}:data')

    @pytest.mark.asyncio
    async def test_error_handling_and_monitoring(self, redis_connection):
        """Тест обработки ошибок и мониторинга."""
        # Тестируем обработку ошибок
        try:
            # Вызываем несуществующий метод для генерации ошибки
            await g_redis_data_adapter.load_trades_dataframe('nonexistent_symbol')
        except Exception as exception:
            g_error_handler.handle_error('test_operation', exception, {'test': True})

        # Проверяем статистику ошибок
        error_stats = g_error_handler.get_error_stats()
        assert 'test_operation' in error_stats['error_counts']
        assert error_stats['error_counts']['test_operation'] >= 1

        # Тестируем мониторинг системы
        health_checks = await g_system_monitor.run_health_checks()
        assert 'redis' in health_checks
        assert 'data_processing' in health_checks
        assert 'system_resources' in health_checks

        logger.info('Error handling and monitoring test passed')

    @pytest.mark.asyncio
    async def test_data_consistency_across_components(self, redis_connection):
        """Тест согласованности данных между компонентами."""
        # Создаем тестовые данные
        trades_df = polars.DataFrame(
            {
                'trade_id': list(range(1, 21)),  # 20 сделок
                'price': [100.0 + i * 0.5 for i in range(20)],
                'quantity': [1.0 + i * 0.1 for i in range(20)],
                'datetime': [datetime.now(UTC) for _ in range(20)],
                'is_buy': [i % 2 == 0 for i in range(20)],  # Чередуем покупки и продажи
            }
        )

        symbol_id = SymbolId.BTC_USDT

        try:
            # Сохраняем и обрабатываем данные
            await g_redis_data_service.save_trades_data(
                symbol_id=symbol_id,
                trades_df=trades_df,
                min_trade_id=1,
                max_trade_id=20,
                min_price=100.0,
                max_price=110.0,
            )

            await g_data_processor.process_trades_data(symbol_id, trades_df)

            # Загружаем данные через разные компоненты
            trades_from_service = await g_redis_data_service.load_trades_data(symbol_id)
            trades_from_adapter = await g_redis_data_adapter.load_trades_dataframe(
                symbol_id
            )

            # Проверяем согласованность
            assert trades_from_service is not None
            assert trades_from_adapter is not None
            assert trades_from_service.equals(trades_from_adapter)

            # Проверяем валидацию согласованности
            candles_df = await g_redis_data_service.load_candles_data(symbol_id, '1m')
            bollinger_df = await g_redis_data_service.load_bollinger_data(symbol_id)

            is_consistent, errors = g_data_validator.validate_data_consistency(
                trades_df=trades_from_service,
                candles_df=candles_df,
                bollinger_df=bollinger_df,
            )

            assert is_consistent, f'Data consistency check failed: {errors}'

            logger.info('Data consistency test passed')

        finally:
            # Очищаем тестовые данные
            await g_redis_manager.delete_dataframe(f'trades:{symbol_id}:data')

    @pytest.mark.asyncio
    async def test_performance_under_load(self, redis_connection):
        """Тест производительности под нагрузкой."""
        import time

        symbol_ids = [
            SymbolId.BTC_USDT,
            SymbolId.ETH_USDT,
            SymbolId.BNB_USDT,
            SymbolId.ADA_USDT,
            SymbolId.SOL_USDT,
            SymbolId.XRP_USDT,
            SymbolId.DOT_USDT,
            SymbolId.DOGE_USDT,
            SymbolId.AVAX_USDT,
            SymbolId.SHIB_USDT,
        ]

        try:
            # Создаем данные для нескольких символов
            start_time = time.time()

            tasks = []
            for symbol_id in symbol_ids:
                trades_df = polars.DataFrame(
                    {
                        'trade_id': list(range(1, 51)),  # 50 сделок на символ
                        'price': [100.0 + i * 0.1 for i in range(50)],
                        'quantity': [1.0 + i * 0.01 for i in range(50)],
                        'datetime': [datetime.now(UTC) for _ in range(50)],
                    }
                )

                task = g_redis_data_service.save_trades_data(
                    symbol_id=symbol_id,
                    trades_df=trades_df,
                    min_trade_id=1,
                    max_trade_id=50,
                    min_price=100.0,
                    max_price=105.0,
                )
                tasks.append(task)

            # Выполняем все операции параллельно
            await asyncio.gather(*tasks)

            processing_time = time.time() - start_time

            # Проверяем, что все данные сохранились
            for symbol_id in symbol_ids:
                data = await g_redis_data_service.load_trades_data(symbol_id)
                assert data is not None
                assert data.height == 50

            logger.info(
                f'Performance test completed in {processing_time:.3f}s for {len(symbol_ids)} symbols'
            )

            # Проверяем, что время обработки разумное (менее 10 секунд)
            assert processing_time < 10.0

        finally:
            # Очищаем тестовые данные
            for symbol_id in symbol_ids:
                await g_redis_manager.delete(f'trades:{symbol_id}:data')
                await g_redis_manager.delete(f'trades:{symbol_id}:data:metadata')

    @pytest.mark.asyncio
    async def test_redis_connection_resilience(self, redis_connection):
        """Тест устойчивости подключения к Redis."""
        # Проверяем, что подключение работает
        await g_redis_manager.ping()

        # Тестируем повторное подключение
        await g_redis_manager.disconnect()
        await g_redis_manager.connect()

        # Проверяем, что подключение восстановилось
        await g_redis_manager.ping()

        logger.info('Redis connection resilience test passed')

    @pytest.mark.asyncio
    async def test_data_validation_comprehensive(self, redis_connection):
        """Комплексный тест валидации данных."""
        # Создаем корректные данные
        valid_trades = polars.DataFrame(
            {
                'trade_id': list(range(1, 11)),
                'price': [100.0 + i * 0.1 for i in range(10)],
                'quantity': [1.0 + i * 0.01 for i in range(10)],
                'datetime': [datetime.now(UTC) for _ in range(10)],
            }
        )

        # Создаем некорректные данные
        invalid_trades = polars.DataFrame(
            {
                'trade_id': [1, 2, 2, 3],  # Дублирующиеся ID
                'price': [-100.0, 101.0, 102.0, 2_000_000.0],  # Некорректные цены
                'quantity': [1.0, 2.0, 3.0, 4.0],
                'datetime': [datetime.now(UTC) for _ in range(4)],
            }
        )

        # Тестируем валидацию корректных данных
        is_valid, errors = g_data_validator.validate_trades_data(valid_trades)
        assert is_valid, f'Valid data failed validation: {errors}'

        # Тестируем валидацию некорректных данных
        is_valid, errors = g_data_validator.validate_trades_data(invalid_trades)
        assert not is_valid, 'Invalid data passed validation'
        assert len(errors) > 0, 'No validation errors found for invalid data'

        logger.info('Comprehensive data validation test passed')


if __name__ == '__main__':
    # Запуск тестов
    pytest.main([__file__, '-v'])
