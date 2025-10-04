"""
Тесты интеграции processor.py с Redis.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from enumerations import SymbolId
from main.show_plot.processor import FinPlotChartProcessor
from main.show_plot.redis_data_adapter import RedisDataAdapter


class TestProcessorRedisIntegration:
    """Тесты интеграции processor.py с Redis."""

    @pytest.fixture
    def mock_redis_adapter(self):
        """Фикстура для мок-адаптера Redis."""
        adapter = AsyncMock(spec=RedisDataAdapter)

        # Настраиваем возвращаемые значения
        adapter.load_trades_dataframe.return_value = None
        adapter.load_bollinger_data.return_value = (None, None, None)
        adapter.load_candle_dataframe.return_value = None
        adapter.load_rsi_data.return_value = None
        adapter.load_smoothed_dataframe.return_value = None
        adapter.load_extreme_lines_data.return_value = (None, None, None)
        adapter.load_order_book_volumes_data.return_value = (None, None, None, None)
        adapter.load_velocity_data.return_value = None
        adapter.load_available_symbols.return_value = None

        return adapter

    @pytest.fixture
    def processor(self, mock_redis_adapter):
        """Фикстура для процессора с мок-адаптером."""
        processor = FinPlotChartProcessor()

        # Заменяем глобальный адаптер на мок
        import main.show_plot.processor as processor_module

        original_adapter = processor_module.g_redis_data_adapter
        processor_module.g_redis_data_adapter = mock_redis_adapter

        yield processor

        # Восстанавливаем оригинальный адаптер
        processor_module.g_redis_data_adapter = original_adapter

    @pytest.mark.asyncio
    async def test_load_trades_dataframe_from_redis(
        self, processor, mock_redis_adapter
    ):
        """Тест загрузки данных о сделках из Redis."""
        # Настраиваем мок
        mock_redis_adapter.load_trades_dataframe.return_value = None

        # Устанавливаем текущий символ
        processor._FinPlotChartProcessor__current_symbol_name = 'BTC-USDT'

        # Вызываем метод
        await processor._FinPlotChartProcessor__update_trades_dataframe()

        # Проверяем, что адаптер был вызван
        mock_redis_adapter.load_trades_dataframe.assert_called_once()

        # Проверяем, что был передан правильный symbol_id
        call_args = mock_redis_adapter.load_trades_dataframe.call_args[0]
        assert isinstance(call_args[0], SymbolId)

    @pytest.mark.asyncio
    async def test_load_available_symbols_from_redis(
        self, processor, mock_redis_adapter
    ):
        """Тест загрузки доступных символов из Redis."""
        # Настраиваем мок
        mock_symbols = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT']
        mock_redis_adapter.load_available_symbols.return_value = mock_symbols

        # Вызываем метод
        await (
            processor._FinPlotChartProcessor__update_current_available_symbol_name_set()
        )

        # Проверяем, что адаптер был вызван
        mock_redis_adapter.load_available_symbols.assert_called_once()

        # Проверяем, что символы были установлены
        assert (
            processor._FinPlotChartProcessor__current_available_symbol_name_set
            == set(mock_symbols)
        )

    @pytest.mark.asyncio
    async def test_fallback_to_database_when_redis_unavailable(
        self, processor, mock_redis_adapter
    ):
        """Тест fallback на базу данных когда Redis недоступен."""
        # Настраиваем мок для возврата None (Redis недоступен)
        mock_redis_adapter.load_available_symbols.return_value = None

        # Мокаем глобальные объекты для базы данных
        import main.show_plot.processor as processor_module

        original_globals = processor_module.g_globals
        processor_module.g_globals = MagicMock()

        # Мокаем сессию базы данных
        mock_session = AsyncMock()
        mock_session.execute.return_value = [
            MagicMock(symbol_id='BTC_USDT'),
            MagicMock(symbol_id='ETH_USDT'),
        ]

        mock_session_maker = AsyncMock()
        mock_session_maker.return_value.__aenter__.return_value = mock_session
        processor_module.g_globals.get_postgres_db_session_maker.return_value = (
            mock_session_maker
        )

        try:
            # Вызываем метод
            await processor._FinPlotChartProcessor__update_current_available_symbol_name_set()

            # Проверяем, что был вызван fallback на базу данных
            mock_redis_adapter.load_available_symbols.assert_called_once()
            processor_module.g_globals.get_postgres_db_session_maker.assert_called_once()

        finally:
            # Восстанавливаем оригинальные глобальные объекты
            processor_module.g_globals = original_globals

    @pytest.mark.asyncio
    async def test_load_all_derived_data_from_redis(
        self, processor, mock_redis_adapter
    ):
        """Тест загрузки всех производных данных из Redis."""
        symbol_id = SymbolId.BTC_USDT

        # Вызываем метод
        await processor._FinPlotChartProcessor__load_all_derived_data_from_redis(
            symbol_id
        )

        # Проверяем, что все методы адаптера были вызваны
        mock_redis_adapter.load_bollinger_data.assert_called_once_with(symbol_id)
        mock_redis_adapter.load_candle_dataframe.assert_called()
        mock_redis_adapter.load_rsi_data.assert_called_once_with(symbol_id)
        mock_redis_adapter.load_smoothed_dataframe.assert_called()
        mock_redis_adapter.load_extreme_lines_data.assert_called_once_with(symbol_id)
        mock_redis_adapter.load_order_book_volumes_data.assert_called_once_with(
            symbol_id
        )
        mock_redis_adapter.load_velocity_data.assert_called_once_with(symbol_id)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
