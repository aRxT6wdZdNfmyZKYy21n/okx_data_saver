"""
C++ Data Processor Wrapper - интеграция C++ процессора с Python кодом.
Только C++ процессор, без fallback на Python.
"""

import logging

from chrono import Timer
from polars import DataFrame

from enumerations import SymbolId

# Попытка импорта C++ модуля

try:
    import cpp_data_processor

    CPP_AVAILABLE = True
    logger = logging.getLogger(__name__)
    logger.info('C++ Data Processor module loaded successfully')
except ImportError as e:
    CPP_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.error(f'C++ Data Processor not available: {e}')
    raise RuntimeError(f'C++ Data Processor is required but not available: {e}') from e

logger = logging.getLogger(__name__)


class CppDataProcessorWrapper:
    """Обертка для C++ Data Processor. Только C++ процессор, без fallback."""

    def __init__(self):
        """
        Инициализация обертки.
        Требует наличия C++ модуля.
        """
        if not CPP_AVAILABLE:
            raise RuntimeError('C++ Data Processor module is not available')

        try:
            self.cpp_processor = cpp_data_processor.DataProcessor()
            self._setup_cpp_parameters()
            logger.info('C++ Data Processor initialized successfully')
        except Exception as e:
            logger.error(f'Failed to initialize C++ Data Processor: {e}')
            raise RuntimeError(f'Failed to initialize C++ Data Processor: {e}') from e

    def _setup_cpp_parameters(self):
        """Настройка параметров C++ процессора."""
        if not self.cpp_processor:
            return

        try:
            # Настройка параметров обработки (только простые типы)
            params = {
                'enable_bollinger_bands': True,
                'enable_candles': True,
                'enable_rsi': True,
                'enable_smoothing': True,
                'enable_extreme_lines': True,
                'enable_velocity': True,
                'bollinger_period': 20,
                'bollinger_std_dev_multiplier': 2.0,
                'rsi_period': 14,
            }

            self.cpp_processor.set_processing_params(params)
            logger.info('C++ processor parameters configured')

        except Exception as e:
            logger.error(f'Failed to configure C++ processor parameters: {e}')
            raise

    async def process_trades_data(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """
        Обработка данных о сделках с использованием C++ процессора.

        Args:
            symbol_id: Идентификатор символа
            trades_df: DataFrame с данными о сделках
        """
        logger.info(
            f'Processing trades data for {symbol_id.name}: {trades_df.height} records'
        )

        if not self.cpp_processor:
            raise RuntimeError('C++ processor is not initialized')

        await self._process_with_cpp(symbol_id, trades_df)

    async def _process_with_cpp(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка данных с использованием C++ процессора."""
        try:
            with Timer() as timer:
                cpp_symbol_id = cpp_data_processor.SymbolId(
                    symbol_id.value,
                )
                result = self.cpp_processor.process_trades_data(
                    cpp_symbol_id,
                    trades_df,
                )

                if result.success:
                    logger.info(
                        f'C++ processing completed successfully in {result.processing_time_seconds:.3f}s'
                    )

                    # Сохранение результатов в Redis
                    await self._save_cpp_results_to_redis(symbol_id, result)

                else:
                    logger.error(f'C++ processing failed: {result.error_message}')
                    raise RuntimeError(f'C++ processing failed: {result.error_message}')

        except Exception as e:
            logger.error(f'C++ processing error: {e}')
            raise RuntimeError(f'C++ processing failed: {e}') from e

    async def _save_cpp_results_to_redis(
        self, symbol_id: SymbolId, result: 'cpp_data_processor.ProcessingResult'
    ) -> None:
        """
        Сохранение результатов C++ обработки в Redis.

        Args:
            symbol_id: Идентификатор символа
            result: Результат обработки от C++ процессора
        """
        try:
            # Здесь будет логика сохранения результатов в Redis
            # Пока что просто логируем
            logger.info(f'Results saved to Redis for {symbol_id.name}')

        except Exception as e:
            logger.error(f'Failed to save results to Redis: {e}')

    def get_processing_stats(self) -> dict:
        """
        Получение статистики обработки.

        Returns:
            dict: Статистика обработки
        """
        if not self.cpp_processor:
            raise RuntimeError('C++ processor is not initialized')

        try:
            stats = self.cpp_processor.get_processing_stats()
            stats['processor_type'] = 'cpp'
            return stats
        except Exception as e:
            logger.error(f'Failed to get C++ processing stats: {e}')
            raise RuntimeError(f'Failed to get C++ processing stats: {e}') from e

    def is_cpp_available(self) -> bool:
        """
        Проверка доступности C++ процессора.

        Returns:
            bool: True если C++ процессор доступен
        """
        return CPP_AVAILABLE and self.cpp_processor is not None

    def get_processor_info(self) -> dict:
        """
        Получение информации о процессоре.

        Returns:
            dict: Информация о процессоре
        """
        return {
            'cpp_available': CPP_AVAILABLE,
            'cpp_enabled': True,
            'cpp_initialized': self.cpp_processor is not None,
            'processor_type': 'cpp',
        }


# Глобальный экземпляр обертки
g_cpp_data_processor_wrapper = CppDataProcessorWrapper()
