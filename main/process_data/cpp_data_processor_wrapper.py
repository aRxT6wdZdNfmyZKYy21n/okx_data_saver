"""
C++ Data Processor Wrapper - интеграция C++ процессора с Python кодом.
"""

import logging
import typing
from datetime import UTC, datetime
from typing import Optional

import numpy
import polars
from chrono import Timer
from polars import DataFrame

from constants.common import CommonConstants
from constants.plot import PlotConstants
from enumerations import SymbolId
from main.process_data.redis_service import g_redis_data_service

# Попытка импорта C++ модуля
try:
    import cpp_data_processor
    CPP_AVAILABLE = True
    logger = logging.getLogger(__name__)
    logger.info("C++ Data Processor module loaded successfully")
except ImportError as e:
    CPP_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning(f"C++ Data Processor not available: {e}")
    cpp_data_processor = None

logger = logging.getLogger(__name__)


class CppDataProcessorWrapper:
    """Обертка для C++ Data Processor с fallback на Python версию."""

    def __init__(self, enable_cpp: bool = True):
        """
        Инициализация обертки.
        
        Args:
            enable_cpp: Включить использование C++ процессора
        """
        self.enable_cpp = enable_cpp and CPP_AVAILABLE
        self.cpp_processor = None
        self.fallback_processor = None
        
        if self.enable_cpp:
            try:
                self.cpp_processor = cpp_data_processor.DataProcessor()
                self._setup_cpp_parameters()
                logger.info("C++ Data Processor initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize C++ Data Processor: {e}")
                self.enable_cpp = False
                self.cpp_processor = None
        else:
            logger.info("Using Python fallback processor")

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
                'enable_order_book_volumes': True,
                'enable_velocity': True,
                'bollinger_period': 20,
                'bollinger_std_dev_multiplier': 2.0,
                'rsi_period': 14
            }
            
            self.cpp_processor.set_processing_params(params)
            logger.info("C++ processor parameters configured")
            
        except Exception as e:
            logger.error(f"Failed to configure C++ processor parameters: {e}")

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

        if self.enable_cpp and self.cpp_processor:
            await self._process_with_cpp(symbol_id, trades_df)
        else:
            await self._process_with_python_fallback(symbol_id, trades_df)

    async def _process_with_cpp(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка данных с использованием C++ процессора."""
        try:
            with Timer() as timer:
                # Конвертация Polars DataFrame в формат для C++
                trades_data = self._convert_polars_to_cpp_format(trades_df)
                
                # Обработка данных
                result = self.cpp_processor.process_trades_data(symbol_id, trades_data)
                
                if result.success:
                    logger.info(
                        f"C++ processing completed successfully in {result.processing_time_seconds:.3f}s"
                    )
                    
                    # Сохранение результатов в Redis
                    await self._save_cpp_results_to_redis(symbol_id, result)
                    
                else:
                    logger.error(f"C++ processing failed: {result.error_message}")
                    # Fallback к Python процессору
                    await self._process_with_python_fallback(symbol_id, trades_df)
                    
        except Exception as e:
            logger.error(f"C++ processing error: {e}")
            # Fallback к Python процессору
            await self._process_with_python_fallback(symbol_id, trades_df)

    async def _process_with_python_fallback(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Fallback обработка с использованием Python процессора."""
        logger.info("Using Python fallback processor")
        
        # Здесь будет вызов оригинального Python процессора
        # Пока что просто логируем
        logger.info("Python fallback processing (not implemented yet)")

    def _convert_polars_to_cpp_format(self, trades_df: DataFrame) -> dict:
        """
        Конвертация Polars DataFrame в формат для C++ процессора.
        
        Args:
            trades_df: Polars DataFrame с данными о сделках
            
        Returns:
            dict: Данные в формате для C++ процессора
        """
        # Конвертация datetime в timestamp
        datetime_series = trades_df['datetime'].cast(polars.Int64) // 1000  # Convert to seconds
        
        return {
            'trade_id': trades_df['trade_id'].to_list(),
            'price': trades_df['price'].to_list(),
            'quantity': trades_df['quantity'].to_list(),
            'is_buy': trades_df['is_buy'].to_list(),
            'datetime': datetime_series.to_list()
        }

    async def _save_cpp_results_to_redis(
        self,
        symbol_id: SymbolId,
        result: 'cpp_data_processor.ProcessingResult'
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
            logger.info(f"Results saved to Redis for {symbol_id.name}")
            
        except Exception as e:
            logger.error(f"Failed to save results to Redis: {e}")

    def get_processing_stats(self) -> dict:
        """
        Получение статистики обработки.
        
        Returns:
            dict: Статистика обработки
        """
        if self.enable_cpp and self.cpp_processor:
            try:
                stats = self.cpp_processor.get_processing_stats()
                stats['processor_type'] = 'cpp'
                return stats
            except Exception as e:
                logger.error(f"Failed to get C++ processing stats: {e}")
                return {'processor_type': 'cpp', 'error': str(e)}
        else:
            return {'processor_type': 'python_fallback'}

    def is_cpp_available(self) -> bool:
        """
        Проверка доступности C++ процессора.
        
        Returns:
            bool: True если C++ процессор доступен
        """
        return self.enable_cpp and self.cpp_processor is not None

    def get_processor_info(self) -> dict:
        """
        Получение информации о процессоре.
        
        Returns:
            dict: Информация о процессоре
        """
        return {
            'cpp_available': CPP_AVAILABLE,
            'cpp_enabled': self.enable_cpp,
            'cpp_initialized': self.cpp_processor is not None,
            'processor_type': 'cpp' if self.is_cpp_available() else 'python_fallback'
        }


# Глобальный экземпляр обертки
g_cpp_data_processor_wrapper = CppDataProcessorWrapper()


def get_cpp_data_processor() -> CppDataProcessorWrapper:
    """
    Получение глобального экземпляра C++ data processor wrapper.
    
    Returns:
        CppDataProcessorWrapper: Экземпляр обертки
    """
    return g_cpp_data_processor_wrapper
