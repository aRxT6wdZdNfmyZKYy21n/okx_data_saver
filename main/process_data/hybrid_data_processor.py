"""
C++ Data Processor - процессор данных, использующий только C++.
Упрощенная версия без fallback на Python.
"""

import logging
import typing
from datetime import UTC, datetime

import numpy
import polars
import polars_talib
import talib
from chrono import Timer
from polars import DataFrame

from constants.common import CommonConstants
from constants.plot import PlotConstants
from enumerations import SymbolId
from main.process_data.redis_service import g_redis_data_service
from main.process_data.cpp_data_processor_wrapper import g_cpp_data_processor_wrapper

logger = logging.getLogger(__name__)


class CppDataProcessor:
    """
    C++ процессор данных для высокопроизводительной обработки.
    Использует только C++ без fallback на Python.
    """

    def __init__(self):
        """
        Инициализация C++ процессора.
        """
        self.cpp_wrapper = g_cpp_data_processor_wrapper
        
        # Статистика обработки
        self.stats = {
            'total_processed': 0,
            'total_time_ms': 0.0,
            'total_trades_processed': 0,
            'successful_operations': 0,
            'failed_operations': 0
        }

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
            f'C++ processing trades data for {symbol_id.name}: {trades_df.height} records'
        )

        start_time = datetime.now(UTC)
        
        try:
            # Обработка данных через C++
            await self.cpp_wrapper.process_trades_data(symbol_id, trades_df)
            
            # Обновление статистики
            processing_time = (datetime.now(UTC) - start_time).total_seconds() * 1000
            self.stats['total_processed'] += 1
            self.stats['total_time_ms'] += processing_time
            self.stats['total_trades_processed'] += trades_df.height
            self.stats['successful_operations'] += 1
            
            logger.info(f"C++ processing completed in {processing_time:.3f}ms")
            
        except Exception as e:
            logger.error(f"C++ processing failed: {e}")
            self.stats['failed_operations'] += 1
            raise RuntimeError(f"C++ processing failed: {e}") from e

    def get_processing_stats(self) -> dict:
        """
        Получение статистики обработки.
        
        Returns:
            dict: Статистика обработки
        """
        base_stats = self.stats.copy()
        
        # Добавление статистики от C++ процессора
        if self.cpp_wrapper.is_cpp_available():
            cpp_stats = self.cpp_wrapper.get_processing_stats()
            base_stats.update(cpp_stats)
        
        # Расчет средних значений
        if self.stats['total_processed'] > 0:
            base_stats['average_time_ms'] = self.stats['total_time_ms'] / self.stats['total_processed']
            base_stats['average_trades_per_operation'] = self.stats['total_trades_processed'] / self.stats['total_processed']
            base_stats['success_rate'] = (self.stats['successful_operations'] / self.stats['total_processed']) * 100
        
        base_stats['processor_type'] = 'cpp'
        
        return base_stats

    def get_processor_info(self) -> dict:
        """
        Получение информации о процессоре.
        
        Returns:
            dict: Информация о процессоре
        """
        info = {
            'processor_type': 'cpp',
            'cpp_wrapper_info': self.cpp_wrapper.get_processor_info()
        }
        
        return info

    def reset_stats(self) -> None:
        """Сброс статистики обработки."""
        self.stats = {
            'total_processed': 0,
            'total_time_ms': 0.0,
            'total_trades_processed': 0,
            'successful_operations': 0,
            'failed_operations': 0
        }
        logger.info("Processing statistics reset")


# Глобальный экземпляр C++ процессора
g_cpp_data_processor = CppDataProcessor()
