"""
Hybrid Data Processor - комбинированный процессор, использующий C++ и Python.
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
from main.process_data.cpp_data_processor_wrapper import get_cpp_data_processor

logger = logging.getLogger(__name__)


class HybridDataProcessor:
    """
    Гибридный процессор данных, использующий C++ для основных вычислений
    и Python для дополнительной обработки и интеграции с Redis.
    """

    def __init__(self, prefer_cpp: bool = True):
        """
        Инициализация гибридного процессора.
        
        Args:
            prefer_cpp: Предпочитать C++ процессор для основных вычислений
        """
        self.prefer_cpp = prefer_cpp
        self.cpp_wrapper = get_cpp_data_processor()
        
        # Статистика обработки
        self.stats = {
            'total_processed': 0,
            'cpp_processed': 0,
            'python_processed': 0,
            'total_time_ms': 0.0,
            'cpp_time_ms': 0.0,
            'python_time_ms': 0.0
        }

    async def process_trades_data(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """
        Обработка данных о сделках с использованием гибридного подхода.
        
        Args:
            symbol_id: Идентификатор символа
            trades_df: DataFrame с данными о сделках
        """
        logger.info(
            f'Hybrid processing trades data for {symbol_id.name}: {trades_df.height} records'
        )

        start_time = datetime.now(UTC)
        
        try:
            if self.prefer_cpp and self.cpp_wrapper.is_cpp_available():
                await self._process_with_cpp_primary(symbol_id, trades_df)
            else:
                await self._process_with_python_primary(symbol_id, trades_df)
                
        except Exception as e:
            logger.error(f"Hybrid processing failed: {e}")
            # Fallback к Python процессору
            await self._process_with_python_fallback(symbol_id, trades_df)
            
        finally:
            # Обновление статистики
            processing_time = (datetime.now(UTC) - start_time).total_seconds() * 1000
            self.stats['total_processed'] += 1
            self.stats['total_time_ms'] += processing_time

    async def _process_with_cpp_primary(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Основная обработка с использованием C++ процессора."""
        logger.info("Using C++ processor as primary")
        
        start_time = datetime.now(UTC)
        
        try:
            # Обработка основных данных через C++
            await self.cpp_wrapper.process_trades_data(symbol_id, trades_df)
            
            # Дополнительная обработка через Python (если необходимо)
            await self._process_additional_python_data(symbol_id, trades_df)
            
            processing_time = (datetime.now(UTC) - start_time).total_seconds() * 1000
            self.stats['cpp_processed'] += 1
            self.stats['cpp_time_ms'] += processing_time
            
            logger.info(f"C++ primary processing completed in {processing_time:.3f}ms")
            
        except Exception as e:
            logger.error(f"C++ primary processing failed: {e}")
            raise

    async def _process_with_python_primary(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Основная обработка с использованием Python процессора."""
        logger.info("Using Python processor as primary")
        
        start_time = datetime.now(UTC)
        
        try:
            # Здесь будет вызов оригинального Python процессора
            # Пока что просто логируем
            logger.info("Python primary processing (not implemented yet)")
            
            processing_time = (datetime.now(UTC) - start_time).total_seconds() * 1000
            self.stats['python_processed'] += 1
            self.stats['python_time_ms'] += processing_time
            
            logger.info(f"Python primary processing completed in {processing_time:.3f}ms")
            
        except Exception as e:
            logger.error(f"Python primary processing failed: {e}")
            raise

    async def _process_with_python_fallback(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Fallback обработка с использованием Python процессора."""
        logger.info("Using Python processor as fallback")
        
        start_time = datetime.now(UTC)
        
        try:
            # Здесь будет вызов оригинального Python процессора
            # Пока что просто логируем
            logger.info("Python fallback processing (not implemented yet)")
            
            processing_time = (datetime.now(UTC) - start_time).total_seconds() * 1000
            self.stats['python_processed'] += 1
            self.stats['python_time_ms'] += processing_time
            
        except Exception as e:
            logger.error(f"Python fallback processing failed: {e}")
            raise

    async def _process_additional_python_data(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """
        Дополнительная обработка данных через Python.
        Используется для задач, которые лучше выполнять в Python.
        """
        logger.info("Processing additional Python data")
        
        # Здесь можно добавить дополнительную обработку:
        # - Сложные аналитические вычисления
        # - Интеграция с внешними API
        # - Специфичные для Python библиотеки
        # - Дополнительная валидация данных
        
        pass

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
            base_stats['cpp_usage_percentage'] = (self.stats['cpp_processed'] / self.stats['total_processed']) * 100
            base_stats['python_usage_percentage'] = (self.stats['python_processed'] / self.stats['total_processed']) * 100
        
        return base_stats

    def get_processor_info(self) -> dict:
        """
        Получение информации о процессоре.
        
        Returns:
            dict: Информация о процессоре
        """
        info = {
            'processor_type': 'hybrid',
            'prefer_cpp': self.prefer_cpp,
            'cpp_wrapper_info': self.cpp_wrapper.get_processor_info()
        }
        
        return info

    def set_prefer_cpp(self, prefer_cpp: bool) -> None:
        """
        Установка предпочтения C++ процессора.
        
        Args:
            prefer_cpp: Предпочитать C++ процессор
        """
        self.prefer_cpp = prefer_cpp
        logger.info(f"Set prefer_cpp to {prefer_cpp}")

    def reset_stats(self) -> None:
        """Сброс статистики обработки."""
        self.stats = {
            'total_processed': 0,
            'cpp_processed': 0,
            'python_processed': 0,
            'total_time_ms': 0.0,
            'cpp_time_ms': 0.0,
            'python_time_ms': 0.0
        }
        logger.info("Processing statistics reset")


# Глобальный экземпляр гибридного процессора
g_hybrid_data_processor = HybridDataProcessor()


def get_hybrid_data_processor() -> HybridDataProcessor:
    """
    Получение глобального экземпляра гибридного data processor.
    
    Returns:
        HybridDataProcessor: Экземпляр гибридного процессора
    """
    return g_hybrid_data_processor
