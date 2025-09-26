"""
Адаптер для загрузки данных из Redis в FinPlotChartProcessor.
"""

import logging
from typing import Any

import polars
from polars import DataFrame, Series

from constants.symbol import SymbolConstants
from enumerations import SymbolId
from main.process_data.redis_service import g_redis_data_service

logger = logging.getLogger(__name__)


class RedisDataAdapter:
    """Адаптер для загрузки данных из Redis."""

    def __init__(self):
        self.redis_service = g_redis_data_service

    async def load_trades_dataframe(
        self, symbol_id: SymbolId
    ) -> DataFrame | None:
        """Загрузка данных о сделках из Redis."""
        try:
            symbol_id_str = symbol_id.name
            trades_df = await self.redis_service.load_trades_data(symbol_id_str)
            
            if trades_df is None:
                logger.warning(f"No trades data found in Redis for {symbol_id_str}")
                return None
                
            logger.info(f"Loaded trades data from Redis for {symbol_id_str}: {trades_df.height} records")
            return trades_df
            
        except Exception as e:
            logger.error(f"Error loading trades data from Redis for {symbol_id.name}: {e}")
            return None

    async def load_bollinger_data(
        self, symbol_id: SymbolId
    ) -> tuple[Series | None, Series | None, Series | None]:
        """Загрузка полос Боллинджера из Redis."""
        try:
            symbol_id_str = symbol_id.name
            bollinger_df = await self.redis_service.load_bollinger_data(symbol_id_str)
            
            if bollinger_df is None:
                logger.warning(f"No bollinger data found in Redis for {symbol_id_str}")
                return None, None, None
                
            upper_band = bollinger_df.get_column('upper_band')
            middle_band = bollinger_df.get_column('middle_band')
            lower_band = bollinger_df.get_column('lower_band')
            
            logger.info(f"Loaded bollinger data from Redis for {symbol_id_str}")
            return upper_band, middle_band, lower_band
            
        except Exception as e:
            logger.error(f"Error loading bollinger data from Redis for {symbol_id.name}: {e}")
            return None, None, None

    async def load_candle_dataframe(
        self, symbol_id: SymbolId, interval: str
    ) -> DataFrame | None:
        """Загрузка свечных данных из Redis."""
        try:
            symbol_id_str = symbol_id.name
            candles_df = await self.redis_service.load_candles_data(symbol_id_str, interval)
            
            if candles_df is None:
                logger.warning(f"No candles data found in Redis for {symbol_id_str}:{interval}")
                return None
                
            logger.info(f"Loaded candles data from Redis for {symbol_id_str}:{interval}: {candles_df.height} records")
            return candles_df
            
        except Exception as e:
            logger.error(f"Error loading candles data from Redis for {symbol_id.name}:{interval}: {e}")
            return None

    async def load_rsi_data(self, symbol_id: SymbolId) -> Series | None:
        """Загрузка RSI данных из Redis."""
        try:
            symbol_id_str = symbol_id.name
            rsi_df = await self.redis_service.load_rsi_data(symbol_id_str)
            
            if rsi_df is None:
                logger.warning(f"No RSI data found in Redis for {symbol_id_str}")
                return None
                
            rsi_series = rsi_df.get_column('rsi')
            logger.info(f"Loaded RSI data from Redis for {symbol_id_str}")
            return rsi_series
            
        except Exception as e:
            logger.error(f"Error loading RSI data from Redis for {symbol_id.name}: {e}")
            return None

    async def load_smoothed_dataframe(
        self, symbol_id: SymbolId, level: str
    ) -> DataFrame | None:
        """Загрузка сглаженных данных из Redis."""
        try:
            symbol_id_str = symbol_id.name
            smoothed_df = await self.redis_service.load_smoothed_data(symbol_id_str, level)
            
            if smoothed_df is None:
                logger.warning(f"No smoothed data found in Redis for {symbol_id_str}:{level}")
                return None
                
            logger.info(f"Loaded smoothed data from Redis for {symbol_id_str}:{level}: {smoothed_df.height} records")
            return smoothed_df
            
        except Exception as e:
            logger.error(f"Error loading smoothed data from Redis for {symbol_id.name}:{level}: {e}")
            return None

    async def load_extreme_lines_data(
        self, symbol_id: SymbolId
    ) -> tuple[Any | None, tuple[float, float] | None, float | None]:
        """Загрузка экстремальных линий из Redis."""
        try:
            symbol_id_str = symbol_id.name
            extreme_lines_array = await self.redis_service.load_extreme_lines_data(symbol_id_str)
            
            if extreme_lines_array is None:
                logger.warning(f"No extreme lines data found in Redis for {symbol_id_str}")
                return None, None, None
                
            # Получаем метаданные для позиции и масштаба
            metadata = await self.redis_service.load_symbol_metadata(symbol_id_str)
            if metadata:
                # Для экстремальных линий нужны дополнительные метаданные
                # Пока возвращаем заглушки
                position = (0.0, 0.0)
                scale = 1.0
            else:
                position = None
                scale = None
                
            logger.info(f"Loaded extreme lines data from Redis for {symbol_id_str}")
            return extreme_lines_array, position, scale
            
        except Exception as e:
            logger.error(f"Error loading extreme lines data from Redis for {symbol_id.name}: {e}")
            return None, None, None

    async def load_order_book_volumes_data(
        self, symbol_id: SymbolId
    ) -> tuple[Any | None, Any | None, tuple[float, float] | None, float | None]:
        """Загрузка объемов стакана из Redis."""
        try:
            symbol_id_str = symbol_id.name
            asks_array, bids_array = await self.redis_service.load_order_book_volumes_data(symbol_id_str)
            
            if asks_array is None or bids_array is None:
                logger.warning(f"No order book volumes data found in Redis for {symbol_id_str}")
                return None, None, None, None
                
            # Получаем метаданные для позиции и масштаба
            metadata = await self.redis_service.load_symbol_metadata(symbol_id_str)
            if metadata:
                # Для объемов стакана нужны дополнительные метаданные
                # Пока возвращаем заглушки
                position = (0.0, 0.0)
                scale = 1.0
            else:
                position = None
                scale = None
                
            logger.info(f"Loaded order book volumes data from Redis for {symbol_id_str}")
            return asks_array, bids_array, position, scale
            
        except Exception as e:
            logger.error(f"Error loading order book volumes data from Redis for {symbol_id.name}: {e}")
            return None, None, None, None

    async def load_velocity_data(self, symbol_id: SymbolId) -> Series | None:
        """Загрузка данных скорости из Redis."""
        try:
            symbol_id_str = symbol_id.name
            velocity_df = await self.redis_service.load_velocity_data(symbol_id_str)
            
            if velocity_df is None:
                logger.warning(f"No velocity data found in Redis for {symbol_id_str}")
                return None
                
            velocity_series = velocity_df.get_column('velocity')
            logger.info(f"Loaded velocity data from Redis for {symbol_id_str}")
            return velocity_series
            
        except Exception as e:
            logger.error(f"Error loading velocity data from Redis for {symbol_id.name}: {e}")
            return None

    async def load_available_symbols(self) -> list[str] | None:
        """Загрузка списка доступных символов из Redis."""
        try:
            symbol_names = await self.redis_service.load_available_symbols()
            
            if not symbol_names:
                logger.warning("No available symbols found in Redis")
                return None
                
            logger.info(f"Loaded available symbols from Redis: {len(symbol_names)} symbols")
            return symbol_names
            
        except Exception as e:
            logger.error(f"Error loading available symbols from Redis: {e}")
            return None

    async def get_symbol_metadata(self, symbol_id: SymbolId) -> Any | None:
        """Получение метаданных символа из Redis."""
        try:
            symbol_id_str = symbol_id.name
            metadata = await self.redis_service.load_symbol_metadata(symbol_id_str)
            
            if metadata is None:
                logger.warning(f"No metadata found in Redis for {symbol_id_str}")
                return None
                
            logger.info(f"Loaded symbol metadata from Redis for {symbol_id_str}")
            return metadata
            
        except Exception as e:
            logger.error(f"Error loading symbol metadata from Redis for {symbol_id.name}: {e}")
            return None


# Глобальный экземпляр адаптера
g_redis_data_adapter = RedisDataAdapter()