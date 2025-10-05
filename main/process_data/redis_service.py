"""
Сервис для работы с данными в Redis.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any

import numpy
import polars

from constants.redis import (
    MAX_PART_SIZE_BYTES,
    get_available_symbols_key,
    get_bollinger_key,
    get_candles_key,
    get_extreme_lines_key,
    get_lines_key,
    get_metadata_key,
    get_order_book_volumes_key,
    get_rsi_key,
    get_smoothed_key,
    get_trades_data_key,
    get_velocity_key,
)
from enumerations import (
    CompressionAlgorithm,
    SymbolId,
)
from main.process_data.schemas import (
    BollingerMetadata,
    CandlesMetadata,
    ExtremeLinesMetadata,
    LinesMetadata,
    OrderBookVolumesMetadata,
    ProcessingStatus,
    RSIMetadata,
    SmoothedMetadata,
    SymbolMetadata,
    TradesDataMetadata,
    VelocityMetadata,
)
from utils.redis import g_redis_manager

logger = logging.getLogger(__name__)


class RedisDataService:
    """Сервис для работы с данными в Redis."""

    async def save_trades_data(
        self,
        symbol_id: SymbolId,
        trades_df: polars.DataFrame,
        min_trade_id: int,
        max_trade_id: int,
        min_price: float,
        max_price: float,
    ) -> TradesDataMetadata:
        """Сохранение данных о сделках."""
        key = get_trades_data_key(
            symbol_id,
        )

        # Сохраняем DataFrame
        metadata = await g_redis_manager.save_dataframe(
            key=key,
            dataframe=trades_df,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
        )

        # Создаем полные метаданные
        trades_metadata = TradesDataMetadata(
            **metadata,
            min_trade_id=min_trade_id,
            max_trade_id=max_trade_id,
            min_price=min_price,
            max_price=max_price,
            symbol_id=symbol_id,
            last_updated=datetime.now(UTC),
        )

        # Сохраняем метаданные
        await g_redis_manager.set_metadata(
            key,
            trades_metadata.model_dump_json(),
        )

        logger.info(
            f'Saved trades data for {symbol_id.name}: {trades_metadata.total_size} bytes'
        )
        return trades_metadata

    async def load_trades_data(
        self,
        symbol_id: SymbolId,
    ) -> Any | None:  # Optional[polars.DataFrame]
        """Загрузка данных о сделках."""
        key = get_trades_data_key(
            symbol_id,
        )
        return await g_redis_manager.load_dataframe(key)

    async def load_trades_metadata(
        self,
        symbol_id: SymbolId,
    ) -> TradesDataMetadata | None:
        """Загрузка метаданных данных о сделках."""
        key = get_trades_data_key(
            symbol_id,
        )
        metadata_dict = await g_redis_manager.get_metadata(key)
        if metadata_dict:
            return TradesDataMetadata.model_validate(metadata_dict)
        return None

    async def save_bollinger_data(
        self,
        symbol_id: SymbolId,
        upper_band: Any,  # polars.Series
        middle_band: Any,  # polars.Series
        lower_band: Any,  # polars.Series
        timeperiod: int = 20,
    ) -> BollingerMetadata:
        """Сохранение полос Боллинджера."""

        key = get_bollinger_key(
            symbol_id,
        )

        # Создаем DataFrame из серий
        import polars

        bollinger_df = polars.DataFrame(
            {
                'upper_band': upper_band,
                'middle_band': middle_band,
                'lower_band': lower_band,
            }
        )

        # Сохраняем DataFrame
        metadata = await g_redis_manager.save_dataframe(
            key=key,
            dataframe=bollinger_df,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
        )

        # Создаем полные метаданные
        bollinger_metadata = BollingerMetadata(
            **metadata,
            symbol_id=symbol_id,
            timeperiod=timeperiod,
            last_updated=datetime.now(UTC),
        )

        # Сохраняем метаданные
        await g_redis_manager.set_metadata(
            key,
            bollinger_metadata.model_dump_json(),
        )

        logger.info(
            f'Saved bollinger data for {symbol_id.name}: {bollinger_metadata.total_size} bytes'
        )
        return bollinger_metadata

    async def load_bollinger_data(
        self,
        symbol_id: SymbolId,
    ) -> Any | None:  # Optional[polars.DataFrame]
        """Загрузка полос Боллинджера."""
        key = get_bollinger_key(symbol_id)
        return await g_redis_manager.load_dataframe(key)

    async def save_candles_data(
        self,
        symbol_id: SymbolId,
        interval: str,
        candles_df: polars.DataFrame,
        min_trade_id: int,
        max_trade_id: int,
    ) -> CandlesMetadata:
        """Сохранение свечных данных."""
        key = get_candles_key(
            symbol_id,
            interval,
        )

        # Сохраняем DataFrame
        metadata = await g_redis_manager.save_dataframe(
            key=key,
            dataframe=candles_df,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
        )

        # Создаем полные метаданные
        candles_metadata = CandlesMetadata(
            **metadata,
            symbol_id=symbol_id,
            interval=interval,
            min_trade_id=min_trade_id,
            max_trade_id=max_trade_id,
            last_updated=datetime.now(UTC),
        )

        # Сохраняем метаданные
        await g_redis_manager.set_metadata(
            key,
            candles_metadata.model_dump_json(),
        )

        logger.info(
            f'Saved candles data for {symbol_id.name}:{interval}: {candles_metadata.total_size} bytes',
        )

        return candles_metadata

    async def load_candles_data(
        self,
        symbol_id: SymbolId,
        interval: str,
    ) -> Any | None:  # Optional[polars.DataFrame]
        """Загрузка свечных данных."""
        key = get_candles_key(
            symbol_id,
            interval,
        )
        return await g_redis_manager.load_dataframe(key)

    async def save_rsi_data(
        self,
        symbol_id: SymbolId,
        interval: str,
        rsi_series: Any,  # polars.Series
        timeperiod: int = 14,
    ) -> RSIMetadata:
        """Сохранение RSI данных."""
        key = get_rsi_key(
            symbol_id,
        )

        # Создаем DataFrame из серии
        import polars

        rsi_df = polars.DataFrame({'rsi': rsi_series})

        # Сохраняем DataFrame
        metadata = await g_redis_manager.save_dataframe(
            key=key,
            dataframe=rsi_df,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
        )

        # Создаем полные метаданные
        rsi_metadata = RSIMetadata(
            **metadata,
            symbol_id=symbol_id,
            interval=interval,
            timeperiod=timeperiod,
            last_updated=datetime.now(UTC),
        )

        # Сохраняем метаданные
        await g_redis_manager.set_metadata(
            key,
            rsi_metadata.model_dump_json(),
        )

        logger.info(
            f'Saved RSI data for {symbol_id.name}:{interval}: {rsi_metadata.total_size} bytes'
        )

        return rsi_metadata

    async def load_rsi_data(
        self,
        symbol_id: SymbolId,
    ) -> Any | None:  # Optional[polars.DataFrame]
        """Загрузка RSI данных."""
        key = get_rsi_key(
            symbol_id,
        )
        return await g_redis_manager.load_dataframe(key)

    async def save_smoothed_data(
        self,
        symbol_id: SymbolId,
        level: str,
        smoothed_df: polars.DataFrame,
        min_trade_id: int,
        max_trade_id: int,
    ) -> SmoothedMetadata:
        """Сохранение сглаженных данных."""
        key = get_smoothed_key(symbol_id, level)

        # Сохраняем DataFrame
        metadata = await g_redis_manager.save_dataframe(
            key=key,
            dataframe=smoothed_df,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
        )

        # Создаем полные метаданные
        smoothed_metadata = SmoothedMetadata(
            **metadata,
            symbol_id=symbol_id,
            level=level,
            min_trade_id=min_trade_id,
            max_trade_id=max_trade_id,
            last_updated=datetime.now(UTC),
        )

        # Сохраняем метаданные
        await g_redis_manager.set_metadata(
            key,
            smoothed_metadata.model_dump_json(),
        )

        logger.info(
            f'Saved smoothed data for {symbol_id.name}:{level}: {smoothed_metadata.total_size} bytes'
        )
        return smoothed_metadata

    async def load_smoothed_data(
        self,
        symbol_id: SymbolId,
        level: str,
    ) -> Any | None:  # Optional[polars.DataFrame]
        """Загрузка сглаженных данных."""
        key = get_smoothed_key(symbol_id, level)
        return await g_redis_manager.load_dataframe(key)

    async def save_lines_data(
        self,
        symbol_id: SymbolId,
        level: str,
        lines_df: polars.DataFrame,
        min_trade_id: int,
        max_trade_id: int,
    ) -> LinesMetadata:
        """Сохранение данных линий."""
        key = get_lines_key(symbol_id, level)

        # Сохраняем DataFrame
        metadata = await g_redis_manager.save_dataframe(
            key=key,
            dataframe=lines_df,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
        )

        # Создаем полные метаданные
        lines_metadata = LinesMetadata(
            **metadata,
            symbol_id=symbol_id,
            level=level,
            min_trade_id=min_trade_id,
            max_trade_id=max_trade_id,
            last_updated=datetime.now(UTC),
        )

        # Сохраняем метаданные
        await g_redis_manager.set_metadata(
            key,
            lines_metadata.model_dump_json(),
        )

        logger.info(
            f'Saved lines data for {symbol_id.name}:{level}: {lines_metadata.total_size} bytes'
        )
        return lines_metadata

    async def load_lines_data(
        self,
        symbol_id: SymbolId,
        level: str,
    ) -> Any | None:  # Optional[polars.DataFrame]
        """Загрузка данных линий."""
        key = get_lines_key(symbol_id, level)
        return await g_redis_manager.load_dataframe(key)

    async def save_extreme_lines_data(
        self,
        symbol_id: SymbolId,
        extreme_lines_array: numpy.ndarray,
        width: int,
        height: int,
        scale: float,
        min_trade_id: int,
        min_price: float,
    ) -> ExtremeLinesMetadata:
        """Сохранение экстремальных линий."""
        key = get_extreme_lines_key(
            symbol_id,
        )

        # Конвертируем numpy array в DataFrame
        import polars

        # Преобразуем 2D массив в список строк для сериализации
        extreme_lines_list = extreme_lines_array.tolist()
        extreme_lines_df = polars.DataFrame({'extreme_lines': [extreme_lines_list]})

        # Сохраняем DataFrame
        metadata = await g_redis_manager.save_dataframe(
            key=key,
            dataframe=extreme_lines_df,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
        )

        # Создаем полные метаданные
        extreme_lines_metadata = ExtremeLinesMetadata(
            **metadata,
            symbol_id=symbol_id,
            width=width,
            height=height,
            scale=scale,
            min_trade_id=min_trade_id,
            min_price=min_price,
            last_updated=datetime.now(UTC),
        )

        # Сохраняем метаданные
        await g_redis_manager.set_metadata(
            key,
            extreme_lines_metadata.model_dump_json(),
        )

        logger.info(
            f'Saved extreme lines data for {symbol_id.name}: {extreme_lines_metadata.total_size} bytes'
        )
        return extreme_lines_metadata

    async def load_extreme_lines_data(
        self,
        symbol_id: SymbolId,
    ) -> Any | None:  # Optional[numpy.ndarray]
        """Загрузка экстремальных линий."""
        key = get_extreme_lines_key(
            symbol_id,
        )

        df = await g_redis_manager.load_dataframe(key)

        if df is None:
            return None

        # Восстанавливаем numpy array из DataFrame
        import numpy as np

        extreme_lines_list = df.get_column('extreme_lines').to_list()[0]
        return np.array(extreme_lines_list)

    async def save_order_book_volumes_data(
        self,
        symbol_id: SymbolId,
        asks_array: numpy.ndarray,
        bids_array: numpy.ndarray,
        width: int,
        height: int,
        scale: float,
        min_trade_id: int,
        min_price: float,
    ) -> OrderBookVolumesMetadata:
        """Сохранение объемов стакана."""
        key = get_order_book_volumes_key(
            symbol_id,
        )

        # Конвертируем numpy arrays в DataFrame
        import polars

        # Преобразуем 2D массивы в список строк для сериализации
        asks_list = asks_array.tolist()
        bids_list = bids_array.tolist()

        order_book_df = polars.DataFrame(
            {
                'asks': [asks_list],
                'bids': [bids_list],
            }
        )

        # Сохраняем DataFrame
        metadata = await g_redis_manager.save_dataframe(
            key=key,
            dataframe=order_book_df,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
        )

        # Создаем полные метаданные
        order_book_metadata = OrderBookVolumesMetadata(
            **metadata,
            symbol_id=symbol_id,
            width=width,
            height=height,
            scale=scale,
            min_trade_id=min_trade_id,
            min_price=min_price,
            last_updated=datetime.now(UTC),
        )

        # Сохраняем метаданные
        await g_redis_manager.set_metadata(
            key,
            order_book_metadata.model_dump_json(),
        )

        logger.info(
            f'Saved order book volumes data for {symbol_id.name}: {order_book_metadata.total_size} bytes'
        )
        return order_book_metadata

    async def load_order_book_volumes_data(
        self,
        symbol_id: SymbolId,
    ) -> tuple[
        Any | None, Any | None
    ]:  # tuple[Optional[numpy.ndarray], Optional[numpy.ndarray]]
        """Загрузка объемов стакана."""
        key = get_order_book_volumes_key(
            symbol_id,
        )
        df = await g_redis_manager.load_dataframe(key)

        if df is None:
            return None, None

        # Восстанавливаем numpy arrays из DataFrame
        import numpy as np

        asks_list = df.get_column('asks').to_list()[0]
        bids_list = df.get_column('bids').to_list()[0]

        return np.array(asks_list), np.array(bids_list)

    async def save_velocity_series(
        self,
        symbol_id: SymbolId,
        interval_name: str,
        velocity_series: polars.Series,
    ) -> VelocityMetadata:
        """Сохранение данных скорости."""
        key = get_velocity_key(
            symbol_id,
            interval_name,
        )

        # Создаем DataFrame из серии

        velocity_df = polars.DataFrame({
            'velocity': velocity_series,
        })

        # Сохраняем DataFrame
        metadata = await g_redis_manager.save_dataframe(
            key=key,
            dataframe=velocity_df,
            compression=CompressionAlgorithm.LZ4,
            max_size_bytes=MAX_PART_SIZE_BYTES,
        )

        # Создаем полные метаданные
        velocity_metadata = VelocityMetadata(
            **metadata,
            symbol_id=symbol_id,
            interval=interval_name,
            last_updated=datetime.now(UTC),
        )

        # Сохраняем метаданные
        await g_redis_manager.set_metadata(
            key,
            velocity_metadata.model_dump_json(),
        )

        logger.info(
            f'Saved velocity data for {symbol_id.name}:{interval_name}: {velocity_metadata.total_size} bytes'
        )
        return velocity_metadata

    async def load_velocity_series(
        self,
        symbol_id: SymbolId,
        interval_name: str,
    ) -> polars.DataFrame | None:
        """Загрузка данных скорости."""
        key = get_velocity_key(
            symbol_id,
            interval_name,
        )

        return await g_redis_manager.load_dataframe(
            key,
        )

    async def save_available_symbols(self, symbol_names: list[str]) -> None:
        """Сохранение списка доступных символов."""
        key = get_available_symbols_key()

        await g_redis_manager.set(
            key,
            json.dumps(symbol_names),
        )

        logger.info(
            f'Saved available symbols: {len(symbol_names)} symbols',
        )

    async def load_available_symbols(self) -> list[str]:
        """Загрузка списка доступных символов."""
        key = get_available_symbols_key()
        data = await g_redis_manager.get(key)
        if data:
            return json.loads(data)
        return []

    async def save_symbol_metadata(self, symbol_metadata: SymbolMetadata) -> None:
        """Сохранение метаданных символа."""
        key = get_metadata_key(
            symbol_metadata.symbol_id,
        )

        await g_redis_manager.set(
            key,
            symbol_metadata.model_dump_json(),
        )

        logger.info(f'Saved symbol metadata for {symbol_metadata.symbol_id}')

    async def load_symbol_metadata(
        self,
        symbol_id: SymbolId,
    ) -> SymbolMetadata | None:
        """Загрузка метаданных символа."""
        key = get_metadata_key(
            symbol_id,
        )
        data = await g_redis_manager.get(key)
        if data:
            return SymbolMetadata.model_validate_json(data)
        return None

    async def save_processing_status(
        self,
        status: ProcessingStatus,
    ) -> None:
        """Сохранение статуса обработки."""
        key = f'processing_status:{status.symbol_id.value}'

        await g_redis_manager.set(
            key,
            status.model_dump_json(),
        )

        logger.debug(
            f'Saved processing status for {status.symbol_id.name}: {status.status}',
        )

    async def load_processing_status(
        self,
        symbol_id: SymbolId,
    ) -> ProcessingStatus | None:
        """Загрузка статуса обработки."""
        key = f'processing_status:{symbol_id.value}'
        data = await g_redis_manager.get(key)
        if data:
            return ProcessingStatus.model_validate_json(data)

        return None


# Глобальный экземпляр сервиса
g_redis_data_service = RedisDataService()
