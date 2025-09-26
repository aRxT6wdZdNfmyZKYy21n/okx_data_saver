"""
Модуль для обработки финансовых данных.
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
from enumerations import (
    SymbolId,
)
from main.process_data.performance_optimizer import g_performance_optimizer
from main.process_data.redis_service import g_redis_data_service

logger = logging.getLogger(__name__)


class DataProcessor:
    """Процессор для обработки финансовых данных."""

    def __init__(self):
        self.redis_service = g_redis_data_service

    async def process_trades_data(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка данных о сделках и создание всех производных данных."""
        logger.info(
            f'Processing trades data for {symbol_id.name}: {trades_df.height} records'
        )

        # Обработка полос Боллинджера
        await self._process_bollinger_bands(
            symbol_id,
            trades_df,
        )

        # Обработка свечных данных
        await self._process_candles_data(
            symbol_id,
            trades_df,
        )

        # Обработка RSI
        await self._process_rsi_data(
            symbol_id,
            trades_df,
        )

        # Обработка сглаженных данных
        await self._process_smoothed_data(
            symbol_id,
            trades_df,
        )

        # Обработка экстремальных линий
        await self._process_extreme_lines(
            symbol_id,
            trades_df,
        )

        # Обработка объемов стакана
        await self._process_order_book_volumes(
            symbol_id,
            trades_df,
        )

        # Обработка данных скорости
        await self._process_velocity_data(
            symbol_id,
            trades_df,
        )

        logger.info(
            f'Completed processing trades data for {symbol_id.name}',
        )

    async def _process_bollinger_bands(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка полос Боллинджера."""
        with Timer() as timer:
            # Вычисляем полосы Боллинджера
            bollinger_df = trades_df.with_columns(
                polars_talib.bbands(
                    matype=int(talib.MA_Type.SMA),
                    real=polars.col('price'),
                    timeperiod=20,
                ).alias('bbands'),
            ).unnest('bbands')

            upper_band = bollinger_df.get_column('upperband')
            middle_band = bollinger_df.get_column('middleband')
            lower_band = bollinger_df.get_column('lowerband')

            if not upper_band.is_empty():
                # Сохраняем в Redis
                await self.redis_service.save_bollinger_data(
                    symbol_id=symbol_id,
                    upper_band=upper_band,
                    middle_band=middle_band,
                    lower_band=lower_band,
                    timeperiod=20,
                )

                logger.info(f'Bollinger bands processed in {timer.elapsed:.3f}s')

    async def _process_candles_data(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка свечных данных по интервалам."""
        with Timer() as timer:
            for interval_name in PlotConstants.IntervalNames:
                await self._process_candles_for_interval(
                    symbol_id=symbol_id,
                    trades_df=trades_df,
                    interval_name=interval_name,
                )

            logger.info(f'Candles data processed in {timer.elapsed:.3f}s')

    async def _process_candles_for_interval(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
        interval_name: str,
    ) -> None:
        """Обработка свечных данных для конкретного интервала."""
        candle_raw_data_list: list[dict[str, typing.Any]] = []

        interval_duration = CommonConstants.IntervalDurationByNameMap[interval_name]
        interval_duration_ms = int(interval_duration.total_seconds() * 1000)

        last_candle_raw_data: dict[str, typing.Any] | None = None

        # Получаем существующие данные из Redis
        existing_candles = await self.redis_service.load_candles_data(
            symbol_id,
            interval_name,
        )
        min_trade_id = 0

        if existing_candles is not None and existing_candles.height > 0:
            min_trade_id = int(existing_candles.get_column('start_trade_id').max())

        # Обрабатываем только новые данные
        new_trades = trades_df.filter(polars.col('trade_id') >= min_trade_id)

        for trade_data in new_trades.iter_rows(named=True):
            trade_id: int = trade_data['trade_id']
            price: float = trade_data['price']
            quantity: float = trade_data['quantity']
            volume = price * quantity
            datetime_: datetime = trade_data['datetime']

            timestamp_ms = int(datetime_.timestamp() * 1000)
            candle_start_timestamp_ms = timestamp_ms - (
                timestamp_ms % interval_duration_ms
            )
            candle_end_timestamp_ms = candle_start_timestamp_ms + interval_duration_ms

            if last_candle_raw_data is not None:
                if (
                    candle_start_timestamp_ms
                    == last_candle_raw_data['start_timestamp_ms']
                ):
                    # Обновляем существующую свечу
                    if price > last_candle_raw_data['high_price']:
                        last_candle_raw_data['high_price'] = price
                    if price < last_candle_raw_data['low_price']:
                        last_candle_raw_data['low_price'] = price

                    last_candle_raw_data.update(
                        {
                            'trades_count': last_candle_raw_data['trades_count'] + 1,
                            'end_trade_id': trade_id,
                            'close_price': price,
                            'volume': last_candle_raw_data['volume'] + volume,
                        }
                    )
                else:
                    # Сохраняем предыдущую свечу и начинаем новую
                    candle_raw_data_list.append(last_candle_raw_data)
                    last_candle_raw_data = None

            if last_candle_raw_data is None:
                last_candle_raw_data = {
                    'close_price': price,
                    'end_timestamp_ms': candle_end_timestamp_ms,
                    'end_trade_id': trade_id + 1,
                    'high_price': price,
                    'low_price': price,
                    'open_price': price,
                    'start_timestamp_ms': candle_start_timestamp_ms,
                    'start_trade_id': trade_id,
                    'trades_count': 1,
                    'volume': volume,
                }

        if last_candle_raw_data is not None:
            candle_raw_data_list.append(last_candle_raw_data)

        if candle_raw_data_list:
            new_candle_dataframe = polars.DataFrame(candle_raw_data_list)

            new_candle_dataframe = new_candle_dataframe.with_columns(
                polars.col('end_timestamp_ms')
                .cast(polars.Datetime(time_unit='ms', time_zone=UTC))
                .alias('end_datetime'),
                polars.col('start_timestamp_ms')
                .cast(polars.Datetime(time_unit='ms', time_zone=UTC))
                .alias('start_datetime'),
            )

            new_candle_dataframe = new_candle_dataframe.sort('start_trade_id')

            # Объединяем с существующими данными
            if existing_candles is not None:
                final_candles = polars.concat([existing_candles, new_candle_dataframe])
            else:
                final_candles = new_candle_dataframe

            final_candles = final_candles.sort('start_trade_id')

            # Сохраняем в Redis
            min_trade_id = int(final_candles.get_column('start_trade_id').min())
            max_trade_id = int(final_candles.get_column('end_trade_id').max())

            await self.redis_service.save_candles_data(
                symbol_id=symbol_id,
                interval=interval_name,
                candles_df=final_candles,
                min_trade_id=min_trade_id,
                max_trade_id=max_trade_id,
            )

    async def _process_rsi_data(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка RSI данных."""
        with Timer() as timer:
            # Получаем свечные данные для расчета RSI с кэшированием
            candles_df = await g_performance_optimizer.get_cached_data(
                key=f'candles:{symbol_id.value}:1m',
                loader_func=self.redis_service.load_candles_data,
                symbol_id=symbol_id,
                interval='1m',
            )

            if candles_df is not None and candles_df.height > 0:
                rsi_df = candles_df.with_columns(
                    polars_talib.rsi(
                        real=polars.col('close_price'),
                        timeperiod=14,
                    ).alias('rsi')
                )

                rsi_series = rsi_df.get_column('rsi')

                if not rsi_series.is_empty():
                    await self.redis_service.save_rsi_data(
                        symbol_id=symbol_id,
                        interval='1m',
                        rsi_series=rsi_series,
                        timeperiod=14,
                    )

            logger.info(f'RSI data processed in {timer.elapsed:.3f}s')

    async def _process_smoothed_data(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка сглаженных данных."""
        with Timer() as timer:
            # Получаем существующие сглаженные данные
            smoothed_data = {}
            for level in PlotConstants.TradesSmoothingLevels:
                if level != 'Raw (0)':
                    existing_data = await self.redis_service.load_smoothed_data(
                        symbol_id,
                        level,
                    )
                    if existing_data is not None:
                        smoothed_data[level] = existing_data

            # Обрабатываем сглаживание для каждого уровня
            for level in PlotConstants.TradesSmoothingLevels:
                if level == 'Raw (0)':
                    continue

                smoothed_df = await self._calculate_smoothed_data(
                    trades_df=trades_df,
                    level=level,
                    existing_data=smoothed_data.get(level),
                )

                if smoothed_df is not None and smoothed_df.height > 0:
                    min_trade_id = int(smoothed_df.get_column('trade_id').min())
                    max_trade_id = int(smoothed_df.get_column('trade_id').max())

                    await self.redis_service.save_smoothed_data(
                        symbol_id=symbol_id,
                        level=level,
                        smoothed_df=smoothed_df,
                        min_trade_id=min_trade_id,
                        max_trade_id=max_trade_id,
                    )

            logger.info(f'Smoothed data processed in {timer.elapsed:.3f}s')

    async def _calculate_smoothed_data(
        self,
        trades_df: DataFrame,
        level: str,
        existing_data: DataFrame | None = None,
    ) -> DataFrame | None:
        """Вычисление сглаженных данных для конкретного уровня."""
        # Упрощенная версия алгоритма сглаживания
        # В реальной реализации здесь должна быть полная логика из processor.py

        if level == 'Smoothed (1)':
            # Простое сглаживание - берем каждую 10-ю точку
            smoothed_trades = trades_df.slice(0, None, 10)
            return smoothed_trades.select(['trade_id', 'price', 'datetime'])

        return None

    async def _process_extreme_lines(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка экстремальных линий."""
        with Timer() as timer:
            price_series = trades_df.get_column('price')
            max_price = float(price_series.max())
            min_price = float(price_series.min())
            delta_price = max_price - min_price

            if not delta_price:
                return

            trade_id_series = trades_df.get_column('trade_id')
            max_trade_id = int(trade_id_series.max())
            min_trade_id = int(trade_id_series.min())
            delta_trade_id = max_trade_id - min_trade_id

            if not delta_trade_id:
                return

            aspect_ratio = delta_trade_id / delta_price
            height = 100
            scale = delta_price / height
            width = int(height * aspect_ratio)

            # Создаем массив экстремальных линий
            extreme_lines_array = numpy.zeros((width, height))

            # Упрощенная логика заполнения массива
            # В реальной реализации здесь должна быть полная логика из processor.py

            await self.redis_service.save_extreme_lines_data(
                symbol_id=symbol_id,
                extreme_lines_array=extreme_lines_array,
                width=width,
                height=height,
                scale=scale,
                min_trade_id=min_trade_id,
                min_price=min_price,
            )

            logger.info(f'Extreme lines processed in {timer.elapsed:.3f}s')

    async def _process_order_book_volumes(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка объемов стакана."""
        with Timer() as timer:
            # Получаем данные о стакане заявок
            price_series = trades_df.get_column('price')
            max_price = float(price_series.max())
            min_price = float(price_series.min())
            delta_price = max_price - min_price

            if not delta_price:
                return

            trade_id_series = trades_df.get_column('trade_id')
            max_trade_id = int(trade_id_series.max())
            min_trade_id = int(trade_id_series.min())
            delta_trade_id = max_trade_id - min_trade_id

            if not delta_trade_id:
                return

            aspect_ratio = delta_trade_id / delta_price
            height = 100
            scale = delta_price / height
            width = int(height * aspect_ratio)

            # Создаем массивы объемов
            asks_array = numpy.zeros((width, height))
            bids_array = numpy.zeros((width, height))

            # Упрощенная логика заполнения массивов
            # В реальной реализации здесь должна быть полная логика из processor.py

            await self.redis_service.save_order_book_volumes_data(
                symbol_id=symbol_id,
                asks_array=asks_array,
                bids_array=bids_array,
                width=width,
                height=height,
                scale=scale,
                min_trade_id=min_trade_id,
                min_price=min_price,
            )

            logger.info(f'Order book volumes processed in {timer.elapsed:.3f}s')

    async def _process_velocity_data(
        self,
        symbol_id: SymbolId,
        trades_df: DataFrame,
    ) -> None:
        """Обработка данных скорости."""
        with Timer() as timer:
            # Получаем свечные данные для расчета скорости
            candles_df = await self.redis_service.load_candles_data(
                symbol_id,
                '1m',
            )

            if candles_df is not None and candles_df.height > 0:
                velocity_series = candles_df.get_column('trades_count')

                await self.redis_service.save_velocity_data(
                    symbol_id=symbol_id,
                    interval='1m',
                    velocity_series=velocity_series,
                )

            logger.info(f'Velocity data processed in {timer.elapsed:.3f}s')


# Глобальный экземпляр процессора
g_data_processor = DataProcessor()
