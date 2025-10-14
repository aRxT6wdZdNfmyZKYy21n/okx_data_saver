"""
Модуль для получения данных из таблицы okx_data_set_record_data_2.
"""

import logging
from datetime import UTC, datetime

import polars
from chrono import Timer
from polars import DataFrame

from enumerations import SymbolId
from main.save_final_data_set_2.schemas import OKXDataSetRecordData_2
from settings import settings

logger = logging.getLogger(__name__)


class DataSetRecordFetcher:
    """Класс для получения агрегированных данных из таблицы okx_data_set_record_data_2."""

    @staticmethod
    async def fetch_data_set_records_dataframe(
        min_start_trade_id: int,
        symbol_id: SymbolId,
    ) -> DataFrame | None:
        """Получение агрегированных данных из базы данных."""
        with Timer() as timer:
            data_set_dataframe = polars.read_database_uri(
                engine='connectorx',
                query=(
                    'SELECT'
                    # Primary key fields
                    ' symbol_id'
                    ', start_trade_id'
                    # Attribute fields
                    ', buy_quantity'
                    ', buy_trades_count'
                    ', buy_volume'
                    ', close_price'
                    ', end_timestamp_ms'
                    ', end_trade_id'
                    ', high_price'
                    ', low_price'
                    ', open_price'
                    ', start_timestamp_ms'
                    ', total_quantity'
                    ', total_trades_count'
                    ', total_volume'
                    f' FROM {OKXDataSetRecordData_2.__tablename__}'
                    ' WHERE'
                    f' symbol_id = {symbol_id.name!r}'
                    f' AND start_trade_id >= {min_start_trade_id!r}'
                    ' ORDER BY'
                    ' symbol_id ASC'
                    ', start_trade_id ASC'
                    f' LIMIT {2_000_000!r}'
                    ';'
                ),
                uri=(
                    'postgresql'
                    '://'
                    f'{settings.POSTGRES_DB_USER_NAME}'
                    ':'
                    f'{settings.POSTGRES_DB_PASSWORD.get_secret_value()}'
                    '@'
                    f'{settings.POSTGRES_DB_HOST_NAME}'
                    ':'
                    f'{settings.POSTGRES_DB_PORT}'
                    '/'
                    f'{settings.POSTGRES_DB_NAME}'
                ),
            )

        logger.info(f'Fetched data set records dataframe by {timer.elapsed:.3f}s')

        if data_set_dataframe.height == 0:
            return None

        # Преобразуем типы данных
        data_set_dataframe = data_set_dataframe.with_columns([
            polars.col('start_timestamp_ms')
            .cast(polars.Datetime(time_unit='ms', time_zone=UTC))
            .alias('start_datetime'),
            polars.col('end_timestamp_ms')
            .cast(polars.Datetime(time_unit='ms', time_zone=UTC))
            .alias('end_datetime'),
            polars.col('open_price').cast(polars.Float64),
            polars.col('high_price').cast(polars.Float64),
            polars.col('low_price').cast(polars.Float64),
            polars.col('close_price').cast(polars.Float64),
            polars.col('total_quantity').cast(polars.Float64),
            polars.col('buy_quantity').cast(polars.Float64),
            polars.col('total_volume').cast(polars.Float64),
            polars.col('buy_volume').cast(polars.Float64),
        ])

        data_set_dataframe = data_set_dataframe.sort('start_trade_id')

        return data_set_dataframe


# Глобальный экземпляр
g_data_set_record_fetcher = DataSetRecordFetcher()
