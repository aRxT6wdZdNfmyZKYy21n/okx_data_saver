"""
Модуль для валидации данных.
"""

import logging
from datetime import UTC, datetime
from typing import Any

from polars import DataFrame, Series

logger = logging.getLogger(__name__)


class DataValidator:
    """Валидатор для проверки корректности данных."""

    def __init__(self):
        self.validation_rules = self._setup_validation_rules()

    def _setup_validation_rules(self) -> dict[str, dict[str, Any]]:
        """Настройка правил валидации."""
        return {
            'trades_data': {
                'required_columns': ['trade_id', 'price', 'quantity', 'datetime'],
                'price_range': (0.0, 1_000_000.0),
                'quantity_range': (0.0, 1_000_000.0),
                'datetime_range': (datetime(2020, 1, 1, tzinfo=UTC), datetime.now(UTC)),
            },
            'candles_data': {
                'required_columns': [
                    'start_trade_id',
                    'end_trade_id',
                    'open_price',
                    'high_price',
                    'low_price',
                    'close_price',
                    'volume',
                ],
                'price_range': (0.0, 1_000_000.0),
                'volume_range': (0.0, 1_000_000_000.0),
            },
            'bollinger_bands': {
                'required_columns': ['upper_band', 'middle_band', 'lower_band'],
                'band_relationship': 'upper >= middle >= lower',
            },
            'rsi_data': {
                'required_columns': ['rsi'],
                'rsi_range': (0.0, 100.0),
            },
        }

    def validate_trades_data(self, df: DataFrame) -> tuple[bool, list[str]]:
        """Валидация данных о сделках."""
        errors = []

        if df is None or df.height == 0:
            return False, ['Empty or None trades data']

        # Проверяем обязательные колонки
        required_columns = self.validation_rules['trades_data']['required_columns']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f'Missing required columns: {missing_columns}')

        if errors:
            return False, errors

        # Проверяем диапазоны значений
        price_range = self.validation_rules['trades_data']['price_range']
        if 'price' in df.columns:
            price_stats = df.get_column('price').describe()
            min_price = price_stats.filter(pl.col('statistic') == 'min').get_column(
                'value'
            )[0]
            max_price = price_stats.filter(pl.col('statistic') == 'max').get_column(
                'value'
            )[0]
            if min_price < price_range[0] or max_price > price_range[1]:
                errors.append(f'Price values out of range: {min_price}-{max_price}')

        quantity_range = self.validation_rules['trades_data']['quantity_range']
        if 'quantity' in df.columns:
            quantity_stats = df.get_column('quantity').describe()
            min_quantity = quantity_stats.filter(
                pl.col('statistic') == 'min'
            ).get_column('value')[0]
            max_quantity = quantity_stats.filter(
                pl.col('statistic') == 'max'
            ).get_column('value')[0]
            if min_quantity < quantity_range[0] or max_quantity > quantity_range[1]:
                errors.append(
                    f'Quantity values out of range: {min_quantity}-{max_quantity}'
                )

        # Проверяем уникальность trade_id
        if 'trade_id' in df.columns:
            unique_trade_ids = df.get_column('trade_id').n_unique()
            if unique_trade_ids != df.height:
                errors.append(
                    f'Duplicate trade_id found: {df.height - unique_trade_ids} duplicates'
                )

        # Проверяем сортировку по trade_id
        if 'trade_id' in df.columns:
            trade_ids = df.get_column('trade_id').to_list()
            if trade_ids != sorted(trade_ids):
                errors.append('Trade IDs are not sorted')

        return len(errors) == 0, errors

    def validate_candles_data(self, df: DataFrame) -> tuple[bool, list[str]]:
        """Валидация свечных данных."""
        errors = []

        if df is None or df.height == 0:
            return False, ['Empty or None candles data']

        # Проверяем обязательные колонки
        required_columns = self.validation_rules['candles_data']['required_columns']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f'Missing required columns: {missing_columns}')

        if errors:
            return False, errors

        # Проверяем логику свечей (high >= low, high >= open, high >= close, low <= open, low <= close)
        if all(
            col in df.columns
            for col in ['high_price', 'low_price', 'open_price', 'close_price']
        ):
            high = df.get_column('high_price')
            low = df.get_column('low_price')
            open_price = df.get_column('open_price')
            close_price = df.get_column('close_price')

            invalid_candles = (
                (high < low)
                | (high < open_price)
                | (high < close_price)
                | (low > open_price)
                | (low > close_price)
            )

            if invalid_candles.any():
                invalid_count = invalid_candles.sum()
                errors.append(f'Invalid candle logic in {invalid_count} candles')

        # Проверяем диапазоны цен
        price_range = self.validation_rules['candles_data']['price_range']
        for price_col in ['open_price', 'high_price', 'low_price', 'close_price']:
            if price_col in df.columns:
                price_stats = df.get_column(price_col).describe()
                if (
                    price_stats['min'] < price_range[0]
                    or price_stats['max'] > price_range[1]
                ):
                    errors.append(
                        f'{price_col} values out of range: {price_stats["min"]}-{price_stats["max"]}'
                    )

        return len(errors) == 0, errors

    def validate_bollinger_bands(self, df: DataFrame) -> tuple[bool, list[str]]:
        """Валидация полос Боллинджера."""
        errors = []

        if df is None or df.height == 0:
            return False, ['Empty or None bollinger bands data']

        # Проверяем обязательные колонки
        required_columns = self.validation_rules['bollinger_bands']['required_columns']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f'Missing required columns: {missing_columns}')

        if errors:
            return False, errors

        # Проверяем соотношение полос (upper >= middle >= lower)
        upper = df.get_column('upper_band')
        middle = df.get_column('middle_band')
        lower = df.get_column('lower_band')

        invalid_bands = (upper < middle) | (middle < lower)
        if invalid_bands.any():
            invalid_count = invalid_bands.sum()
            errors.append(
                f'Invalid bollinger band relationship in {invalid_count} rows'
            )

        return len(errors) == 0, errors

    def validate_rsi_data(self, series: Series) -> tuple[bool, list[str]]:
        """Валидация RSI данных."""
        errors = []

        if series is None or series.is_empty():
            return False, ['Empty or None RSI data']

        # Проверяем диапазон RSI (0-100)
        rsi_range = self.validation_rules['rsi_data']['rsi_range']
        rsi_stats = series.describe()

        if rsi_stats['min'] < rsi_range[0] or rsi_stats['max'] > rsi_range[1]:
            errors.append(
                f'RSI values out of range: {rsi_stats["min"]}-{rsi_stats["max"]}'
            )

        # Проверяем на NaN значения
        nan_count = series.is_null().sum()
        if nan_count > 0:
            errors.append(f'RSI contains {nan_count} NaN values')

        return len(errors) == 0, errors

    def validate_data_consistency(
        self,
        trades_df: DataFrame,
        candles_df: DataFrame | None = None,
        bollinger_df: DataFrame | None = None,
    ) -> tuple[bool, list[str]]:
        """Валидация согласованности данных между разными типами."""
        errors = []

        if trades_df is None:
            return False, ['Trades data is required for consistency check']

        # Проверяем согласованность с свечными данными
        if candles_df is not None:
            trades_trade_ids = set(trades_df.get_column('trade_id').to_list())
            candles_start_ids = set(candles_df.get_column('start_trade_id').to_list())
            candles_end_ids = set(candles_df.get_column('end_trade_id').to_list())

            # Проверяем, что все start_trade_id и end_trade_id из свечей есть в trades
            missing_start_ids = candles_start_ids - trades_trade_ids
            if missing_start_ids:
                errors.append(
                    f'Missing start_trade_id in trades: {len(missing_start_ids)} IDs'
                )

            missing_end_ids = candles_end_ids - trades_trade_ids
            if missing_end_ids:
                errors.append(
                    f'Missing end_trade_id in trades: {len(missing_end_ids)} IDs'
                )

        # Проверяем согласованность с полосами Боллинджера
        if bollinger_df is not None:
            trades_height = trades_df.height
            bollinger_height = bollinger_df.height

            if trades_height != bollinger_height:
                errors.append(
                    f'Height mismatch: trades={trades_height}, bollinger={bollinger_height}'
                )

        return len(errors) == 0, errors

    def validate_symbol_metadata(
        self, metadata: dict[str, Any]
    ) -> tuple[bool, list[str]]:
        """Валидация метаданных символа."""
        errors = []

        required_fields = ['symbol_id', 'symbol_name', 'last_updated']
        missing_fields = [field for field in required_fields if field not in metadata]
        if missing_fields:
            errors.append(f'Missing required fields: {missing_fields}')

        # Проверяем формат symbol_id
        if 'symbol_id' in metadata:
            symbol_id = metadata['symbol_id']
            if not isinstance(symbol_id, str) or not symbol_id:
                errors.append('Invalid symbol_id format')

        # Проверяем формат symbol_name
        if 'symbol_name' in metadata:
            symbol_name = metadata['symbol_name']
            if not isinstance(symbol_name, str) or not symbol_name:
                errors.append('Invalid symbol_name format')

        # Проверяем формат last_updated
        if 'last_updated' in metadata:
            last_updated = metadata['last_updated']
            if not isinstance(last_updated, datetime):
                errors.append('Invalid last_updated format')

        return len(errors) == 0, errors

    def get_validation_summary(
        self, validation_results: dict[str, tuple[bool, list[str]]]
    ) -> dict[str, Any]:
        """Получение сводки валидации."""
        total_checks = len(validation_results)
        passed_checks = sum(
            1 for is_valid, _ in validation_results.values() if is_valid
        )
        failed_checks = total_checks - passed_checks

        all_errors = []
        for data_type, (is_valid, errors) in validation_results.items():
            if not is_valid:
                all_errors.extend([f'{data_type}: {error}' for error in errors])

        return {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'success_rate': passed_checks / total_checks if total_checks > 0 else 0,
            'errors': all_errors,
        }


# Глобальный экземпляр валидатора
g_data_validator = DataValidator()
