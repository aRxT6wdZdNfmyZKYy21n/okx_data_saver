"""
Тесты для валидации данных.
"""

from datetime import UTC, datetime

import polars
import pytest

from main.process_data.data_validator import g_data_validator


class TestDataValidation:
    """Тесты для валидации данных."""

    def test_validate_trades_data_valid(self):
        """Тест валидации корректных данных о сделках."""
        df = polars.DataFrame(
            {
                'trade_id': [1, 2, 3, 4, 5],
                'price': [100.0, 101.0, 102.0, 103.0, 104.0],
                'quantity': [1.0, 2.0, 3.0, 4.0, 5.0],
                'datetime': [datetime.now(UTC) for _ in range(5)],
            }
        )

        is_valid, errors = g_data_validator.validate_trades_data(df)
        assert is_valid
        assert len(errors) == 0

    def test_validate_trades_data_invalid_columns(self):
        """Тест валидации данных с отсутствующими колонками."""
        df = polars.DataFrame(
            {
                'trade_id': [1, 2, 3],
                'price': [100.0, 101.0, 102.0],
                # Отсутствуют quantity и datetime
            }
        )

        is_valid, errors = g_data_validator.validate_trades_data(df)
        assert not is_valid
        assert 'Missing required columns' in errors[0]

    def test_validate_trades_data_invalid_price_range(self):
        """Тест валидации данных с некорректным диапазоном цен."""
        df = polars.DataFrame(
            {
                'trade_id': [1, 2, 3],
                'price': [
                    -100.0,
                    101.0,
                    2_000_000.0,
                ],  # Отрицательная и слишком большая цена
                'quantity': [1.0, 2.0, 3.0],
                'datetime': [datetime.now(UTC) for _ in range(3)],
            }
        )

        is_valid, errors = g_data_validator.validate_trades_data(df)
        assert not is_valid
        assert 'Price values out of range' in errors[0]

    def test_validate_trades_data_duplicate_trade_ids(self):
        """Тест валидации данных с дублирующимися trade_id."""
        df = polars.DataFrame(
            {
                'trade_id': [1, 2, 2, 3],  # Дублирующийся trade_id
                'price': [100.0, 101.0, 102.0, 103.0],
                'quantity': [1.0, 2.0, 3.0, 4.0],
                'datetime': [datetime.now(UTC) for _ in range(4)],
            }
        )

        is_valid, errors = g_data_validator.validate_trades_data(df)
        assert not is_valid
        assert 'Duplicate trade_id found' in errors[0]

    def test_validate_trades_data_unsorted_trade_ids(self):
        """Тест валидации данных с несортированными trade_id."""
        df = polars.DataFrame(
            {
                'trade_id': [3, 1, 2],  # Несортированные trade_id
                'price': [100.0, 101.0, 102.0],
                'quantity': [1.0, 2.0, 3.0],
                'datetime': [datetime.now(UTC) for _ in range(3)],
            }
        )

        is_valid, errors = g_data_validator.validate_trades_data(df)
        assert not is_valid
        assert 'Trade IDs are not sorted' in errors[0]

    def test_validate_candles_data_valid(self):
        """Тест валидации корректных свечных данных."""
        df = polars.DataFrame(
            {
                'start_trade_id': [1, 4, 7],
                'end_trade_id': [3, 6, 9],
                'open_price': [100.0, 103.0, 106.0],
                'high_price': [102.0, 105.0, 108.0],
                'low_price': [99.0, 102.0, 105.0],
                'close_price': [101.0, 104.0, 107.0],
                'volume': [100.0, 200.0, 300.0],
            }
        )

        is_valid, errors = g_data_validator.validate_candles_data(df)
        assert is_valid
        assert len(errors) == 0

    def test_validate_candles_data_invalid_logic(self):
        """Тест валидации свечных данных с некорректной логикой."""
        df = polars.DataFrame(
            {
                'start_trade_id': [1, 4],
                'end_trade_id': [3, 6],
                'open_price': [100.0, 103.0],
                'high_price': [99.0, 105.0],  # high < low в первой свече
                'low_price': [101.0, 102.0],
                'close_price': [101.0, 104.0],
                'volume': [100.0, 200.0],
            }
        )

        is_valid, errors = g_data_validator.validate_candles_data(df)
        assert not is_valid
        assert 'Invalid candle logic' in errors[0]

    def test_validate_bollinger_bands_valid(self):
        """Тест валидации корректных полос Боллинджера."""
        df = polars.DataFrame(
            {
                'upper_band': [102.0, 103.0, 104.0],
                'middle_band': [100.0, 101.0, 102.0],
                'lower_band': [98.0, 99.0, 100.0],
            }
        )

        is_valid, errors = g_data_validator.validate_bollinger_bands(df)
        assert is_valid
        assert len(errors) == 0

    def test_validate_bollinger_bands_invalid_relationship(self):
        """Тест валидации полос Боллинджера с некорректным соотношением."""
        df = polars.DataFrame(
            {
                'upper_band': [98.0, 103.0, 104.0],  # upper < middle в первой строке
                'middle_band': [100.0, 101.0, 102.0],
                'lower_band': [99.0, 99.0, 100.0],
            }
        )

        is_valid, errors = g_data_validator.validate_bollinger_bands(df)
        assert not is_valid
        assert 'Invalid bollinger band relationship' in errors[0]

    def test_validate_rsi_data_valid(self):
        """Тест валидации корректных RSI данных."""
        series = polars.Series('rsi', [30.0, 50.0, 70.0, 80.0])

        is_valid, errors = g_data_validator.validate_rsi_data(series)
        assert is_valid
        assert len(errors) == 0

    def test_validate_rsi_data_invalid_range(self):
        """Тест валидации RSI данных с некорректным диапазоном."""
        series = polars.Series('rsi', [30.0, 50.0, 150.0, -10.0])  # Вне диапазона 0-100

        is_valid, errors = g_data_validator.validate_rsi_data(series)
        assert not is_valid
        assert 'RSI values out of range' in errors[0]

    def test_validate_rsi_data_with_nan(self):
        """Тест валидации RSI данных с NaN значениями."""
        series = polars.Series('rsi', [30.0, None, 70.0, 80.0])  # NaN значение

        is_valid, errors = g_data_validator.validate_rsi_data(series)
        assert not is_valid
        assert 'NaN values' in errors[0]

    def test_validate_data_consistency_valid(self):
        """Тест валидации согласованности корректных данных."""
        trades_df = polars.DataFrame(
            {
                'trade_id': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                'price': [
                    100.0,
                    101.0,
                    102.0,
                    103.0,
                    104.0,
                    105.0,
                    106.0,
                    107.0,
                    108.0,
                ],
                'quantity': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0],
                'datetime': [datetime.now(UTC) for _ in range(9)],
            }
        )

        candles_df = polars.DataFrame(
            {
                'start_trade_id': [1, 4, 7],
                'end_trade_id': [3, 6, 9],
                'open_price': [100.0, 103.0, 106.0],
                'high_price': [102.0, 105.0, 108.0],
                'low_price': [99.0, 102.0, 105.0],
                'close_price': [101.0, 104.0, 107.0],
                'volume': [100.0, 200.0, 300.0],
            }
        )

        is_valid, errors = g_data_validator.validate_data_consistency(
            trades_df=trades_df,
            candles_df=candles_df,
        )
        assert is_valid
        assert len(errors) == 0

    def test_validate_data_consistency_missing_trade_ids(self):
        """Тест валидации согласованности с отсутствующими trade_id."""
        trades_df = polars.DataFrame(
            {
                'trade_id': [1, 2, 3, 4, 5],  # Отсутствуют 6, 7, 8, 9
                'price': [100.0, 101.0, 102.0, 103.0, 104.0],
                'quantity': [1.0, 2.0, 3.0, 4.0, 5.0],
                'datetime': [datetime.now(UTC) for _ in range(5)],
            }
        )

        candles_df = polars.DataFrame(
            {
                'start_trade_id': [1, 4, 7],  # 7 отсутствует в trades
                'end_trade_id': [3, 6, 9],  # 6, 9 отсутствуют в trades
                'open_price': [100.0, 103.0, 106.0],
                'high_price': [102.0, 105.0, 108.0],
                'low_price': [99.0, 102.0, 105.0],
                'close_price': [101.0, 104.0, 107.0],
                'volume': [100.0, 200.0, 300.0],
            }
        )

        is_valid, errors = g_data_validator.validate_data_consistency(
            trades_df=trades_df,
            candles_df=candles_df,
        )
        assert not is_valid
        assert any('Missing start_trade_id' in error for error in errors)
        assert any('Missing end_trade_id' in error for error in errors)

    def test_validate_symbol_metadata_valid(self):
        """Тест валидации корректных метаданных символа."""
        metadata = {
            'symbol_id': 'BTC-USDT',
            'symbol_name': 'Bitcoin/USDT',
            'last_updated': datetime.now(UTC),
            'has_trades_data': True,
        }

        is_valid, errors = g_data_validator.validate_symbol_metadata(metadata)
        assert is_valid
        assert len(errors) == 0

    def test_validate_symbol_metadata_invalid(self):
        """Тест валидации некорректных метаданных символа."""
        metadata = {
            'symbol_id': '',  # Пустой symbol_id
            'symbol_name': 123,  # Неправильный тип
            'last_updated': '2023-01-01',  # Неправильный тип
        }

        is_valid, errors = g_data_validator.validate_symbol_metadata(metadata)
        assert not is_valid
        assert len(errors) > 0

    def test_get_validation_summary(self):
        """Тест получения сводки валидации."""
        validation_results = {
            'trades': (True, []),
            'candles': (False, ['Invalid candle logic']),
            'bollinger': (True, []),
            'rsi': (False, ['RSI values out of range']),
        }

        summary = g_data_validator.get_validation_summary(validation_results)

        assert summary['total_checks'] == 4
        assert summary['passed_checks'] == 2
        assert summary['failed_checks'] == 2
        assert summary['success_rate'] == 0.5
        assert len(summary['errors']) == 2


if __name__ == '__main__':
    # Запуск тестов
    pytest.main([__file__, '-v'])
