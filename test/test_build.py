#!/usr/bin/env python3
"""
Simple test script to verify C++ Data Processor build and basic functionality.
"""
import asyncio
import logging
import sys
import time
from datetime import UTC, datetime

from utils.redis import g_redis_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO, format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s'
)
logger = logging.getLogger(__name__)


def test_import():
    """Test importing the C++ module."""
    logger.info('Testing C++ module import...')

    try:
        import cpp_data_processor

        logger.info('✅ C++ module imported successfully')
        return True
    except ImportError as e:
        logger.error(f'❌ Failed to import C++ module: {e}')
        return False
    except Exception as e:
        logger.error(f'❌ Unexpected error importing C++ module: {e}')
        return False


def test_basic_functionality():
    """Test basic functionality of the C++ module."""
    logger.info('Testing basic functionality...')

    try:
        import cpp_data_processor

        # Test DataProcessor creation
        processor = cpp_data_processor.DataProcessor()
        logger.info('✅ DataProcessor created successfully')

        # Test individual processors
        bollinger_processor = cpp_data_processor.BollingerBandsProcessor(20, 2.0)
        logger.info('✅ BollingerBandsProcessor created successfully')

        candles_processor = cpp_data_processor.CandlesProcessor()
        logger.info('✅ CandlesProcessor created successfully')

        rsi_calculator = cpp_data_processor.RSICalculator(14)
        logger.info('✅ RSICalculator created successfully')

        smoothing_processor = cpp_data_processor.SmoothingProcessor()
        logger.info('✅ SmoothingProcessor created successfully')

        extreme_lines_processor = cpp_data_processor.ExtremeLinesProcessor()
        logger.info('✅ ExtremeLinesProcessor created successfully')

        order_book_processor = cpp_data_processor.OrderBookProcessor()
        logger.info('✅ OrderBookProcessor created successfully')

        velocity_calculator = cpp_data_processor.VelocityCalculator()
        logger.info('✅ VelocityCalculator created successfully')

        return True

    except Exception as e:
        logger.error(f'❌ Basic functionality test failed: {e}')
        return False


def test_data_structures():
    """Test data structures."""
    logger.info('Testing data structures...')

    try:
        import cpp_data_processor

        # Test TradeData
        now = datetime.now(UTC)
        trade = cpp_data_processor.TradeData(12345, 50000.0, 0.1, True, now)
        logger.info(
            f'✅ TradeData created: trade_id={trade.trade_id}, price={trade.price}'
        )

        # Test CandleData
        candle = cpp_data_processor.CandleData(
            50000.0, 50100.0, 49900.0, 50050.0, 1000.0, 12345, 12350, 1000, 2000, 6
        )
        logger.info(
            f'✅ CandleData created: open={candle.open_price}, close={candle.close_price}'
        )

        # Test BollingerBands
        bollinger = cpp_data_processor.BollingerBands(20)
        bollinger.upper_band = [100.0, 101.0, 102.0]
        bollinger.middle_band = [100.0, 100.5, 101.0]
        bollinger.lower_band = [100.0, 100.0, 100.0]
        logger.info(f'✅ BollingerBands created: period={bollinger.timeperiod}')

        # Test RSIData
        rsi = cpp_data_processor.RSIData(14)
        rsi.rsi_values = [50.0, 55.0, 60.0]
        logger.info(f'✅ RSIData created: period={rsi.timeperiod}')

        # Test ProcessingResult
        result = cpp_data_processor.ProcessingResult(True, 'Test successful', 1.5)
        logger.info(
            f'✅ ProcessingResult created: success={result.success}, time={result.processing_time_seconds}'
        )

        return True

    except Exception as e:
        logger.error(f'❌ Data structures test failed: {e}')
        return False


def test_simple_calculations():
    """Test simple calculations."""
    logger.info('Testing simple calculations...')

    try:
        import cpp_data_processor

        # Test Bollinger Bands calculation
        bollinger_processor = cpp_data_processor.BollingerBandsProcessor(20, 2.0)

        # Create sample price data
        prices = [100.0 + i for i in range(50)]  # 50 prices from 100 to 149

        bollinger = bollinger_processor.calculate(prices)
        logger.info(
            f'✅ Bollinger Bands calculated: {len(bollinger.upper_band)} values'
        )

        # Test RSI calculation
        rsi_calculator = cpp_data_processor.RSICalculator(14)
        rsi = rsi_calculator.calculate(prices)
        logger.info(f'✅ RSI calculated: {len(rsi.rsi_values)} values')

        # Test candles processing
        candles_processor = cpp_data_processor.CandlesProcessor()

        # Create sample trade data
        trades = []
        base_time = datetime.now(UTC)
        for i in range(100):
            trade = cpp_data_processor.TradeData(
                i + 1,
                50000.0 + i,
                0.1,
                i % 2 == 0,
                base_time.replace(microsecond=i * 1000),
            )
            trades.append(trade)

        candles_map = candles_processor.process_trades(
            cpp_data_processor.SymbolId.BTC_USDT, trades
        )
        logger.info(f'✅ Candles processed: {len(candles_map)} intervals')

        return True

    except Exception as e:
        logger.error(f'❌ Simple calculations test failed: {e}')
        return False


def test_performance():
    """Test performance with larger dataset."""
    logger.info('Testing performance...')

    try:
        import cpp_data_processor

        # Create larger dataset
        trades = []
        base_time = datetime.now(UTC)
        for i in range(1000):  # 1000 trades
            trade = cpp_data_processor.TradeData(
                i + 1,
                50000.0 + (i % 100),
                0.1,
                i % 2 == 0,
                base_time.replace(microsecond=i * 1000),
            )
            trades.append(trade)

        # Test main processor
        processor = cpp_data_processor.DataProcessor()

        # Convert trades to Polars DataFrame format
        import polars as pl
        
        trades_df = pl.DataFrame({
            'trade_id': [t.trade_id for t in trades],
            'price': [t.price for t in trades],
            'quantity': [t.quantity for t in trades],
            'is_buy': [t.is_buy for t in trades],
            'datetime': [int(t.datetime.timestamp() * 1000) for t in trades],
        })

        start_time = time.time()
        result = processor.process_trades_data(
            cpp_data_processor.SymbolId.BTC_USDT, trades_df
        )
        end_time = time.time()

        processing_time = end_time - start_time

        logger.info('✅ Performance test completed:')
        logger.info(f'   Processing time: {processing_time:.3f}s')
        logger.info(f'   Success: {result.success}')
        logger.info(f'   Message: {result.error_message}')

        # Get statistics
        stats = processor.get_processing_stats()
        logger.info(f'   Stats: {stats}')

        return True

    except Exception as e:
        logger.error(f'❌ Performance test failed: {e}')
        return False


async def main():
    """Main test function."""
    logger.info('Starting C++ Data Processor Build Test')
    logger.info('=' * 50)

    await g_redis_manager.connect()

    tests = [
        ('Import Test', test_import),
        ('Basic Functionality Test', test_basic_functionality),
        ('Data Structures Test', test_data_structures),
        ('Simple Calculations Test', test_simple_calculations),
        ('Performance Test', test_performance),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        logger.info(f'\n--- {test_name} ---')
        try:
            if test_func():
                passed += 1
                logger.info(f'✅ {test_name} PASSED')
            else:
                logger.error(f'❌ {test_name} FAILED')
        except Exception as e:
            logger.error(f'❌ {test_name} FAILED with exception: {e}')

    logger.info(f'\n{"=" * 50}')
    logger.info(f'Test Results: {passed}/{total} tests passed')

    if passed == total:
        logger.info('🎉 All tests passed! C++ Data Processor is working correctly.')
        return 0
    else:
        logger.error(
            f'❌ {total - passed} tests failed. Please check the errors above.'
        )
        return 1


if __name__ == '__main__':
    sys.exit(
        asyncio.run(
            main(),
        ),
    )
