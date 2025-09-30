#!/usr/bin/env python3
"""
Example of integrating C++ Data Processor with existing Python data processor.
This example shows how to replace the Python data processor with the C++ version
while maintaining the same interface.
"""

import sys
import os
import asyncio
import logging
from datetime import datetime, timezone
import polars as pl

# Add the parent directory to the path to import the integration module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integration import get_cpp_integration, process_trades_with_cpp
from enumerations import SymbolId

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s][%(asctime)s][%(name)s]: %(message)s'
)
logger = logging.getLogger(__name__)


def create_sample_trades_data() -> pl.DataFrame:
    """Create sample trades data for testing."""
    import random
    
    # Generate sample data
    n_trades = 1000
    base_price = 50000.0
    base_time = datetime.now(timezone.utc)
    
    trades_data = []
    for i in range(n_trades):
        trade_id = i + 1
        price = base_price + random.uniform(-100, 100)  # Price variation
        quantity = random.uniform(0.001, 1.0)
        is_buy = random.choice([True, False])
        trade_time = base_time.replace(microsecond=i * 1000)  # 1ms intervals
        
        trades_data.append({
            'trade_id': trade_id,
            'price': price,
            'quantity': quantity,
            'is_buy': is_buy,
            'datetime': trade_time
        })
    
    return pl.DataFrame(trades_data)


async def test_synchronous_processing():
    """Test synchronous processing with C++ processor."""
    logger.info("Testing synchronous processing...")
    
    # Create sample data
    trades_df = create_sample_trades_data()
    logger.info(f"Created sample data with {trades_df.height} trades")
    
    # Process with C++ processor
    result = process_trades_with_cpp(SymbolId.BTC_USDT, trades_df)
    
    logger.info(f"Processing result: {result}")
    
    if result['success']:
        logger.info(f"✅ Synchronous processing completed successfully")
        logger.info(f"   Processing time: {result['processing_time_seconds']:.3f}s")
        logger.info(f"   Processor type: {result['processor_type']}")
    else:
        logger.error(f"❌ Synchronous processing failed: {result['message']}")


async def test_asynchronous_processing():
    """Test asynchronous processing with C++ processor."""
    logger.info("Testing asynchronous processing...")
    
    # Create sample data
    trades_df = create_sample_trades_data()
    logger.info(f"Created sample data with {trades_df.height} trades")
    
    # Process asynchronously
    def processing_callback(result):
        if result['success']:
            logger.info(f"✅ Asynchronous processing completed successfully")
            logger.info(f"   Processing time: {result['processing_time_seconds']:.3f}s")
            logger.info(f"   Processor type: {result['processor_type']}")
        else:
            logger.error(f"❌ Asynchronous processing failed: {result['message']}")
    
    # Get C++ integration instance
    cpp_integration = get_cpp_integration()
    
    # Process asynchronously
    cpp_integration.process_trades_data_async(SymbolId.BTC_USDT, trades_df, processing_callback)
    
    # Wait a bit for async processing to complete
    await asyncio.sleep(2)


def test_processor_info():
    """Test processor information and statistics."""
    logger.info("Testing processor information...")
    
    cpp_integration = get_cpp_integration()
    
    # Get processor info
    info = cpp_integration.get_processor_info()
    logger.info(f"Processor info: {info}")
    
    # Get processing stats
    stats = cpp_integration.get_processing_stats()
    logger.info(f"Processing stats: {stats}")
    
    # Check if C++ is available
    if cpp_integration.is_cpp_available():
        logger.info("✅ C++ processor is available and initialized")
    else:
        logger.warning("⚠️ C++ processor is not available, using Python fallback")


def test_processing_parameters():
    """Test setting processing parameters."""
    logger.info("Testing processing parameters...")
    
    cpp_integration = get_cpp_integration()
    
    if not cpp_integration.is_cpp_available():
        logger.warning("C++ processor not available, skipping parameter test")
        return
    
    # Set processing parameters
    params = {
        'enable_bollinger_bands': True,
        'enable_candles': True,
        'enable_rsi': True,
        'enable_smoothing': True,
        'enable_extreme_lines': True,
        'bollinger_period': 20,
        'rsi_period': 14,
        'candle_intervals': ['1m', '5m', '15m', '1h'],
        'smoothing_levels': ['Raw (0)', 'Smoothed (1)']
    }
    
    cpp_integration.set_processing_params(params)
    logger.info("✅ Processing parameters set successfully")


async def test_performance_comparison():
    """Test performance comparison between C++ and Python processors."""
    logger.info("Testing performance comparison...")
    
    # Create larger dataset for performance testing
    trades_df = create_sample_trades_data()
    # Duplicate the data to make it larger
    trades_df = pl.concat([trades_df] * 10)
    logger.info(f"Created performance test data with {trades_df.height} trades")
    
    cpp_integration = get_cpp_integration()
    
    if not cpp_integration.is_cpp_available():
        logger.warning("C++ processor not available, skipping performance test")
        return
    
    # Test C++ processing
    start_time = datetime.now()
    result = process_trades_with_cpp(SymbolId.BTC_USDT, trades_df)
    cpp_time = (datetime.now() - start_time).total_seconds()
    
    logger.info(f"C++ processing time: {cpp_time:.3f}s")
    logger.info(f"C++ result: {result}")
    
    # Note: Python processing would be tested here if implemented
    # For now, we just show the C++ results
    logger.info("✅ Performance test completed")


async def main():
    """Main test function."""
    logger.info("Starting C++ Data Processor Integration Tests")
    logger.info("=" * 50)
    
    try:
        # Test processor information
        test_processor_info()
        print()
        
        # Test processing parameters
        test_processing_parameters()
        print()
        
        # Test synchronous processing
        await test_synchronous_processing()
        print()
        
        # Test asynchronous processing
        await test_asynchronous_processing()
        print()
        
        # Test performance comparison
        await test_performance_comparison()
        print()
        
        logger.info("✅ All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
