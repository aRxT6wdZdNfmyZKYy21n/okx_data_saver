#include <iostream>
#include <vector>
#include <chrono>
#include <cassert>
#include "cpp_data_processor.h"

using namespace okx_data_processor;

void test_bollinger_bands() {
    std::cout << "Testing Bollinger Bands Processor..." << std::endl;
    
    // Create test data
    std::vector<double> prices = {100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 109.0,
                                 111.0, 110.0, 112.0, 114.0, 113.0, 115.0, 117.0, 116.0, 118.0, 120.0,
                                 119.0, 121.0, 123.0, 122.0, 124.0, 126.0, 125.0, 127.0, 129.0, 128.0};
    
    BollingerBandsProcessor processor(20, 2.0);
    BollingerBands result = processor.calculate(prices);
    
    // Check that we have the right number of bands
    assert(result.upper_band.size() == prices.size());
    assert(result.middle_band.size() == prices.size());
    assert(result.lower_band.size() == prices.size());
    
    // Check that the first few values are NaN (not enough data)
    for (size_t i = 0; i < 19; ++i) {
        assert(std::isnan(result.upper_band[i]));
        assert(std::isnan(result.middle_band[i]));
        assert(std::isnan(result.lower_band[i]));
    }
    
    // Check that we have valid values for the last few points
    for (size_t i = 19; i < prices.size(); ++i) {
        assert(!std::isnan(result.upper_band[i]));
        assert(!std::isnan(result.middle_band[i]));
        assert(!std::isnan(result.lower_band[i]));
        
        // Upper band should be higher than middle band
        assert(result.upper_band[i] > result.middle_band[i]);
        // Lower band should be lower than middle band
        assert(result.lower_band[i] < result.middle_band[i]);
    }
    
    std::cout << "  ✓ Bollinger Bands test passed" << std::endl;
}

void test_candles_processor() {
    std::cout << "Testing Candles Processor..." << std::endl;
    
    // Create test trade data
    std::vector<TradeData> trades;
    auto base_time = std::chrono::system_clock::now();
    
    // Create trades over 5 minutes (300 seconds)
    for (int i = 0; i < 300; ++i) {
        double price = 50000.0 + (i % 100); // Price varies between 50000-50099
        double quantity = 0.1;
        bool is_buy = (i % 2) == 0;
        auto trade_time = base_time + std::chrono::seconds(i);
        
        trades.emplace_back(i, price, quantity, is_buy, trade_time);
    }
    
    CandlesProcessor processor;
    
    // Test 1-minute candles
    auto candles_1m = processor.process_trades_for_interval(SymbolId::BTC_USDT, trades, "1m");
    
    // Should have 5 candles (300 seconds / 60 seconds per minute)
    assert(candles_1m.size() == 5);
    
    // Check that candles are sorted by start_trade_id
    for (size_t i = 1; i < candles_1m.size(); ++i) {
        assert(candles_1m[i-1].start_trade_id < candles_1m[i].start_trade_id);
    }
    
    // Check that each candle has valid data
    for (const auto& candle : candles_1m) {
        assert(candle.high_price >= candle.low_price);
        assert(candle.high_price >= candle.open_price);
        assert(candle.high_price >= candle.close_price);
        assert(candle.low_price <= candle.open_price);
        assert(candle.low_price <= candle.close_price);
        assert(candle.volume > 0);
        assert(candle.trades_count > 0);
    }
    
    std::cout << "  ✓ Candles Processor test passed" << std::endl;
}

void test_data_structures() {
    std::cout << "Testing Data Structures..." << std::endl;
    
    // Test TradeData
    auto now = std::chrono::system_clock::now();
    TradeData trade(12345, 50000.0, 0.1, true, now);
    
    assert(trade.trade_id == 12345);
    assert(trade.price == 50000.0);
    assert(trade.quantity == 0.1);
    assert(trade.is_buy == true);
    assert(trade.datetime == now);
    
    // Test CandleData
    CandleData candle(50000.0, 50100.0, 49900.0, 50050.0, 1000.0, 12345, 12350, 1000, 2000, 6);
    
    assert(candle.open_price == 50000.0);
    assert(candle.high_price == 50100.0);
    assert(candle.low_price == 49900.0);
    assert(candle.close_price == 50050.0);
    assert(candle.volume == 1000.0);
    assert(candle.start_trade_id == 12345);
    assert(candle.end_trade_id == 12350);
    assert(candle.trades_count == 6);
    
    // Test BollingerBands
    BollingerBands bollinger(20);
    bollinger.upper_band = {100.0, 101.0, 102.0};
    bollinger.middle_band = {100.0, 100.5, 101.0};
    bollinger.lower_band = {100.0, 100.0, 100.0};
    
    assert(bollinger.upper_band.size() == 3);
    assert(bollinger.middle_band.size() == 3);
    assert(bollinger.lower_band.size() == 3);
    assert(bollinger.timeperiod == 20);
    
    std::cout << "  ✓ Data Structures test passed" << std::endl;
}

void test_data_processor() {
    std::cout << "Testing Data Processor..." << std::endl;
    
    DataProcessor processor;
    
    // Test processing parameters
    pybind11::dict params;
    params["enable_bollinger_bands"] = true;
    params["enable_candles"] = true;
    params["bollinger_period"] = 14;
    
    processor.set_processing_params(params);
    
    // Test statistics
    auto stats = processor.get_processing_stats();
    assert(stats["total_trades_processed"].cast<uint64_t>() == 0);
    assert(stats["successful_operations"].cast<uint64_t>() == 0);
    assert(stats["failed_operations"].cast<uint64_t>() == 0);
    
    std::cout << "  ✓ Data Processor test passed" << std::endl;
}

int main() {
    std::cout << "Running C++ Data Processor Tests" << std::endl;
    std::cout << "=================================" << std::endl;
    
    try {
        test_data_structures();
        test_bollinger_bands();
        test_candles_processor();
        test_data_processor();
        
        std::cout << "\nAll tests passed successfully!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cout << "\nTest failed with exception: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cout << "\nTest failed with unknown exception" << std::endl;
        return 1;
    }
}
