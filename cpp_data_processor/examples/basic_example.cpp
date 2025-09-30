#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include "../include/main_processor.h"

using namespace okx_data_processor;

int main() {
    std::cout << "C++ Data Processor Basic Example" << std::endl;
    std::cout << "=================================" << std::endl;
    
    // Create data processor
    DataProcessor processor;
    
    // Generate sample trade data
    std::vector<TradeData> trades;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> price_dist(50000.0, 60000.0);
    std::uniform_real_distribution<double> quantity_dist(0.001, 1.0);
    std::uniform_int_distribution<int> buy_dist(0, 1);
    
    auto start_time = std::chrono::system_clock::now();
    
    for (int i = 0; i < 1000; ++i) {
        double price = price_dist(gen);
        double quantity = quantity_dist(gen);
        bool is_buy = buy_dist(gen) == 1;
        auto trade_time = start_time + std::chrono::milliseconds(i * 100);
        
        trades.emplace_back(i, price, quantity, is_buy, trade_time);
    }
    
    std::cout << "Generated " << trades.size() << " sample trades" << std::endl;
    
    // Test Bollinger Bands processor
    std::cout << "\nTesting Bollinger Bands Processor:" << std::endl;
    BollingerBandsProcessor bollinger_processor(20, 2.0);
    
    auto bollinger_start = std::chrono::high_resolution_clock::now();
    BollingerBands bollinger = bollinger_processor.calculate_from_trades(trades);
    auto bollinger_end = std::chrono::high_resolution_clock::now();
    
    auto bollinger_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        bollinger_end - bollinger_start);
    
    std::cout << "Bollinger Bands calculated in " << bollinger_duration.count() << " microseconds" << std::endl;
    std::cout << "Upper band size: " << bollinger.upper_band.size() << std::endl;
    std::cout << "Middle band size: " << bollinger.middle_band.size() << std::endl;
    std::cout << "Lower band size: " << bollinger.lower_band.size() << std::endl;
    
    // Test Candles processor
    std::cout << "\nTesting Candles Processor:" << std::endl;
    CandlesProcessor candles_processor;
    
    auto candles_start = std::chrono::high_resolution_clock::now();
    auto candles_map = candles_processor.process_trades(SymbolId::BTC_USDT, trades);
    auto candles_end = std::chrono::high_resolution_clock::now();
    
    auto candles_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        candles_end - candles_start);
    
    std::cout << "Candles processed in " << candles_duration.count() << " microseconds" << std::endl;
    std::cout << "Number of intervals processed: " << candles_map.size() << std::endl;
    
    for (const auto& pair : candles_map) {
        std::cout << "  " << pair.first << ": " << pair.second.size() << " candles" << std::endl;
    }
    
    // Test main processor
    std::cout << "\nTesting Main Data Processor:" << std::endl;
    
    // Convert trades to Python-like format for testing
    pybind11::dict trades_dict;
    pybind11::list trade_ids, prices, quantities, is_buys, datetimes;
    
    for (const auto& trade : trades) {
        trade_ids.append(trade.trade_id);
        prices.append(trade.price);
        quantities.append(trade.quantity);
        is_buys.append(trade.is_buy);
        
        auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            trade.datetime.time_since_epoch()).count();
        datetimes.append(timestamp_ms);
    }
    
    trades_dict["trade_id"] = trade_ids;
    trades_dict["price"] = prices;
    trades_dict["quantity"] = quantities;
    trades_dict["is_buy"] = is_buys;
    trades_dict["datetime"] = datetimes;
    
    auto main_start = std::chrono::high_resolution_clock::now();
    ProcessingResult result = processor.process_trades_data(SymbolId::BTC_USDT, trades_dict);
    auto main_end = std::chrono::high_resolution_clock::now();
    
    auto main_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        main_end - main_start);
    
    std::cout << "Main processor completed in " << main_duration.count() << " microseconds" << std::endl;
    std::cout << "Success: " << (result.success ? "true" : "false") << std::endl;
    std::cout << "Message: " << result.error_message << std::endl;
    std::cout << "Processing time: " << result.processing_time_seconds << " seconds" << std::endl;
    
    // Get processing statistics
    auto stats = processor.get_processing_stats();
    std::cout << "\nProcessing Statistics:" << std::endl;
    std::cout << "Total trades processed: " << stats["total_trades_processed"].cast<uint64_t>() << std::endl;
    std::cout << "Total processing time (ms): " << stats["total_processing_time_ms"].cast<uint64_t>() << std::endl;
    std::cout << "Successful operations: " << stats["successful_operations"].cast<uint64_t>() << std::endl;
    std::cout << "Failed operations: " << stats["failed_operations"].cast<uint64_t>() << std::endl;
    std::cout << "Average processing time (ms): " << stats["average_processing_time_ms"].cast<double>() << std::endl;
    
    std::cout << "\nExample completed successfully!" << std::endl;
    
    return 0;
}
