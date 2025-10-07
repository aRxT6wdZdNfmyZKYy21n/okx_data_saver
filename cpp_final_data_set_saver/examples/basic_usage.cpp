#include "utils/config.h"
#include "utils/logger.h"
#include "processors/data_set_calculator.h"
#include "database/database_connection.h"
#include "data_structures.h"
#include "utils/decimal.h"
#include <iostream>
#include <vector>
#include <chrono>
#include <thread>

using namespace okx;
using namespace okx::processors;
using namespace okx::utils;

int main() {
    try {
        std::cout << "=== OKX Final Data Set Saver - Basic Usage Example ===" << std::endl;
        
        // 1. Инициализация логгера
        std::cout << "1. Initializing logger..." << std::endl;
        Logger::getInstance().initialize("INFO", "", true, false);
        LOG_INFO("Logger initialized successfully");
        
        // 2. Загрузка конфигурации
        std::cout << "2. Loading configuration..." << std::endl;
        Config config;
        LOG_INFO("Configuration loaded: DB={}:{}", 
                 config.getDatabaseConfig().host, 
                 config.getDatabaseConfig().port);
        
        // 3. Создание калькулятора данных
        std::cout << "3. Creating data calculator..." << std::endl;
        DataSetCalculator calculator;
        
        // 4. Создание тестовых данных
        std::cout << "4. Creating test data..." << std::endl;
        
        // Тестовые сделки
        std::vector<TradeData> trades = {
            TradeData(SymbolId::BTC_USDT, 1000, 1, Decimal::fromString("45000.0"), Decimal::fromString("0.1"), true),
            TradeData(SymbolId::BTC_USDT, 1100, 2, Decimal::fromString("45100.0"), Decimal::fromString("0.2"), false),
            TradeData(SymbolId::BTC_USDT, 1200, 3, Decimal::fromString("45050.0"), Decimal::fromString("0.15"), true),
            TradeData(SymbolId::BTC_USDT, 1300, 4, Decimal::fromString("45200.0"), Decimal::fromString("0.05"), false)
        };
        
        // Тестовые снимки order book
        std::vector<OrderBookSnapshot> snapshots = {
            OrderBookSnapshot(SymbolId::BTC_USDT, 1000, OKXOrderBookActionId::Snapshot,
                {{"45050.0", "1.0", "0", "0"}}, // asks
                {{"44950.0", "1.0", "0", "0"}}  // bids
            ),
            OrderBookSnapshot(SymbolId::BTC_USDT, 2000, OKXOrderBookActionId::Update,
                {{"45100.0", "1.5", "0", "0"}}, // asks
                {{"44900.0", "2.0", "0", "0"}}  // bids
            )
        };
        
        LOG_INFO("Created {} trades and {} order book snapshots", 
                 trades.size(), snapshots.size());
        
        // 5. Расчет финального датасета
        std::cout << "5. Calculating final dataset..." << std::endl;
        auto records = calculator.calculateFinalDataSet(SymbolId::BTC_USDT, snapshots, trades, 0);
        
        std::cout << "Final Dataset Records:" << std::endl;
        std::cout << "  Number of records: " << records.size() << std::endl;
        
        for (size_t i = 0; i < records.size(); ++i) {
            const auto& record = records[i];
            std::cout << "  Record " << i << ":" << std::endl;
            std::cout << "    Symbol: " << static_cast<int>(record.symbol_id) << std::endl;
            std::cout << "    Data Set Index: " << record.data_set_idx << std::endl;
            std::cout << "    Record Index: " << record.record_idx << std::endl;
            std::cout << "    Trade Count: " << record.total_trades_count << std::endl;
            std::cout << "    Total Quantity: " << record.total_quantity.toString() << std::endl;
            std::cout << "    Total Volume: " << record.total_volume.toString() << std::endl;
            std::cout << "    Open Price: " << record.open_price.toString() << std::endl;
            std::cout << "    Close Price: " << record.close_price.toString() << std::endl;
            std::cout << "    High Price: " << record.high_price.toString() << std::endl;
            std::cout << "    Low Price: " << record.low_price.toString() << std::endl;
        }
        
        // 6. Демонстрация структурированного логирования
        std::cout << "6. Demonstrating structured logging..." << std::endl;
        std::map<std::string, std::string> fields = {
            {"symbol", "BTC_USDT"},
            {"records_count", std::to_string(records.size())},
            {"total_trades", std::to_string(trades.size())}
        };
        
        LOG_STRUCTURED("INFO", "Processing completed", fields);
        
        // 7. Демонстрация метрик производительности
        std::cout << "7. Demonstrating performance metrics..." << std::endl;
        auto start = std::chrono::high_resolution_clock::now();
        
        // Имитируем некоторую обработку
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        std::map<std::string, std::string> perf_metadata = {
            {"operation_type", "data_processing"},
            {"data_size", std::to_string(trades.size())}
        };
        
        LOG_PERFORMANCE("data_processing", duration.count() / 1000.0, perf_metadata);
        
        std::cout << "=== Example completed successfully! ===" << std::endl;
        
        // Очистка ресурсов
        Logger::getInstance().shutdown();
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
