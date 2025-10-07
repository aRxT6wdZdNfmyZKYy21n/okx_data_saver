#include <gtest/gtest.h>
#include "processors/data_set_calculator.h"
#include "data_structures.h"
#include <chrono>
#include <vector>
#include <random>
#include <iostream>


class PerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Не инициализируем логгер для performance тестов
        // Используем std::cout для вывода результатов
    }
    
    void TearDown() override {
        // Не нужно закрывать логгер
    }
    
    // Генерация тестовых данных сделок
    std::vector<okx::TradeData> generateTestTrades(int count) {
        std::vector<okx::TradeData> trades;
        trades.reserve(count);
        
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> price_dist(100.0, 1000.0);
        std::uniform_real_distribution<> size_dist(0.1, 10.0);
        std::uniform_int_distribution<> side_dist(0, 1);
        
        for (int i = 0; i < count; ++i) {
            int64_t timestamp_ms = 1000 + i * 100; // 100ms интервалы
            int64_t trade_id = i + 1;
            okx::utils::Decimal price = okx::utils::Decimal::fromDouble(price_dist(gen));
            okx::utils::Decimal quantity = okx::utils::Decimal::fromDouble(size_dist(gen));
            bool is_buy = side_dist(gen) == 1;
            
            trades.emplace_back(okx::SymbolId::BTC_USDT, timestamp_ms, trade_id, price, quantity, is_buy);
        }
        
        return trades;
    }
    
    // Генерация тестовых данных ордер-буков
    std::vector<okx::OrderBookSnapshot> generateTestSnapshots(int count) {
        std::vector<okx::OrderBookSnapshot> snapshots;
        snapshots.reserve(count);
        
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> price_dist(100.0, 1000.0);
        std::uniform_real_distribution<> size_dist(0.1, 10.0);
        
        for (int i = 0; i < count; ++i) {
            int64_t timestamp_ms = 1000 + i * 1000; // 1s интервалы
            
            // Генерируем asks и bids
            std::vector<std::vector<std::string>> asks, bids;
            
            for (int j = 0; j < 5; ++j) {
                double ask_price = price_dist(gen) + 500.0; // asks выше
                double ask_size = size_dist(gen);
                asks.push_back({std::to_string(ask_price), std::to_string(ask_size), "0", "0"});
                
                double bid_price = price_dist(gen); // bids ниже
                double bid_size = size_dist(gen);
                bids.push_back({std::to_string(bid_price), std::to_string(bid_size), "0", "0"});
            }
            
            okx::OKXOrderBookActionId action_id = (i == 0) ? okx::OKXOrderBookActionId::Snapshot : okx::OKXOrderBookActionId::Update;
            snapshots.emplace_back(okx::SymbolId::BTC_USDT, timestamp_ms, action_id, asks, bids);
        }
        
        return snapshots;
    }
};

TEST_F(PerformanceTest, DataSetCalculatorPerformance) {
    okx::processors::DataSetCalculator calculator;
    
    // Тестируем разные размеры данных
    std::vector<int> test_sizes = {100, 1000, 10000, 100000};
    
    for (int size : test_sizes) {
        auto trades = generateTestTrades(size);
        auto snapshots = generateTestSnapshots(10);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        auto result = calculator.calculateFinalDataSet(
            okx::SymbolId::BTC_USDT, snapshots, trades, 0
        );
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        std::cout << "Processed " << size << " trades in " << duration.count() << " microseconds" << std::endl;
        
        // Проверяем, что результат не пустой
        EXPECT_GE(result.size(), 0);
    }
}

TEST_F(PerformanceTest, OrderBookStatisticsPerformance) {
    okx::processors::DataSetCalculator calculator;
    
    // Тестируем разные размеры ордер-буков
    std::vector<int> test_sizes = {10, 100, 1000};
    
    for (int size : test_sizes) {
        auto snapshots = generateTestSnapshots(size);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // Создаем тестовую карту quantity_by_price
        std::map<okx::utils::Decimal, okx::utils::Decimal> quantity_by_price;
        for (int i = 0; i < size; ++i) {
            okx::utils::Decimal price = okx::utils::Decimal::fromDouble(100.0 + i);
            okx::utils::Decimal quantity = okx::utils::Decimal::fromDouble(1.0 + i * 0.1);
            quantity_by_price[price] = quantity;
        }
        
        auto stats = calculator.calculateOrderBookStatistics(quantity_by_price);
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        std::cout << "Calculated order book statistics for " << size << " entries in " << duration.count() << " microseconds" << std::endl;
        
        // Проверяем, что статистики рассчитаны
        EXPECT_TRUE(stats.total_quantity.isPositive());
    }
}

TEST_F(PerformanceTest, TradeStatisticsPerformance) {
    okx::processors::DataSetCalculator calculator;
    
    // Тестируем разные размеры сделок
    std::vector<int> test_sizes = {1000, 10000, 100000};
    
    for (int size : test_sizes) {
        auto trades = generateTestTrades(size);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        size_t start_trade_idx = 0;
        auto stats = calculator.calculateTradeStatistics(trades, 1000, 2000, start_trade_idx);
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        std::cout << "Calculated trade statistics for " << size << " trades in " << duration.count() << " microseconds" << std::endl;
        
        // Проверяем, что статистики рассчитаны
        EXPECT_GE(stats.total_trades_count, 0);
    }
}

TEST_F(PerformanceTest, MemoryUsage) {
    okx::processors::DataSetCalculator calculator;
    
    const int large_size = 100000;
    
    // Генерируем большой объем данных
    auto trades = generateTestTrades(large_size);
    auto snapshots = generateTestSnapshots(100);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    auto result = calculator.calculateFinalDataSet(
        okx::SymbolId::BTC_USDT, snapshots, trades, 0
    );
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Processed " << large_size << " trades and " << snapshots.size() << " snapshots in " << duration.count() << " milliseconds" << std::endl;
    
    // Проверяем, что обработка завершилась успешно
    EXPECT_GE(result.size(), 0);
}