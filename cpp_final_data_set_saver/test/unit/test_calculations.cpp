#include <gtest/gtest.h>
#include "processors/data_set_calculator.h"
#include "data_structures.h"
#include "utils/decimal.h"
#include <vector>

using namespace okx;
using namespace okx::processors;
using namespace okx::utils;


class CalculationTest : public ::testing::Test {
protected:
    void SetUp() override {
        calculator_ = std::make_unique<DataSetCalculator>();
    }
    
    std::unique_ptr<DataSetCalculator> calculator_;
};

TEST_F(CalculationTest, CalculateFinalDataSetBasic) {
    // Создаем тестовые данные ордер-буков
    std::vector<OrderBookSnapshot> order_books = {
        OrderBookSnapshot(
            SymbolId::BTC_USDT, 1000, OKXOrderBookActionId::Snapshot,
            {{"50000.0", "1.0", "0", "0"}, {"50001.0", "2.0", "0", "0"}}, // asks
            {{"49999.0", "1.5", "0", "0"}, {"49998.0", "2.5", "0", "0"}}  // bids
        ),
        OrderBookSnapshot(
            SymbolId::BTC_USDT, 2000, OKXOrderBookActionId::Update,
            {{"50000.0", "0.5", "0", "0"}, {"50001.0", "1.5", "0", "0"}}, // asks
            {{"49999.0", "2.0", "0", "0"}, {"49998.0", "3.0", "0", "0"}}  // bids
        )
    };
    
    // Создаем тестовые данные сделок
    std::vector<TradeData> trades = {
        TradeData(SymbolId::BTC_USDT, 1500, 1, Decimal::fromString("50000.5"), Decimal::fromString("0.1"), true),
        TradeData(SymbolId::BTC_USDT, 1800, 2, Decimal::fromString("49999.5"), Decimal::fromString("0.2"), false)
    };
    
    auto records = calculator_->calculateFinalDataSet(SymbolId::BTC_USDT, order_books, trades, 0);
    
    // Должна быть создана одна запись (есть сделки в интервале)
    EXPECT_EQ(records.size(), 1);
    
    const auto& record = records[0];
    
    // Проверяем основные поля
    EXPECT_EQ(record.symbol_id, SymbolId::BTC_USDT);
    EXPECT_EQ(record.data_set_idx, 0);
    EXPECT_EQ(record.record_idx, 0);
    
    // Проверяем статистики по сделкам
    EXPECT_EQ(record.total_trades_count, 2);
    EXPECT_EQ(record.buy_trades_count, 1);
    EXPECT_TRUE(record.buy_quantity == Decimal::fromString("0.1"));
    EXPECT_TRUE(record.total_quantity == Decimal::fromString("0.3"));
    
    // Проверяем цены
    EXPECT_TRUE(record.open_price == Decimal::fromString("50000.5"));
    EXPECT_TRUE(record.close_price == Decimal::fromString("49999.5"));
    EXPECT_TRUE(record.high_price == Decimal::fromString("50000.5"));
    EXPECT_TRUE(record.low_price == Decimal::fromString("49999.5"));
    
    // Проверяем временные метки
    EXPECT_EQ(record.start_timestamp_ms, 1500);
    EXPECT_EQ(record.end_timestamp_ms, 2000);
    EXPECT_EQ(record.start_trade_id, 1);
    EXPECT_EQ(record.end_trade_id, 2);
}

TEST_F(CalculationTest, CalculateFinalDataSetNoTrades) {
    // Создаем тестовые данные ордер-буков без сделок
    std::vector<OrderBookSnapshot> order_books = {
        OrderBookSnapshot(
            SymbolId::BTC_USDT, 1000, OKXOrderBookActionId::Snapshot,
            {{"50000.0", "1.0", "0", "0"}}, // asks
            {{"49999.0", "1.5", "0", "0"}}  // bids
        ),
        OrderBookSnapshot(
            SymbolId::BTC_USDT, 2000, OKXOrderBookActionId::Update,
            {{"50000.0", "0.5", "0", "0"}}, // asks
            {{"49999.0", "2.0", "0", "0"}}  // bids
        )
    };
    
    std::vector<TradeData> trades; // Пустой список сделок
    
    auto records = calculator_->calculateFinalDataSet(SymbolId::BTC_USDT, order_books, trades, 0);
    
    // Не должно быть записей (нет сделок)
    EXPECT_EQ(records.size(), 0);
}

TEST_F(CalculationTest, CalculateFinalDataSetMultipleIntervals) {
    // Создаем тестовые данные с несколькими интервалами
    std::vector<OrderBookSnapshot> order_books = {
        OrderBookSnapshot(
            SymbolId::BTC_USDT, 1000, OKXOrderBookActionId::Snapshot,
            {{"50000.0", "1.0", "0", "0"}}, // asks
            {{"49999.0", "1.5", "0", "0"}}  // bids
        ),
        OrderBookSnapshot(
            SymbolId::BTC_USDT, 2000, OKXOrderBookActionId::Update,
            {{"50000.0", "0.5", "0", "0"}}, // asks
            {{"49999.0", "2.0", "0", "0"}}  // bids
        ),
        OrderBookSnapshot(
            SymbolId::BTC_USDT, 3000, OKXOrderBookActionId::Update,
            {{"50000.0", "0.3", "0", "0"}}, // asks
            {{"49999.0", "2.5", "0", "0"}}  // bids
        )
    };
    
    // Создаем сделки в разных интервалах
    std::vector<TradeData> trades = {
        TradeData(SymbolId::BTC_USDT, 1500, 1, Decimal::fromString("50000.5"), Decimal::fromString("0.1"), true),  // Первый интервал
        TradeData(SymbolId::BTC_USDT, 2500, 2, Decimal::fromString("49999.5"), Decimal::fromString("0.2"), false)  // Второй интервал
    };
    
    auto records = calculator_->calculateFinalDataSet(SymbolId::BTC_USDT, order_books, trades, 0);
    
    // Должны быть созданы две записи (по одной для каждого интервала с сделками)
    EXPECT_EQ(records.size(), 2);
    
    // Проверяем первую запись
    const auto& record1 = records[0];
    EXPECT_EQ(record1.record_idx, 0);
    EXPECT_EQ(record1.total_trades_count, 1);
    EXPECT_EQ(record1.start_trade_id, 1);
    EXPECT_EQ(record1.end_trade_id, 1);
    
    // Проверяем вторую запись
    const auto& record2 = records[1];
    EXPECT_EQ(record2.record_idx, 1);
    EXPECT_EQ(record2.total_trades_count, 1);
    EXPECT_EQ(record2.start_trade_id, 2);
    EXPECT_EQ(record2.end_trade_id, 2);
}

TEST_F(CalculationTest, OrderBookStatisticsCalculation) {
    // Тестируем расчет статистик ордер-бука
    std::map<Decimal, Decimal> quantity_by_price = {
        {Decimal::fromString("50000.0"), Decimal::fromString("1.0")},
        {Decimal::fromString("50001.0"), Decimal::fromString("2.0")},
        {Decimal::fromString("50002.0"), Decimal::fromString("0.5")}
    };
    
    auto stats = calculator_->calculateOrderBookStatistics(quantity_by_price);
    
    // Проверяем общие количества
    EXPECT_TRUE(stats.total_quantity == Decimal::fromString("3.5"));
    
    // Проверяем максимальные и минимальные значения
    EXPECT_TRUE(stats.max_price == Decimal::fromString("50002.0"));
    EXPECT_TRUE(stats.min_price == Decimal::fromString("50000.0"));
    EXPECT_TRUE(stats.max_quantity == Decimal::fromString("2.0"));
    EXPECT_TRUE(stats.min_quantity == Decimal::fromString("0.5"));
    
    // Проверяем объемы
    EXPECT_TRUE(stats.max_volume == Decimal::fromString("100002.0")); // 50001.0 * 2.0
    EXPECT_TRUE(stats.min_volume == Decimal::fromString("25001.0"));  // 50002.0 * 0.5
}

TEST_F(CalculationTest, TradeStatisticsCalculation) {
    // Тестируем расчет статистик сделок
    std::vector<TradeData> trades = {
        TradeData(SymbolId::BTC_USDT, 1500, 1, Decimal::fromString("50000.0"), Decimal::fromString("0.1"), true),
        TradeData(SymbolId::BTC_USDT, 1600, 2, Decimal::fromString("50001.0"), Decimal::fromString("0.2"), false),
        TradeData(SymbolId::BTC_USDT, 1700, 3, Decimal::fromString("49999.0"), Decimal::fromString("0.3"), true)
    };
    
    size_t start_trade_idx = 0;
    auto stats = calculator_->calculateTradeStatistics(trades, 1500, 2000, start_trade_idx);
    
    // Проверяем общие статистики
    EXPECT_EQ(stats.total_trades_count, 3);
    EXPECT_TRUE(stats.total_quantity == Decimal::fromString("0.6"));
    
    // Проверяем статистики по покупкам
    EXPECT_EQ(stats.buy_trades_count, 2);
    EXPECT_TRUE(stats.buy_quantity == Decimal::fromString("0.4"));
    
    // Проверяем цены
    EXPECT_TRUE(stats.open_price == Decimal::fromString("50000.0"));
    EXPECT_TRUE(stats.close_price == Decimal::fromString("49999.0"));
    EXPECT_TRUE(stats.high_price == Decimal::fromString("50001.0"));
    EXPECT_TRUE(stats.low_price == Decimal::fromString("49999.0"));
    
    // Проверяем ID сделок
    EXPECT_EQ(stats.start_trade_id, 1);
    EXPECT_EQ(stats.end_trade_id, 3);
    EXPECT_EQ(stats.start_timestamp_ms, 1500);
}

TEST_F(CalculationTest, InsufficientOrderBooks) {
    // Тест с недостаточным количеством ордер-буков
    std::vector<OrderBookSnapshot> order_books = {
        OrderBookSnapshot(
            SymbolId::BTC_USDT, 1000, OKXOrderBookActionId::Snapshot,
            {{"50000.0", "1.0", "0", "0"}}, // asks
            {{"49999.0", "1.5", "0", "0"}}  // bids
        )
    };
    
    std::vector<TradeData> trades = {
        TradeData(SymbolId::BTC_USDT, 1500, 1, Decimal::fromString("50000.0"), Decimal::fromString("0.1"), true)
    };
    
    auto records = calculator_->calculateFinalDataSet(SymbolId::BTC_USDT, order_books, trades, 0);
    
    // Не должно быть записей (недостаточно ордер-буков)
    EXPECT_EQ(records.size(), 0);
}

TEST_F(CalculationTest, EmptyData) {
    // Тест с пустыми данными
    std::vector<OrderBookSnapshot> empty_order_books;
    std::vector<TradeData> empty_trades;
    
    auto records = calculator_->calculateFinalDataSet(SymbolId::BTC_USDT, empty_order_books, empty_trades, 0);
    
    EXPECT_EQ(records.size(), 0);
}

TEST_F(CalculationTest, OrderBookStateInitialization) {
    // Тестируем инициализацию состояния ордер-бука
    OrderBookSnapshot snapshot(
        SymbolId::BTC_USDT, 1000, OKXOrderBookActionId::Snapshot,
        {{"50000.0", "1.0", "0", "0"}, {"50001.0", "2.0", "0", "0"}}, // asks
        {{"49999.0", "1.5", "0", "0"}, {"49998.0", "2.5", "0", "0"}}  // bids
    );
    
    DataSetCalculator::OrderBookState state;
    calculator_->initializeOrderBookState(state, snapshot);
    
    // Проверяем, что состояние инициализировано
    EXPECT_TRUE(state.initialized);
    EXPECT_EQ(state.ask_quantity_by_price.size(), 2);
    EXPECT_EQ(state.bid_quantity_by_price.size(), 2);
    
    // Проверяем конкретные значения
    EXPECT_TRUE(state.ask_quantity_by_price[Decimal::fromString("50000.0")] == Decimal::fromString("1.0"));
    EXPECT_TRUE(state.ask_quantity_by_price[Decimal::fromString("50001.0")] == Decimal::fromString("2.0"));
    EXPECT_TRUE(state.bid_quantity_by_price[Decimal::fromString("49999.0")] == Decimal::fromString("1.5"));
    EXPECT_TRUE(state.bid_quantity_by_price[Decimal::fromString("49998.0")] == Decimal::fromString("2.5"));
}