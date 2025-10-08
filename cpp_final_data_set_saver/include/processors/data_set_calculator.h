#pragma once

#include "data_structures.h"
#include <vector>
#include <map>
#include <optional>

namespace okx {
namespace processors {

class DataSetCalculator {
public:
    DataSetCalculator() = default;
    ~DataSetCalculator() = default;
    
    // Основная функция расчета финального датасета (соответствует Python save_final_data_set)
    std::vector<OKXDataSetRecordData> calculateFinalDataSet(
        SymbolId symbol_id,
        const std::vector<OrderBookSnapshot>& order_book_snapshots,
        const std::vector<TradeData>& trades,
        int32_t data_set_idx
    );
    
public:
    // Структуры для внутренних вычислений (публичные для тестирования)
    struct OrderBookState {
        std::map<utils::Decimal, utils::Decimal> ask_quantity_by_price;
        std::map<utils::Decimal, utils::Decimal> bid_quantity_by_price;
        bool initialized = false;
    };
    
    struct OrderBookStatistics {
        utils::Decimal total_quantity = utils::Decimal(static_cast<int64_t>(0));
        utils::Decimal total_volume = utils::Decimal(static_cast<int64_t>(0));
        utils::Decimal max_price = utils::Decimal(static_cast<int64_t>(0));
        utils::Decimal max_quantity = utils::Decimal(static_cast<int64_t>(0));
        utils::Decimal max_volume = utils::Decimal(static_cast<int64_t>(0));
        utils::Decimal min_price = utils::Decimal(static_cast<int64_t>(0));
        utils::Decimal min_quantity = utils::Decimal(static_cast<int64_t>(0));
        utils::Decimal min_volume = utils::Decimal(static_cast<int64_t>(0));
    };
    
    struct TradeStatistics {
        utils::Decimal buy_quantity = utils::Decimal(static_cast<int64_t>(0));
        int32_t buy_trades_count = 0;
        utils::Decimal buy_volume = utils::Decimal(static_cast<int64_t>(0));
        std::optional<utils::Decimal> close_price = std::nullopt;
        std::optional<utils::Decimal> high_price = std::nullopt;
        std::optional<utils::Decimal> low_price = std::nullopt;
        std::optional<utils::Decimal> open_price = std::nullopt;
        std::optional<int64_t> start_trade_id = std::nullopt;
        std::optional<int64_t> end_trade_id = std::nullopt;
        std::optional<int64_t> start_timestamp_ms = std::nullopt;
        utils::Decimal total_quantity = utils::Decimal(static_cast<int64_t>(0));
        int32_t total_trades_count = 0;
        utils::Decimal total_volume = utils::Decimal(static_cast<int64_t>(0));
    };

private:
    
public:
    // Вспомогательные функции (публичные для тестирования)
    void initializeOrderBookState(OrderBookState& state, const OrderBookSnapshot& snapshot);
    void updateOrderBookState(OrderBookState& state, const OrderBookSnapshot& update);
    OrderBookStatistics calculateOrderBookStatistics(const std::map<utils::Decimal, utils::Decimal>& quantity_by_price);
    TradeStatistics calculateTradeStatistics(const std::vector<TradeData>& trades, 
                                           int64_t start_timestamp_ms, 
                                           int64_t end_timestamp_ms,
                                           size_t& start_trade_idx);
    
    // Валидация данных
    bool isValidPrice(const utils::Decimal& price);
    bool isValidQuantity(const utils::Decimal& quantity);

private:
};

} // namespace processors
} // namespace okx
