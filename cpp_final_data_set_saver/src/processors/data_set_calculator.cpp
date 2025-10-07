#include "processors/data_set_calculator.h"
#include "utils/logger.h"
#include <algorithm>
#include <stdexcept>

namespace okx {
namespace processors {

std::vector<OKXDataSetRecordData> DataSetCalculator::calculateFinalDataSet(
    SymbolId symbol_id,
    const std::vector<OrderBookSnapshot>& order_book_snapshots,
    const std::vector<TradeData>& trades,
    int32_t data_set_idx) {
    
    std::vector<OKXDataSetRecordData> records;
    
    if (order_book_snapshots.size() < 2) {
        LOG_INFO("There are only {} order book snapshots; skipping final data set saving.", 
                 order_book_snapshots.size());
        return records;
    }
    
    // Создаем объединенный список ордер-буков (как в Python)
    std::vector<OrderBookSnapshot> order_books;
    order_books.push_back(order_book_snapshots[0]); // start snapshot
    order_books.insert(order_books.end(), order_book_snapshots.begin() + 1, order_book_snapshots.end());
    
    OrderBookState order_book_state;
    size_t start_trade_idx = 0;
    int32_t record_idx = 0;
    
    // Переменные для статистик начального ордер-бука (как в Python)
    // Используем статические константы класса для избежания проблем с инициализацией
    utils::Decimal start_asks_total_quantity = utils::Decimal::ZERO;
    utils::Decimal start_asks_total_volume = utils::Decimal::ZERO;
    utils::Decimal max_start_ask_price = utils::Decimal::ZERO;
    utils::Decimal max_start_ask_quantity = utils::Decimal::ZERO;
    utils::Decimal max_start_ask_volume = utils::Decimal::ZERO;
    utils::Decimal min_start_ask_price = utils::Decimal::ZERO;
    utils::Decimal min_start_ask_quantity = utils::Decimal::ZERO;
    utils::Decimal min_start_ask_volume = utils::Decimal::ZERO;
    
    utils::Decimal start_bids_total_quantity = utils::Decimal::ZERO;
    utils::Decimal start_bids_total_volume = utils::Decimal::ZERO;
    utils::Decimal max_start_bid_price = utils::Decimal::ZERO;
    utils::Decimal max_start_bid_quantity = utils::Decimal::ZERO;
    utils::Decimal max_start_bid_volume = utils::Decimal::ZERO;
    utils::Decimal min_start_bid_price = utils::Decimal::ZERO;
    utils::Decimal min_start_bid_quantity = utils::Decimal::ZERO;
    utils::Decimal min_start_bid_volume = utils::Decimal::ZERO;
    
    // Переменная для start_timestamp_ms (как в Python)
    int64_t start_timestamp_ms = 0;
    
    // Обрабатываем каждый интервал между ордер-буками
    for (size_t current_order_book_idx = 0; current_order_book_idx < order_books.size() - 1; ++current_order_book_idx) {
        if (current_order_book_idx % 100 == 0) {
            LOG_INFO("Processed {} / {} order books", current_order_book_idx, order_books.size());
        }
        
        const auto& current_order_book = order_books[current_order_book_idx];
        const auto& next_order_book = order_books[current_order_book_idx + 1];
        
        int64_t current_timestamp_ms = current_order_book.timestamp_ms;
        int64_t next_timestamp_ms = next_order_book.timestamp_ms;
        
        // Инициализируем состояние ордер-бука при первом снимке
        if (!order_book_state.initialized) {
            if (current_order_book.action_id != OKXOrderBookActionId::Snapshot) {
                throw std::runtime_error("First order book must be a snapshot");
            }
            initializeOrderBookState(order_book_state, current_order_book);
        } else {
            if (current_order_book.action_id != OKXOrderBookActionId::Update) {
                throw std::runtime_error("Order book must be an update after initialization");
            }
        }
        
        // Рассчитываем статистики начального ордер-бука (как в Python - на каждой итерации до первого ненулевого объема)
        if (start_asks_total_volume.isZero()) {
            bool first_ask = true;
            for (const auto& [price, quantity] : order_book_state.ask_quantity_by_price) {
                if (first_ask) {
                    max_start_ask_price = price;
                    min_start_ask_price = price;
                    max_start_ask_quantity = quantity;
                    min_start_ask_quantity = quantity;
                    first_ask = false;
                } else {
                    if (price > max_start_ask_price) max_start_ask_price = price;
                    if (price < min_start_ask_price) min_start_ask_price = price;
                    if (quantity > max_start_ask_quantity) max_start_ask_quantity = quantity;
                    if (quantity < min_start_ask_quantity) min_start_ask_quantity = quantity;
                }
                
                start_asks_total_quantity += quantity;
                utils::Decimal ask_volume = price * quantity;
                start_asks_total_volume += ask_volume;
                
                if (max_start_ask_volume.isZero() || ask_volume > max_start_ask_volume) {
                    max_start_ask_volume = ask_volume;
                }
                if (min_start_ask_volume.isZero() || ask_volume < min_start_ask_volume) {
                    min_start_ask_volume = ask_volume;
                }
            }
        }
        
        if (start_bids_total_volume.isZero()) {
            bool first_bid = true;
            for (const auto& [price, quantity] : order_book_state.bid_quantity_by_price) {
                if (first_bid) {
                    max_start_bid_price = price;
                    min_start_bid_price = price;
                    max_start_bid_quantity = quantity;
                    min_start_bid_quantity = quantity;
                    first_bid = false;
                } else {
                    if (price > max_start_bid_price) max_start_bid_price = price;
                    if (price < min_start_bid_price) min_start_bid_price = price;
                    if (quantity > max_start_bid_quantity) max_start_bid_quantity = quantity;
                    if (quantity < min_start_bid_quantity) min_start_bid_quantity = quantity;
                }
                
                start_bids_total_quantity += quantity;
                utils::Decimal bid_volume = price * quantity;
                start_bids_total_volume += bid_volume;
                
                if (max_start_bid_volume.isZero() || bid_volume > max_start_bid_volume) {
                    max_start_bid_volume = bid_volume;
                }
                if (min_start_bid_volume.isZero() || bid_volume < min_start_bid_volume) {
                    min_start_bid_volume = bid_volume;
                }
            }
        }
        
        // Обновляем состояние ордер-бука
        updateOrderBookState(order_book_state, next_order_book);
        
        // Рассчитываем статистики конечного ордер-бука
        OrderBookStatistics end_ask_stats = calculateOrderBookStatistics(order_book_state.ask_quantity_by_price);
        OrderBookStatistics end_bid_stats = calculateOrderBookStatistics(order_book_state.bid_quantity_by_price);
        
        // Рассчитываем статистики по сделкам в этом интервале
        TradeStatistics trade_stats = calculateTradeStatistics(trades, current_timestamp_ms, next_timestamp_ms, start_trade_idx);
        
        // КРИТИЧЕСКИ ВАЖНО: Устанавливаем start_timestamp_ms как в Python коде
        // В Python коде это происходит внутри цикла обработки сделок
        if (start_timestamp_ms == 0) {
            start_timestamp_ms = current_timestamp_ms;
        }
        
        // Создаем запись только если есть сделки
        if (trade_stats.total_trades_count > 0) {
            OKXDataSetRecordData record(symbol_id, data_set_idx, record_idx);
            
            // Заполняем поля записи
            record.buy_quantity = trade_stats.buy_quantity;
            record.buy_trades_count = trade_stats.buy_trades_count;
            record.buy_volume = trade_stats.buy_volume;
            record.close_price = trade_stats.close_price;
            
            // End order book statistics
            record.end_asks_total_quantity = end_ask_stats.total_quantity;
            record.end_asks_total_volume = end_ask_stats.total_volume;
            record.max_end_ask_price = end_ask_stats.max_price;
            record.max_end_ask_quantity = end_ask_stats.max_quantity;
            record.max_end_ask_volume = end_ask_stats.max_volume;
            record.min_end_ask_price = end_ask_stats.min_price;
            record.min_end_ask_quantity = end_ask_stats.min_quantity;
            record.min_end_ask_volume = end_ask_stats.min_volume;
            
            record.end_bids_total_quantity = end_bid_stats.total_quantity;
            record.end_bids_total_volume = end_bid_stats.total_volume;
            record.max_end_bid_price = end_bid_stats.max_price;
            record.max_end_bid_quantity = end_bid_stats.max_quantity;
            record.max_end_bid_volume = end_bid_stats.max_volume;
            record.min_end_bid_price = end_bid_stats.min_price;
            record.min_end_bid_quantity = end_bid_stats.min_quantity;
            record.min_end_bid_volume = end_bid_stats.min_volume;
            
            // Timestamps and IDs
            record.end_timestamp_ms = next_timestamp_ms;
            record.end_trade_id = trade_stats.end_trade_id;
            record.high_price = trade_stats.high_price;
            
            // Start order book statistics (используем рассчитанные выше)
            record.start_asks_total_quantity = start_asks_total_quantity;
            record.start_asks_total_volume = start_asks_total_volume;
            record.max_start_ask_price = max_start_ask_price;
            record.max_start_ask_quantity = max_start_ask_quantity;
            record.max_start_ask_volume = max_start_ask_volume;
            record.min_start_ask_price = min_start_ask_price;
            record.min_start_ask_quantity = min_start_ask_quantity;
            record.min_start_ask_volume = min_start_ask_volume;
            
            record.start_bids_total_quantity = start_bids_total_quantity;
            record.start_bids_total_volume = start_bids_total_volume;
            record.max_start_bid_price = max_start_bid_price;
            record.max_start_bid_quantity = max_start_bid_quantity;
            record.max_start_bid_volume = max_start_bid_volume;
            record.min_start_bid_price = min_start_bid_price;
            record.min_start_bid_quantity = min_start_bid_quantity;
            record.min_start_bid_volume = min_start_bid_volume;
            
            // Additional trade statistics
            record.low_price = trade_stats.low_price;
            record.open_price = trade_stats.open_price;
            record.start_timestamp_ms = start_timestamp_ms;  // Используем переменную из цикла
            record.start_trade_id = trade_stats.start_trade_id;
            record.total_quantity = trade_stats.total_quantity;
            record.total_trades_count = trade_stats.total_trades_count;
            record.total_volume = trade_stats.total_volume;
            
            records.push_back(record);
            record_idx++;
            
            // КРИТИЧЕСКИ ВАЖНО: Обнуляем переменные ПОСЛЕ создания записи (как в Python коде)
            // Это должно происходить ТОЛЬКО после успешного создания записи
            trade_stats.buy_quantity = utils::Decimal::ZERO;
            trade_stats.buy_trades_count = 0;
            trade_stats.buy_volume = utils::Decimal::ZERO;
            trade_stats.close_price = utils::Decimal::ZERO;
            trade_stats.high_price = utils::Decimal::ZERO;
            trade_stats.low_price = utils::Decimal::ZERO;
            trade_stats.open_price = utils::Decimal::ZERO;
            trade_stats.start_trade_id = 0;
            trade_stats.end_trade_id = 0;
            trade_stats.start_timestamp_ms = 0;
            trade_stats.total_quantity = utils::Decimal::ZERO;
            trade_stats.total_trades_count = 0;
            trade_stats.total_volume = utils::Decimal::ZERO;
            
            // Обнуляем start_timestamp_ms для следующей итерации (как в Python коде)
            start_timestamp_ms = 0;
        }
    }
    
    LOG_INFO("Final data set records were saved! Total records: {}", records.size());
    return records;
}

void DataSetCalculator::initializeOrderBookState(OrderBookState& state, const OrderBookSnapshot& snapshot) {
    state.ask_quantity_by_price.clear();
    state.bid_quantity_by_price.clear();
    
    // Обрабатываем asks
    for (const auto& ask_list : snapshot.asks) {
        if (ask_list.size() >= 2) {
            utils::Decimal price = utils::Decimal::fromString(ask_list[0]);
            utils::Decimal quantity = utils::Decimal::fromString(ask_list[1]);
            
            if (isValidQuantity(quantity)) {
                state.ask_quantity_by_price[price] = quantity;
            }
        }
    }
    
    // Обрабатываем bids
    for (const auto& bid_list : snapshot.bids) {
        if (bid_list.size() >= 2) {
            utils::Decimal price = utils::Decimal::fromString(bid_list[0]);
            utils::Decimal quantity = utils::Decimal::fromString(bid_list[1]);
            
            if (isValidQuantity(quantity)) {
                state.bid_quantity_by_price[price] = quantity;
            }
        }
    }
    
    state.initialized = true;
}

void DataSetCalculator::updateOrderBookState(OrderBookState& state, const OrderBookSnapshot& update) {
    // Обновляем asks
    for (const auto& ask_list : update.asks) {
        if (ask_list.size() >= 2) {
            utils::Decimal price = utils::Decimal::fromString(ask_list[0]);
            utils::Decimal quantity = utils::Decimal::fromString(ask_list[1]);
            
            if (isValidQuantity(quantity)) {
                state.ask_quantity_by_price[price] = quantity;
            } else {
                state.ask_quantity_by_price.erase(price);
            }
        }
    }
    
    // Обновляем bids
    for (const auto& bid_list : update.bids) {
        if (bid_list.size() >= 2) {
            utils::Decimal price = utils::Decimal::fromString(bid_list[0]);
            utils::Decimal quantity = utils::Decimal::fromString(bid_list[1]);
            
            if (isValidQuantity(quantity)) {
                state.bid_quantity_by_price[price] = quantity;
            } else {
                state.bid_quantity_by_price.erase(price);
            }
        }
    }
}

DataSetCalculator::OrderBookStatistics DataSetCalculator::calculateOrderBookStatistics(
    const std::map<utils::Decimal, utils::Decimal>& quantity_by_price) {
    
    OrderBookStatistics stats;
    
    if (quantity_by_price.empty()) {
        return stats;
    }
    
    bool first = true;
    for (const auto& [price, quantity] : quantity_by_price) {
        utils::Decimal volume = price * quantity;
        
        if (first) {
            stats.max_price = price;
            stats.min_price = price;
            stats.max_quantity = quantity;
            stats.min_quantity = quantity;
            stats.max_volume = volume;
            stats.min_volume = volume;
            first = false;
        } else {
            if (price > stats.max_price) stats.max_price = price;
            if (price < stats.min_price) stats.min_price = price;
            if (quantity > stats.max_quantity) stats.max_quantity = quantity;
            if (quantity < stats.min_quantity) stats.min_quantity = quantity;
            if (volume > stats.max_volume) stats.max_volume = volume;
            if (volume < stats.min_volume) stats.min_volume = volume;
        }
        
        stats.total_quantity += quantity;
        stats.total_volume += volume;
    }
    
    return stats;
}

DataSetCalculator::TradeStatistics DataSetCalculator::calculateTradeStatistics(
    const std::vector<TradeData>& trades,
    int64_t start_timestamp_ms,
    int64_t end_timestamp_ms,
    size_t& start_trade_idx) {
    
    TradeStatistics stats;
    
    // Находим сделки в заданном временном интервале
    for (size_t trade_idx = start_trade_idx; trade_idx < trades.size(); ++trade_idx) {
        const auto& trade = trades[trade_idx];
        
        if (trade.timestamp_ms < start_timestamp_ms) {
            continue;
        } else if (trade.timestamp_ms >= end_timestamp_ms) {
            start_trade_idx = trade_idx;
            break;
        }
        
        // Обновляем статистики
        if (stats.start_trade_id == 0) {
            stats.start_trade_id = trade.trade_id;
            stats.start_timestamp_ms = trade.timestamp_ms;
        }
        
        stats.end_trade_id = trade.trade_id;
        
        if (stats.open_price.isZero()) {
            stats.open_price = trade.price;
        }
        stats.close_price = trade.price;
        
        if (stats.high_price.isZero() || trade.price > stats.high_price) {
            stats.high_price = trade.price;
        }
        
        if (stats.low_price.isZero() || trade.price < stats.low_price) {
            stats.low_price = trade.price;
        }
        
        utils::Decimal trade_volume = trade.price * trade.quantity;
        
        if (trade.is_buy) {
            stats.buy_quantity += trade.quantity;
            stats.buy_trades_count++;
            stats.buy_volume += trade_volume;
        }
        
        stats.total_quantity += trade.quantity;
        stats.total_trades_count++;
        stats.total_volume += trade_volume;
    }
    
    return stats;
}

bool DataSetCalculator::isValidPrice(const utils::Decimal& price) {
    return !price.isZero() && price.isPositive();
}

bool DataSetCalculator::isValidQuantity(const utils::Decimal& quantity) {
    return !quantity.isZero() && quantity.isPositive();
}

} // namespace processors
} // namespace okx