#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <optional>
#include <map>
#include <stdexcept>
#include "utils/decimal.h"

namespace okx {

/**
 * @brief Symbol ID enumeration
 */
enum class SymbolId : int32_t {
    BTC_USDT = 1,
    ETH_USDT = 2,
    SOL_USDT = 3
};

/**
 * @brief OKX Order Book Action ID enumeration
 */
enum class OKXOrderBookActionId : int32_t {
    Snapshot = 1,
    Update = 2
};

/**
 * @brief Trading Direction enumeration
 */
enum class TradingDirection : int32_t {
    Bear = 1,
    Bull = 2,
    Cross = 3
};

/**
 * @brief Symbol constants for conversion between ID and name
 */
class SymbolConstants {
public:
    static const std::string& getNameById(SymbolId id) {
        static const std::map<SymbolId, std::string> name_by_id = {
            {SymbolId::BTC_USDT, "BTC_USDT"},
            {SymbolId::ETH_USDT, "ETH_USDT"},
            {SymbolId::SOL_USDT, "SOL_USDT"}
        };
        
        auto it = name_by_id.find(id);
        if (it != name_by_id.end()) {
            return it->second;
        }
        throw std::runtime_error("Unknown SymbolId: " + std::to_string(static_cast<int>(id)));
    }
    
    static SymbolId getIdByName(const std::string& name) {
        static const std::map<std::string, SymbolId> id_by_name = {
            {"BTC_USDT", SymbolId::BTC_USDT},
            {"ETH_USDT", SymbolId::ETH_USDT},
            {"SOL_USDT", SymbolId::SOL_USDT}
        };
        
        auto it = id_by_name.find(name);
        if (it != id_by_name.end()) {
            return it->second;
        }
        throw std::runtime_error("Unknown symbol name: " + name);
    }
};

// Структура для записи в таблицу okx_data_set_record_data
struct OKXDataSetRecordData {
    // Primary key fields
    SymbolId symbol_id;
    int32_t data_set_idx;
    int32_t record_idx;
    
    // Trade statistics
    utils::Decimal buy_quantity;
    int32_t buy_trades_count;
    utils::Decimal buy_volume;
    utils::Decimal close_price;
    
    // End order book statistics
    utils::Decimal end_asks_total_quantity;
    utils::Decimal end_asks_total_volume;
    utils::Decimal max_end_ask_price;
    utils::Decimal max_end_ask_quantity;
    utils::Decimal max_end_ask_volume;
    utils::Decimal min_end_ask_price;
    utils::Decimal min_end_ask_quantity;
    utils::Decimal min_end_ask_volume;
    
    utils::Decimal end_bids_total_quantity;
    utils::Decimal end_bids_total_volume;
    utils::Decimal max_end_bid_price;
    utils::Decimal max_end_bid_quantity;
    utils::Decimal max_end_bid_volume;
    utils::Decimal min_end_bid_price;
    utils::Decimal min_end_bid_quantity;
    utils::Decimal min_end_bid_volume;
    
    // Timestamps and IDs
    int64_t end_timestamp_ms;
    int64_t end_trade_id;
    utils::Decimal high_price;
    
    // Start order book statistics
    utils::Decimal start_asks_total_quantity;
    utils::Decimal start_asks_total_volume;
    utils::Decimal max_start_ask_price;
    utils::Decimal max_start_ask_quantity;
    utils::Decimal max_start_ask_volume;
    utils::Decimal min_start_ask_price;
    utils::Decimal min_start_ask_quantity;
    utils::Decimal min_start_ask_volume;
    
    utils::Decimal start_bids_total_quantity;
    utils::Decimal start_bids_total_volume;
    utils::Decimal max_start_bid_price;
    utils::Decimal max_start_bid_quantity;
    utils::Decimal max_start_bid_volume;
    utils::Decimal min_start_bid_price;
    utils::Decimal min_start_bid_quantity;
    utils::Decimal min_start_bid_volume;
    
    // Additional trade statistics
    utils::Decimal low_price;
    utils::Decimal open_price;
    int64_t start_timestamp_ms;
    int64_t start_trade_id;
    utils::Decimal total_quantity;
    int32_t total_trades_count;
    utils::Decimal total_volume;
    
    // Конструктор по умолчанию
    OKXDataSetRecordData() = default;
    
    // Конструктор с основными параметрами
    OKXDataSetRecordData(SymbolId symbol_id, 
                        int32_t data_set_idx,
                        int32_t record_idx)
        : symbol_id(symbol_id)
        , data_set_idx(data_set_idx)
        , record_idx(record_idx)
        , buy_quantity(utils::Decimal::ZERO)
        , buy_trades_count(0)
        , buy_volume(utils::Decimal::ZERO)
        , close_price(utils::Decimal::ZERO)
        , end_asks_total_quantity(utils::Decimal::ZERO)
        , end_asks_total_volume(utils::Decimal::ZERO)
        , max_end_ask_price(utils::Decimal::ZERO)
        , max_end_ask_quantity(utils::Decimal::ZERO)
        , max_end_ask_volume(utils::Decimal::ZERO)
        , min_end_ask_price(utils::Decimal::ZERO)
        , min_end_ask_quantity(utils::Decimal::ZERO)
        , min_end_ask_volume(utils::Decimal::ZERO)
        , end_bids_total_quantity(utils::Decimal::ZERO)
        , end_bids_total_volume(utils::Decimal::ZERO)
        , max_end_bid_price(utils::Decimal::ZERO)
        , max_end_bid_quantity(utils::Decimal::ZERO)
        , max_end_bid_volume(utils::Decimal::ZERO)
        , min_end_bid_price(utils::Decimal::ZERO)
        , min_end_bid_quantity(utils::Decimal::ZERO)
        , min_end_bid_volume(utils::Decimal::ZERO)
        , end_timestamp_ms(0)
        , end_trade_id(0)
        , high_price(utils::Decimal::ZERO)
        , start_asks_total_quantity(utils::Decimal::ZERO)
        , start_asks_total_volume(utils::Decimal::ZERO)
        , max_start_ask_price(utils::Decimal::ZERO)
        , max_start_ask_quantity(utils::Decimal::ZERO)
        , max_start_ask_volume(utils::Decimal::ZERO)
        , min_start_ask_price(utils::Decimal::ZERO)
        , min_start_ask_quantity(utils::Decimal::ZERO)
        , min_start_ask_volume(utils::Decimal::ZERO)
        , start_bids_total_quantity(utils::Decimal::ZERO)
        , start_bids_total_volume(utils::Decimal::ZERO)
        , max_start_bid_price(utils::Decimal::ZERO)
        , max_start_bid_quantity(utils::Decimal::ZERO)
        , max_start_bid_volume(utils::Decimal::ZERO)
        , min_start_bid_price(utils::Decimal::ZERO)
        , min_start_bid_quantity(utils::Decimal::ZERO)
        , min_start_bid_volume(utils::Decimal::ZERO)
        , low_price(utils::Decimal::ZERO)
        , open_price(utils::Decimal::ZERO)
        , start_timestamp_ms(0)
        , start_trade_id(0)
        , total_quantity(utils::Decimal::ZERO)
        , total_trades_count(0)
        , total_volume(utils::Decimal::ZERO) {}
};

// Структура для order book снимка (соответствует OKXOrderBookData2)
struct OrderBookSnapshot {
    SymbolId symbol_id;
    int64_t timestamp_ms;
    OKXOrderBookActionId action_id;
    std::vector<std::vector<std::string>> asks;  // [price, quantity, ...]
    std::vector<std::vector<std::string>> bids;  // [price, quantity, ...]
    
    OrderBookSnapshot() = default;
    
    OrderBookSnapshot(SymbolId symbol_id,
                     int64_t timestamp_ms,
                     OKXOrderBookActionId action_id,
                     const std::vector<std::vector<std::string>>& asks,
                     const std::vector<std::vector<std::string>>& bids)
        : symbol_id(symbol_id)
        , timestamp_ms(timestamp_ms)
        , action_id(action_id)
        , asks(asks)
        , bids(bids) {}
};

// Структура для trade данных (соответствует OKXTradeData2)
struct TradeData {
    SymbolId symbol_id;
    int64_t timestamp_ms;
    int64_t trade_id;
    utils::Decimal price;
    utils::Decimal quantity;
    bool is_buy;
    
    TradeData() = default;
    
    TradeData(SymbolId symbol_id,
              int64_t timestamp_ms,
              int64_t trade_id,
              const utils::Decimal& price,
              const utils::Decimal& quantity,
              bool is_buy)
        : symbol_id(symbol_id)
        , timestamp_ms(timestamp_ms)
        , trade_id(trade_id)
        , price(price)
        , quantity(quantity)
        , is_buy(is_buy) {}
};

// Структура для конфигурации
struct DatabaseConfig {
    std::string host;
    int port;
    std::string database_name;
    std::string username;
    std::string password;
    
    DatabaseConfig() = default;
    
    DatabaseConfig(const std::string& host,
                   int port,
                   const std::string& database_name,
                   const std::string& username,
                   const std::string& password)
        : host(host)
        , port(port)
        , database_name(database_name)
        , username(username)
        , password(password) {}
};

} // namespace okx
