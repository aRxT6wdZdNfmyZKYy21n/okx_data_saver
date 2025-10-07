#include "database/database_query.h"
#include "utils/logger.h"
#include <stdexcept>
#include <sstream>
#include <thread>
#include <chrono>
#include <iomanip>
#include <ctime>

namespace okx {
namespace database {

DatabaseQuery::DatabaseQuery(pqxx::connection& connection, 
                           bool enableCaching, 
                           size_t maxCacheSize)
    : connection_(connection)
    , caching_enabled_(enableCaching)
    , max_cache_size_(maxCacheSize) {
    
    LOG_DEBUG("DatabaseQuery initialized with caching: {}, max cache size: {}", 
              enableCaching, maxCacheSize);
}

pqxx::result DatabaseQuery::execute(const std::string& query, bool useCache) {
    if (useCache && caching_enabled_) {
        auto it = result_cache_.find(query);
        if (it != result_cache_.end()) {
            LOG_DEBUG("Using cached result for query: {}", query);
            return it->second;
        }
    }
    
    try {
        pqxx::work transaction(connection_);
        auto result = transaction.exec(query);
        transaction.commit();
        
        if (useCache && caching_enabled_) {
            // Check cache size limit
            if (result_cache_.size() >= max_cache_size_) {
                result_cache_.erase(result_cache_.begin());
            }
            result_cache_[query] = result;
            LOG_DEBUG("Cached result for query: {}", query);
        }
        
        return result;
    } catch (const std::exception& e) {
        LOG_ERROR("Query execution failed: {}", e.what());
        throw;
    }
}

std::vector<pqxx::result> DatabaseQuery::executeBatch(const std::vector<std::string>& queries) {
    std::vector<pqxx::result> results;
    results.reserve(queries.size());
    
    try {
        pqxx::work transaction(connection_);
        
        for (const auto& query : queries) {
            LOG_DEBUG("Executing batch query: {}", query);
            auto result = transaction.exec(query);
            results.push_back(result);
        }
        
        transaction.commit();
        LOG_DEBUG("Batch execution completed, {} queries executed", queries.size());
    } catch (const std::exception& e) {
        LOG_ERROR("Batch execution failed: {}", e.what());
        throw;
    }
    
    return results;
}

pqxx::result DatabaseQuery::executeWithRetry(const std::string& query, 
                                           int maxRetries, 
                                           int retryDelay) {
    int attempts = 0;
    std::exception lastException;
    
    while (attempts < maxRetries) {
        try {
            return execute(query, false); // Don't use cache for retry
        } catch (const std::exception& e) {
            lastException = e;
            attempts++;
            
            if (attempts < maxRetries) {
                LOG_WARN("Query execution failed (attempt {}/{}): {}. Retrying in {}ms...", 
                         attempts, maxRetries, e.what(), retryDelay);
                std::this_thread::sleep_for(std::chrono::milliseconds(retryDelay));
            }
        }
    }
    
    LOG_ERROR("Query execution failed after {} attempts: {}", maxRetries, lastException.what());
    throw lastException;
}

bool DatabaseQuery::isCached(const std::string& query) const {
    return result_cache_.find(query) != result_cache_.end();
}

void DatabaseQuery::clearCache() {
    result_cache_.clear();
    query_cache_.clear();
    LOG_DEBUG("Query cache cleared");
}

DatabaseQuery::CacheStats DatabaseQuery::getCacheStats() const {
    CacheStats stats;
    stats.query_count = query_cache_.size();
    stats.result_count = result_cache_.size();
    stats.max_size = max_cache_size_;
    
    if (stats.result_count > 0) {
        // Simple hit ratio calculation (this is a placeholder)
        stats.hit_ratio = 0.8; // Would need to track actual hits/misses
    } else {
        stats.hit_ratio = 0.0;
    }
    
    return stats;
}

std::optional<OKXDataSetRecordData> DatabaseQuery::getLastFinalDatasetRecord(const std::string& symbolId) {
    try {
        std::string query = "SELECT * FROM okx_data_set_record_data "
                           "WHERE symbol_id = $1 "
                           "ORDER BY timestamp DESC "
                           "LIMIT 1";
        
        auto result = execute(query, true, symbolId);
        
        if (result.empty()) {
            return std::nullopt;
        }
        
        return rowToDataSetRecord(result[0]);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to get last final dataset record: {}", e.what());
        throw;
    }
}

std::vector<OrderBookSnapshot> DatabaseQuery::getOrderBookSnapshots(
    const std::string& symbolId,
    const std::chrono::system_clock::time_point& afterTimestamp,
    int limit) {
    
    try {
        std::string query = "SELECT * FROM okx_order_book_data "
                           "WHERE symbol_id = $1 AND timestamp > $2 "
                           "ORDER BY timestamp ASC "
                           "LIMIT $3";
        
        // Convert time_point to string for pqxx
        auto time_t = std::chrono::system_clock::to_time_t(afterTimestamp);
        std::string timestamp_str = std::to_string(time_t);
        
        auto result = execute(query, true, symbolId, timestamp_str, limit);
        
        std::vector<OrderBookSnapshot> snapshots;
        snapshots.reserve(result.size());
        
        for (const auto& row : result) {
            snapshots.push_back(rowToOrderBookSnapshot(row));
        }
        
        return snapshots;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to get order book snapshots: {}", e.what());
        throw;
    }
}

std::vector<TradeData> DatabaseQuery::getTradesBetween(
    const std::string& symbolId,
    const std::chrono::system_clock::time_point& startTimestamp,
    const std::chrono::system_clock::time_point& endTimestamp) {
    
    try {
        std::string query = "SELECT * FROM okx_trade_data "
                           "WHERE symbol_id = $1 AND timestamp > $2 AND timestamp <= $3 "
                           "ORDER BY trade_id ASC";
        
        // Convert time_points to strings for pqxx
        auto start_time_t = std::chrono::system_clock::to_time_t(startTimestamp);
        auto end_time_t = std::chrono::system_clock::to_time_t(endTimestamp);
        std::string start_timestamp_str = std::to_string(start_time_t);
        std::string end_timestamp_str = std::to_string(end_time_t);
        
        auto result = execute(query, true, symbolId, start_timestamp_str, end_timestamp_str);
        
        std::vector<TradeData> trades;
        trades.reserve(result.size());
        
        for (const auto& row : result) {
            trades.push_back(rowToTradeData(row));
        }
        
        return trades;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to get trades between timestamps: {}", e.what());
        throw;
    }
}

std::vector<OrderBookSnapshot> DatabaseQuery::getOrderBookUpdates(
    const std::string& symbolId,
    const std::chrono::system_clock::time_point& startTimestamp,
    const std::chrono::system_clock::time_point& endTimestamp) {
    
    try {
        std::string query = "SELECT * FROM okx_order_book_data "
                           "WHERE symbol_id = $1 AND timestamp > $2 AND timestamp <= $3 "
                           "AND action_id = $4 "
                           "ORDER BY timestamp ASC";
        
        // Convert time_points to strings for pqxx
        auto start_time_t = std::chrono::system_clock::to_time_t(startTimestamp);
        auto end_time_t = std::chrono::system_clock::to_time_t(endTimestamp);
        std::string start_timestamp_str = std::to_string(start_time_t);
        std::string end_timestamp_str = std::to_string(end_time_t);
        
        // Assuming action_id = 2 for updates (from OKXOrderBookActionId::Update)
        auto result = execute(query, true, symbolId, start_timestamp_str, end_timestamp_str, 2);
        
        std::vector<OrderBookSnapshot> updates;
        updates.reserve(result.size());
        
        for (const auto& row : result) {
            updates.push_back(rowToOrderBookSnapshot(row));
        }
        
        return updates;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to get order book updates: {}", e.what());
        throw;
    }
}

bool DatabaseQuery::insertFinalDatasetRecord(const OKXDataSetRecordData& record) {
    try {
        std::string symbol_name = SymbolConstants::getNameById(record.symbol_id);
        
        std::string query = "INSERT INTO okx_data_set_record_data "
                           "(symbol_id, data_set_idx, record_idx, "
                           "buy_quantity, buy_trades_count, buy_volume, close_price, "
                           "end_asks_total_quantity, end_asks_total_volume, "
                           "max_end_ask_price, max_end_ask_quantity, max_end_ask_volume, "
                           "min_end_ask_price, min_end_ask_quantity, min_end_ask_volume, "
                           "end_bids_total_quantity, end_bids_total_volume, "
                           "max_end_bid_price, max_end_bid_quantity, max_end_bid_volume, "
                           "min_end_bid_price, min_end_bid_quantity, min_end_bid_volume, "
                           "end_timestamp_ms, end_trade_id, high_price, "
                           "start_asks_total_quantity, start_asks_total_volume, "
                           "max_start_ask_price, max_start_ask_quantity, max_start_ask_volume, "
                           "min_start_ask_price, min_start_ask_quantity, min_start_ask_volume, "
                           "start_bids_total_quantity, start_bids_total_volume, "
                           "max_start_bid_price, max_start_bid_quantity, max_start_bid_volume, "
                           "min_start_bid_price, min_start_bid_quantity, min_start_bid_volume, "
                           "low_price, open_price, start_timestamp_ms, start_trade_id, "
                           "total_quantity, total_trades_count, total_volume) "
                           "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49)";
        
        execute(query, false, 
                symbol_name, record.data_set_idx, record.record_idx,
                record.buy_quantity.toString(), record.buy_trades_count, record.buy_volume.toString(), record.close_price.toString(),
                record.end_asks_total_quantity.toString(), record.end_asks_total_volume.toString(),
                record.max_end_ask_price.toString(), record.max_end_ask_quantity.toString(), record.max_end_ask_volume.toString(),
                record.min_end_ask_price.toString(), record.min_end_ask_quantity.toString(), record.min_end_ask_volume.toString(),
                record.end_bids_total_quantity.toString(), record.end_bids_total_volume.toString(),
                record.max_end_bid_price.toString(), record.max_end_bid_quantity.toString(), record.max_end_bid_volume.toString(),
                record.min_end_bid_price.toString(), record.min_end_bid_quantity.toString(), record.min_end_bid_volume.toString(),
                record.end_timestamp_ms, record.end_trade_id, record.high_price.toString(),
                record.start_asks_total_quantity.toString(), record.start_asks_total_volume.toString(),
                record.max_start_ask_price.toString(), record.max_start_ask_quantity.toString(), record.max_start_ask_volume.toString(),
                record.min_start_ask_price.toString(), record.min_start_ask_quantity.toString(), record.min_start_ask_volume.toString(),
                record.start_bids_total_quantity.toString(), record.start_bids_total_volume.toString(),
                record.max_start_bid_price.toString(), record.max_start_bid_quantity.toString(), record.max_start_bid_volume.toString(),
                record.min_start_bid_price.toString(), record.min_start_bid_quantity.toString(), record.min_start_bid_volume.toString(),
                record.low_price.toString(), record.open_price.toString(), record.start_timestamp_ms, record.start_trade_id,
                record.total_quantity.toString(), record.total_trades_count, record.total_volume.toString());
        
        LOG_DEBUG("Inserted final dataset record for symbol: {}", symbol_name);
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to insert final dataset record: {}", e.what());
        return false;
    }
}

bool DatabaseQuery::isConnected() const {
    try {
        return connection_.is_open();
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to check connection status: {}", e.what());
        return false;
    }
}

std::string DatabaseQuery::getConnectionInfo() const {
    try {
        std::ostringstream oss;
        oss << "Host: " << connection_.hostname() 
            << ", Port: " << connection_.port()
            << ", Database: " << connection_.dbname()
            << ", User: " << connection_.username()
            << ", Connected: " << (connection_.is_open() ? "Yes" : "No");
        return oss.str();
    } catch (const std::exception& e) {
        return "Connection info unavailable: " + std::string(e.what());
    }
}

OKXDataSetRecordData DatabaseQuery::rowToDataSetRecord(const pqxx::row& row) const {
    OKXDataSetRecordData record;
    
    try {
        // Конвертируем строку в SymbolId
        std::string symbol_name = row["symbol_id"].as<std::string>();
        if (symbol_name == "BTC_USDT") {
            record.symbol_id = SymbolId::BTC_USDT;
        } else if (symbol_name == "ETH_USDT") {
            record.symbol_id = SymbolId::ETH_USDT;
        } else if (symbol_name == "SOL_USDT") {
            record.symbol_id = SymbolId::SOL_USDT;
        } else {
            throw std::runtime_error("Unknown symbol: " + symbol_name);
        }
        
        // Primary key fields
        record.data_set_idx = row["data_set_idx"].as<int32_t>();
        record.record_idx = row["record_idx"].as<int32_t>();
        
        // Trade statistics
        record.buy_quantity = utils::Decimal(row["buy_quantity"].as<std::string>());
        record.buy_trades_count = row["buy_trades_count"].as<int32_t>();
        record.buy_volume = utils::Decimal(row["buy_volume"].as<std::string>());
        record.close_price = utils::Decimal(row["close_price"].as<std::string>());
        
        // End order book statistics
        record.end_asks_total_quantity = utils::Decimal(row["end_asks_total_quantity"].as<std::string>());
        record.end_asks_total_volume = utils::Decimal(row["end_asks_total_volume"].as<std::string>());
        record.max_end_ask_price = utils::Decimal(row["max_end_ask_price"].as<std::string>());
        record.max_end_ask_quantity = utils::Decimal(row["max_end_ask_quantity"].as<std::string>());
        record.max_end_ask_volume = utils::Decimal(row["max_end_ask_volume"].as<std::string>());
        record.min_end_ask_price = utils::Decimal(row["min_end_ask_price"].as<std::string>());
        record.min_end_ask_quantity = utils::Decimal(row["min_end_ask_quantity"].as<std::string>());
        record.min_end_ask_volume = utils::Decimal(row["min_end_ask_volume"].as<std::string>());
        
        record.end_bids_total_quantity = utils::Decimal(row["end_bids_total_quantity"].as<std::string>());
        record.end_bids_total_volume = utils::Decimal(row["end_bids_total_volume"].as<std::string>());
        record.max_end_bid_price = utils::Decimal(row["max_end_bid_price"].as<std::string>());
        record.max_end_bid_quantity = utils::Decimal(row["max_end_bid_quantity"].as<std::string>());
        record.max_end_bid_volume = utils::Decimal(row["max_end_bid_volume"].as<std::string>());
        record.min_end_bid_price = utils::Decimal(row["min_end_bid_price"].as<std::string>());
        record.min_end_bid_quantity = utils::Decimal(row["min_end_bid_quantity"].as<std::string>());
        record.min_end_bid_volume = utils::Decimal(row["min_end_bid_volume"].as<std::string>());
        
        // Timestamps and IDs
        record.end_timestamp_ms = row["end_timestamp_ms"].as<int64_t>();
        record.end_trade_id = row["end_trade_id"].as<int64_t>();
        record.high_price = utils::Decimal(row["high_price"].as<std::string>());
        
        // Start order book statistics
        record.start_asks_total_quantity = utils::Decimal(row["start_asks_total_quantity"].as<std::string>());
        record.start_asks_total_volume = utils::Decimal(row["start_asks_total_volume"].as<std::string>());
        record.max_start_ask_price = utils::Decimal(row["max_start_ask_price"].as<std::string>());
        record.max_start_ask_quantity = utils::Decimal(row["max_start_ask_quantity"].as<std::string>());
        record.max_start_ask_volume = utils::Decimal(row["max_start_ask_volume"].as<std::string>());
        record.min_start_ask_price = utils::Decimal(row["min_start_ask_price"].as<std::string>());
        record.min_start_ask_quantity = utils::Decimal(row["min_start_ask_quantity"].as<std::string>());
        record.min_start_ask_volume = utils::Decimal(row["min_start_ask_volume"].as<std::string>());
        
        record.start_bids_total_quantity = utils::Decimal(row["start_bids_total_quantity"].as<std::string>());
        record.start_bids_total_volume = utils::Decimal(row["start_bids_total_volume"].as<std::string>());
        record.max_start_bid_price = utils::Decimal(row["max_start_bid_price"].as<std::string>());
        record.max_start_bid_quantity = utils::Decimal(row["max_start_bid_quantity"].as<std::string>());
        record.max_start_bid_volume = utils::Decimal(row["max_start_bid_volume"].as<std::string>());
        record.min_start_bid_price = utils::Decimal(row["min_start_bid_price"].as<std::string>());
        record.min_start_bid_quantity = utils::Decimal(row["min_start_bid_quantity"].as<std::string>());
        record.min_start_bid_volume = utils::Decimal(row["min_start_bid_volume"].as<std::string>());
        
        // Additional fields
        record.low_price = utils::Decimal(row["low_price"].as<std::string>());
        record.open_price = utils::Decimal(row["open_price"].as<std::string>());
        record.start_timestamp_ms = row["start_timestamp_ms"].as<int64_t>();
        record.start_trade_id = row["start_trade_id"].as<int64_t>();
        record.total_quantity = utils::Decimal(row["total_quantity"].as<std::string>());
        record.total_trades_count = row["total_trades_count"].as<int32_t>();
        record.total_volume = utils::Decimal(row["total_volume"].as<std::string>());
        
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to convert row to OKXDataSetRecordData: {}", e.what());
        throw;
    }
    
    return record;
}

OrderBookSnapshot DatabaseQuery::rowToOrderBookSnapshot(const pqxx::row& row) const {
    OrderBookSnapshot snapshot;
    
    try {
        // Конвертируем строку в SymbolId
        std::string symbol_name = row["symbol_id"].as<std::string>();
        if (symbol_name == "BTC_USDT") {
            snapshot.symbol_id = SymbolId::BTC_USDT;
        } else if (symbol_name == "ETH_USDT") {
            snapshot.symbol_id = SymbolId::ETH_USDT;
        } else if (symbol_name == "SOL_USDT") {
            snapshot.symbol_id = SymbolId::SOL_USDT;
        } else {
            throw std::runtime_error("Unknown symbol: " + symbol_name);
        }
        
        // Convert timestamp from string to milliseconds
        std::string timestamp_str = row["timestamp"].as<std::string>();
        std::tm tm = {};
        std::istringstream iss(timestamp_str);
        iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        auto time_point = std::chrono::system_clock::from_time_t(std::mktime(&tm));
        auto duration = time_point.time_since_epoch();
        snapshot.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        
        snapshot.action_id = OKXOrderBookActionId::Snapshot;
        
        // Создаем пустые векторы для asks и bids (данные будут заполнены позже)
        snapshot.asks = std::vector<std::vector<std::string>>();
        snapshot.bids = std::vector<std::vector<std::string>>();
        
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to convert row to OrderBookSnapshot: {}", e.what());
        throw;
    }
    
    return snapshot;
}

TradeData DatabaseQuery::rowToTradeData(const pqxx::row& row) const {
    TradeData trade;
    
    try {
        // Конвертируем строку в SymbolId
        std::string symbol_name = row["symbol_id"].as<std::string>();
        if (symbol_name == "BTC_USDT") {
            trade.symbol_id = SymbolId::BTC_USDT;
        } else if (symbol_name == "ETH_USDT") {
            trade.symbol_id = SymbolId::ETH_USDT;
        } else if (symbol_name == "SOL_USDT") {
            trade.symbol_id = SymbolId::SOL_USDT;
        } else {
            throw std::runtime_error("Unknown symbol: " + symbol_name);
        }
        
        // Convert timestamp from string to milliseconds
        std::string timestamp_str = row["timestamp"].as<std::string>();
        std::tm tm = {};
        std::istringstream iss(timestamp_str);
        iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        auto time_point = std::chrono::system_clock::from_time_t(std::mktime(&tm));
        auto duration = time_point.time_since_epoch();
        trade.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        
        trade.trade_id = row["trade_id"].as<int64_t>();
        trade.price = utils::Decimal(row["price"].as<std::string>());
        trade.quantity = utils::Decimal(row["size"].as<std::string>());
        
        // Конвертируем side в bool
        std::string side = row["side"].as<std::string>();
        trade.is_buy = (side == "buy");
        
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to convert row to TradeData: {}", e.what());
        throw;
    }
    
    return trade;
}

} // namespace database
} // namespace okx
