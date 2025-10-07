#include "database/database_connection.h"
#include "utils/logger.h"
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include <nlohmann/json.hpp>

namespace okx {
namespace database {

DatabaseConnection::DatabaseConnection(const DatabaseConfig& config)
    : config_(config) {
    createConnection();
}

DatabaseConnection::~DatabaseConnection() {
    disconnect();
}

void DatabaseConnection::createConnection() {
    try {
        // Формируем строку подключения
        std::ostringstream conn_string;
        conn_string << "host=" << config_.host
                   << " port=" << config_.port
                   << " dbname=" << config_.database_name
                   << " user=" << config_.username
                   << " password=" << config_.password;
        
        connection_ = std::make_unique<pqxx::connection>(conn_string.str());
        
        if (!connection_->is_open()) {
            throw std::runtime_error("Failed to connect to database");
        }
        
    } catch (const std::exception& e) {
        throw std::runtime_error("Database connection error: " + std::string(e.what()));
    }
}

void DatabaseConnection::connect() {
    if (!isConnected()) {
        createConnection();
    }
}

void DatabaseConnection::disconnect() {
    if (connection_ && connection_->is_open()) {
        connection_->disconnect();
    }
}

bool DatabaseConnection::isConnected() const {
    return connection_ && connection_->is_open();
}

pqxx::connection& DatabaseConnection::getConnection() {
    if (!isConnected()) {
        throw std::runtime_error("Database connection is not established");
    }
    return *connection_;
}

std::optional<OKXDataSetRecordData> DatabaseConnection::getLastFinalDataSetRecord(SymbolId symbol_id) {
    validateConnection();
    
    std::string symbol_name = SymbolConstants::getNameById(symbol_id);
    
    std::string query = R"(
        SELECT symbol_id, data_set_idx, record_idx,
               buy_quantity, buy_trades_count, buy_volume, close_price,
               end_asks_total_quantity, end_asks_total_volume,
               max_end_ask_price, max_end_ask_quantity, max_end_ask_volume,
               min_end_ask_price, min_end_ask_quantity, min_end_ask_volume,
               end_bids_total_quantity, end_bids_total_volume,
               max_end_bid_price, max_end_bid_quantity, max_end_bid_volume,
               min_end_bid_price, min_end_bid_quantity, min_end_bid_volume,
               end_timestamp_ms, end_trade_id, high_price,
               start_asks_total_quantity, start_asks_total_volume,
               max_start_ask_price, max_start_ask_quantity, max_start_ask_volume,
               min_start_ask_price, min_start_ask_quantity, min_start_ask_volume,
               start_bids_total_quantity, start_bids_total_volume,
               max_start_bid_price, max_start_bid_quantity, max_start_bid_volume,
               min_start_bid_price, min_start_bid_quantity, min_start_bid_volume,
               low_price, open_price, start_timestamp_ms, start_trade_id,
               total_quantity, total_trades_count, total_volume
        FROM okx_data_set_record_data 
        WHERE symbol_id = $1 
        ORDER BY data_set_idx DESC, record_idx DESC 
        LIMIT 1
    )";
    
    try {
        pqxx::work txn(*connection_);
        pqxx::result result = txn.exec_params(query, symbol_name);
        txn.commit();
        
        if (result.empty()) {
            return std::nullopt;
        }
        
        const auto& row = result[0];
        OKXDataSetRecordData record;
        
        // Primary key fields
        record.symbol_id = symbol_id;
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
        
        return record;
        
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to get last final dataset record: " + std::string(e.what()));
    }
}

std::vector<OrderBookSnapshot> DatabaseConnection::getOrderBookSnapshots(
    const std::string& symbol_id,
    const std::chrono::system_clock::time_point& start_time,
    const std::chrono::system_clock::time_point& end_time,
    int limit) {
    
    validateConnection();
    
    std::string query = R"(
        SELECT symbol_id, timestamp_ms, action_id, asks, bids
        FROM okx_order_book_data_2 
        WHERE symbol_id = $1 
          AND timestamp_ms >= $2 
          AND timestamp_ms <= $3
          AND action_id = 'Snapshot'
        ORDER BY timestamp_ms ASC 
        LIMIT $4
    )";
    
    try {
        pqxx::work txn(*connection_);
        pqxx::result result = txn.exec_params(query, 
            symbol_id, 
            timestampToMs(start_time), 
            timestampToMs(end_time), 
            limit);
        txn.commit();
        
        std::vector<OrderBookSnapshot> snapshots;
        snapshots.reserve(result.size());
        
        for (const auto& row : result) {
            // Конвертируем строку в SymbolId
            std::string symbol_name = row["symbol_id"].as<std::string>();
            SymbolId symbol_id;
            if (symbol_name == "BTC_USDT") {
                symbol_id = SymbolId::BTC_USDT;
            } else if (symbol_name == "ETH_USDT") {
                symbol_id = SymbolId::ETH_USDT;
            } else if (symbol_name == "SOL_USDT") {
                symbol_id = SymbolId::SOL_USDT;
            } else {
                continue; // Пропускаем неизвестные символы
            }
            
            // Получаем timestamp_ms
            int64_t timestamp_ms = row["timestamp_ms"].as<int64_t>();
            
            // Получаем action_id
            std::string action_str = row["action_id"].as<std::string>();
            OKXOrderBookActionId action_id;
            if (action_str == "Snapshot") {
                action_id = OKXOrderBookActionId::Snapshot;
            } else if (action_str == "Update") {
                action_id = OKXOrderBookActionId::Update;
            } else {
                throw std::runtime_error("Unknown action_id: " + action_str);
            }
            
            // Получаем asks и bids из JSON
            std::vector<std::vector<std::string>> asks;
            std::vector<std::vector<std::string>> bids;
            
            try {
                // Парсим asks из JSON
                std::string asks_json = row["asks"].as<std::string>();
                asks = parseJsonArray(asks_json);
                
                // Парсим bids из JSON
                std::string bids_json = row["bids"].as<std::string>();
                bids = parseJsonArray(bids_json);
            } catch (const std::exception& e) {
                // В случае ошибки парсинга JSON, оставляем пустые векторы
                LOG_WARN("Failed to parse JSON for asks/bids: " + std::string(e.what()));
            }
            
            snapshots.emplace_back(symbol_id, timestamp_ms, action_id, asks, bids);
        }
        
        return snapshots;
        
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to get order book snapshots: " + std::string(e.what()));
    }
}

std::vector<OrderBookSnapshot> DatabaseConnection::getOrderBookUpdates(
    const std::string& symbol_id,
    int64_t start_timestamp_ms,
    int64_t end_timestamp_ms) {
    
    validateConnection();
    
    std::string query = R"(
        SELECT symbol_id, timestamp_ms, action_id, asks, bids
        FROM okx_order_book_data_2 
        WHERE symbol_id = $1 
          AND timestamp_ms >= $2 
          AND timestamp_ms < $3
          AND action_id = 'Update'
        ORDER BY timestamp_ms ASC
    )";
    
    try {
        pqxx::work txn(*connection_);
        pqxx::result result = txn.exec_params(query, 
            symbol_id, 
            start_timestamp_ms, 
            end_timestamp_ms);
        txn.commit();
        
        std::vector<OrderBookSnapshot> updates;
        updates.reserve(result.size());
        
        for (const auto& row : result) {
            // Конвертируем строку в SymbolId
            std::string symbol_name = row["symbol_id"].as<std::string>();
            SymbolId symbol_id_enum;
            if (symbol_name == "BTC_USDT") {
                symbol_id_enum = SymbolId::BTC_USDT;
            } else if (symbol_name == "ETH_USDT") {
                symbol_id_enum = SymbolId::ETH_USDT;
            } else if (symbol_name == "SOL_USDT") {
                symbol_id_enum = SymbolId::SOL_USDT;
            } else {
                continue; // Пропускаем неизвестные символы
            }
            
            // Получаем timestamp_ms
            int64_t timestamp_ms = row["timestamp_ms"].as<int64_t>();
            
            // Получаем action_id (должен быть Update)
            std::string action_str = row["action_id"].as<std::string>();
            OKXOrderBookActionId action_id;
            if (action_str == "Update") {
                action_id = OKXOrderBookActionId::Update;
            } else {
                continue; // Пропускаем не-обновления
            }
            
            // Получаем asks и bids из JSON
            std::vector<std::vector<std::string>> asks;
            std::vector<std::vector<std::string>> bids;
            
            try {
                // Парсим asks из JSON
                std::string asks_json = row["asks"].as<std::string>();
                asks = parseJsonArray(asks_json);
                
                // Парсим bids из JSON
                std::string bids_json = row["bids"].as<std::string>();
                bids = parseJsonArray(bids_json);
            } catch (const std::exception& e) {
                // В случае ошибки парсинга JSON, оставляем пустые векторы
                LOG_WARN("Failed to parse JSON for asks/bids: " + std::string(e.what()));
            }
            
            updates.emplace_back(symbol_id_enum, timestamp_ms, action_id, asks, bids);
        }
        
        return updates;
        
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to get order book updates: " + std::string(e.what()));
    }
}

std::vector<TradeData> DatabaseConnection::getTrades(
    const std::string& symbol_id,
    const std::chrono::system_clock::time_point& start_time,
    const std::chrono::system_clock::time_point& end_time) {
    
    validateConnection();
    
    std::string query = R"(
        SELECT symbol_id, timestamp_ms, trade_id, price, quantity, is_buy
        FROM okx_trade_data_2 
        WHERE symbol_id = $1 
          AND timestamp_ms >= $2 
          AND timestamp_ms <= $3
        ORDER BY trade_id ASC
    )";
    
    try {
        pqxx::work txn(*connection_);
        pqxx::result result = txn.exec_params(query, 
            symbol_id, 
            timestampToMs(start_time), 
            timestampToMs(end_time));
        txn.commit();
        
        std::vector<TradeData> trades;
        trades.reserve(result.size());
        
        for (const auto& row : result) {
            // Конвертируем строку в SymbolId
            std::string symbol_name = row["symbol_id"].as<std::string>();
            SymbolId symbol_id;
            if (symbol_name == "BTC_USDT") {
                symbol_id = SymbolId::BTC_USDT;
            } else if (symbol_name == "ETH_USDT") {
                symbol_id = SymbolId::ETH_USDT;
            } else if (symbol_name == "SOL_USDT") {
                symbol_id = SymbolId::SOL_USDT;
            } else {
                continue; // Пропускаем неизвестные символы
            }
            
            // Получаем timestamp_ms
            int64_t timestamp_ms = row["timestamp_ms"].as<int64_t>();
            
            // Получаем trade_id
            int64_t trade_id = row["trade_id"].as<int64_t>();
            
            // Получаем is_buy
            bool is_buy = row["is_buy"].as<bool>();
            
            trades.emplace_back(
                symbol_id,
                timestamp_ms,
                trade_id,
                utils::Decimal(row["price"].as<std::string>()),
                utils::Decimal(row["quantity"].as<std::string>()),
                is_buy
            );
        }
        
        return trades;
        
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to get trades: " + std::string(e.what()));
    }
}

void DatabaseConnection::saveFinalDataSetRecord(const OKXDataSetRecordData& record) {
    validateConnection();
    
    std::string symbol_name = SymbolConstants::getNameById(record.symbol_id);
    
    std::string query = R"(
        INSERT INTO okx_data_set_record_data 
        (symbol_id, data_set_idx, record_idx,
         buy_quantity, buy_trades_count, buy_volume, close_price,
         end_asks_total_quantity, end_asks_total_volume,
         max_end_ask_price, max_end_ask_quantity, max_end_ask_volume,
         min_end_ask_price, min_end_ask_quantity, min_end_ask_volume,
         end_bids_total_quantity, end_bids_total_volume,
         max_end_bid_price, max_end_bid_quantity, max_end_bid_volume,
         min_end_bid_price, min_end_bid_quantity, min_end_bid_volume,
         end_timestamp_ms, end_trade_id, high_price,
         start_asks_total_quantity, start_asks_total_volume,
         max_start_ask_price, max_start_ask_quantity, max_start_ask_volume,
         min_start_ask_price, min_start_ask_quantity, min_start_ask_volume,
         start_bids_total_quantity, start_bids_total_volume,
         max_start_bid_price, max_start_bid_quantity, max_start_bid_volume,
         min_start_bid_price, min_start_bid_quantity, min_start_bid_volume,
         low_price, open_price, start_timestamp_ms, start_trade_id,
         total_quantity, total_trades_count, total_volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49)
    )";
    
    try {
        pqxx::work txn(*connection_);
        txn.exec_params(query,
            symbol_name,
            record.data_set_idx,
            record.record_idx,
            record.buy_quantity.toString(),
            record.buy_trades_count,
            record.buy_volume.toString(),
            record.close_price.toString(),
            record.end_asks_total_quantity.toString(),
            record.end_asks_total_volume.toString(),
            record.max_end_ask_price.toString(),
            record.max_end_ask_quantity.toString(),
            record.max_end_ask_volume.toString(),
            record.min_end_ask_price.toString(),
            record.min_end_ask_quantity.toString(),
            record.min_end_ask_volume.toString(),
            record.end_bids_total_quantity.toString(),
            record.end_bids_total_volume.toString(),
            record.max_end_bid_price.toString(),
            record.max_end_bid_quantity.toString(),
            record.max_end_bid_volume.toString(),
            record.min_end_bid_price.toString(),
            record.min_end_bid_quantity.toString(),
            record.min_end_bid_volume.toString(),
            record.end_timestamp_ms,
            record.end_trade_id,
            record.high_price.toString(),
            record.start_asks_total_quantity.toString(),
            record.start_asks_total_volume.toString(),
            record.max_start_ask_price.toString(),
            record.max_start_ask_quantity.toString(),
            record.max_start_ask_volume.toString(),
            record.min_start_ask_price.toString(),
            record.min_start_ask_quantity.toString(),
            record.min_start_ask_volume.toString(),
            record.start_bids_total_quantity.toString(),
            record.start_bids_total_volume.toString(),
            record.max_start_bid_price.toString(),
            record.max_start_bid_quantity.toString(),
            record.max_start_bid_volume.toString(),
            record.min_start_bid_price.toString(),
            record.min_start_bid_quantity.toString(),
            record.min_start_bid_volume.toString(),
            record.low_price.toString(),
            record.open_price.toString(),
            record.start_timestamp_ms,
            record.start_trade_id,
            record.total_quantity.toString(),
            record.total_trades_count,
            record.total_volume.toString());
        txn.commit();
        
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to save final dataset record: " + std::string(e.what()));
    }
}

void DatabaseConnection::validateConnection() {
    if (!isConnected()) {
        throw std::runtime_error("Database connection is not established");
    }
}

int64_t DatabaseConnection::timestampToMs(const std::chrono::system_clock::time_point& timestamp) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        timestamp.time_since_epoch()).count();
}

int64_t DatabaseConnection::stringToTimestampMs(const std::string& timestamp_str) {
    std::tm tm = {};
    std::istringstream iss(timestamp_str);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    
    if (iss.fail()) {
        throw std::runtime_error("Failed to parse timestamp: " + timestamp_str);
    }
    
    auto time_t = std::mktime(&tm);
    auto time_point = std::chrono::system_clock::from_time_t(time_t);
    auto duration = time_point.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

std::chrono::system_clock::time_point DatabaseConnection::stringToTimestamp(const std::string& timestamp_str) {
    std::tm tm = {};
    std::istringstream iss(timestamp_str);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    
    if (iss.fail()) {
        throw std::runtime_error("Failed to parse timestamp: " + timestamp_str);
    }
    
    auto time_t = std::mktime(&tm);
    return std::chrono::system_clock::from_time_t(time_t);
}

// Простая функция для парсинга JSON массивов
std::vector<std::vector<std::string>> DatabaseConnection::parseJsonArray(const std::string& json_str) {
    std::vector<std::vector<std::string>> result;
    
    if (json_str.empty() || json_str == "null") {
        return result;
    }
    
    try {
        nlohmann::json json_data = nlohmann::json::parse(json_str);
        if (json_data.is_array()) {
            for (const auto& item : json_data) {
                if (item.is_array() && item.size() >= 2) {
                    std::vector<std::string> entry;
                    entry.push_back(item[0].get<std::string>()); // price
                    entry.push_back(item[1].get<std::string>()); // quantity
                    entry.push_back("0"); // placeholder for 3rd field
                    entry.push_back("0"); // placeholder for 4th field
                    result.push_back(entry);
                }
            }
        }
    } catch (const std::exception& e) {
        LOG_WARN("Failed to parse JSON array: " + std::string(e.what()));
    }
    
    return result;
}

void DatabaseConnection::saveFinalDataSetRecords(const std::vector<OKXDataSetRecordData>& records) {
    if (records.empty()) {
        return;
    }
    
    validateConnection();
    
    std::string query = R"(
        INSERT INTO okx_data_set_record_data 
        (symbol_id, data_set_idx, record_idx,
         buy_quantity, buy_trades_count, buy_volume, close_price,
         end_asks_total_quantity, end_asks_total_volume,
         max_end_ask_price, max_end_ask_quantity, max_end_ask_volume,
         min_end_ask_price, min_end_ask_quantity, min_end_ask_volume,
         end_bids_total_quantity, end_bids_total_volume,
         max_end_bid_price, max_end_bid_quantity, max_end_bid_volume,
         min_end_bid_price, min_end_bid_quantity, min_end_bid_volume,
         end_timestamp_ms, end_trade_id, high_price,
         start_asks_total_quantity, start_asks_total_volume,
         max_start_ask_price, max_start_ask_quantity, max_start_ask_volume,
         min_start_ask_price, min_start_ask_quantity, min_start_ask_volume,
         start_bids_total_quantity, start_bids_total_volume,
         max_start_bid_price, max_start_bid_quantity, max_start_bid_volume,
         min_start_bid_price, min_start_bid_quantity, min_start_bid_volume,
         low_price, open_price, start_timestamp_ms, start_trade_id,
         total_quantity, total_trades_count, total_volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49)
    )";
    
    try {
        pqxx::work txn(*connection_);
        
        for (const auto& record : records) {
            std::string symbol_name = SymbolConstants::getNameById(record.symbol_id);
            
            txn.exec_params(query,
                symbol_name,
                record.data_set_idx,
                record.record_idx,
                record.buy_quantity.toString(),
                record.buy_trades_count,
                record.buy_volume.toString(),
                record.close_price.toString(),
                record.end_asks_total_quantity.toString(),
                record.end_asks_total_volume.toString(),
                record.max_end_ask_price.toString(),
                record.max_end_ask_quantity.toString(),
                record.max_end_ask_volume.toString(),
                record.min_end_ask_price.toString(),
                record.min_end_ask_quantity.toString(),
                record.min_end_ask_volume.toString(),
                record.end_bids_total_quantity.toString(),
                record.end_bids_total_volume.toString(),
                record.max_end_bid_price.toString(),
                record.max_end_bid_quantity.toString(),
                record.max_end_bid_volume.toString(),
                record.min_end_bid_price.toString(),
                record.min_end_bid_quantity.toString(),
                record.min_end_bid_volume.toString(),
                record.end_timestamp_ms,
                record.end_trade_id,
                record.high_price.toString(),
                record.start_asks_total_quantity.toString(),
                record.start_asks_total_volume.toString(),
                record.max_start_ask_price.toString(),
                record.max_start_ask_quantity.toString(),
                record.max_start_ask_volume.toString(),
                record.min_start_ask_price.toString(),
                record.min_start_ask_quantity.toString(),
                record.min_start_ask_volume.toString(),
                record.start_bids_total_quantity.toString(),
                record.start_bids_total_volume.toString(),
                record.max_start_bid_price.toString(),
                record.max_start_bid_quantity.toString(),
                record.max_start_bid_volume.toString(),
                record.min_start_bid_price.toString(),
                record.min_start_bid_quantity.toString(),
                record.min_start_bid_volume.toString(),
                record.low_price.toString(),
                record.open_price.toString(),
                record.start_timestamp_ms,
                record.start_trade_id,
                record.total_quantity.toString(),
                record.total_trades_count,
                record.total_volume.toString()
            );
        }
        
        txn.commit();
        LOG_INFO("Successfully saved {} final dataset records in batch", records.size());
        
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to save final dataset records batch: {}", e.what());
        throw std::runtime_error("Database batch save error: " + std::string(e.what()));
    }
}

} // namespace database
} // namespace okx
