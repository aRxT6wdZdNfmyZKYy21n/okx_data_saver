#include "database/database_transaction.h"
#include "utils/logger.h"
#include <stdexcept>
#include <sstream>

namespace okx {
namespace database {

DatabaseTransaction::DatabaseTransaction(pqxx::connection& connection)
    : transaction_(std::make_unique<pqxx::work>(connection))
    , committed_(false)
    , rolled_back_(false) {
    
    LOG_DEBUG("Database transaction started");
    initializePreparedStatements();
}

DatabaseTransaction::~DatabaseTransaction() {
    if (!committed_ && !rolled_back_) {
        try {
            rollback();
        } catch (const std::exception& e) {
            LOG_ERROR("Failed to rollback transaction in destructor: {}", e.what());
        }
    }
}

pqxx::result DatabaseTransaction::execute(const std::string& query) {
    validateState();
    
    try {
        LOG_DEBUG("Executing query: {}", query);
        auto result = transaction_->exec(query);
        LOG_DEBUG("Query executed successfully, {} rows affected", result.size());
        return result;
    } catch (const std::exception& e) {
        LOG_ERROR("Query execution failed: {}", e.what());
        throw;
    }
}

void DatabaseTransaction::prepare(const std::string& name, const std::string& query) {
    validateState();
    
    try {
        transaction_->conn().prepare(name, query);
        prepared_statements_[name] = query;
        LOG_DEBUG("Prepared statement '{}' created", name);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to prepare statement '{}': {}", name, e.what());
        throw;
    }
}

void DatabaseTransaction::commit() {
    validateState();
    
    try {
        transaction_->commit();
        committed_ = true;
        LOG_INFO("Transaction committed successfully");
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to commit transaction: {}", e.what());
        throw;
    }
}

void DatabaseTransaction::rollback() {
    if (committed_ || rolled_back_) {
        return;
    }
    
    try {
        transaction_->abort();
        rolled_back_ = true;
        LOG_INFO("Transaction rolled back");
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to rollback transaction: {}", e.what());
        throw;
    }
}

bool DatabaseTransaction::isActive() const {
    return !committed_ && !rolled_back_;
}

std::vector<pqxx::result> DatabaseTransaction::executeBatch(const std::vector<std::string>& queries) {
    validateState();
    
    std::vector<pqxx::result> results;
    results.reserve(queries.size());
    
    try {
        for (const auto& query : queries) {
            LOG_DEBUG("Executing batch query: {}", query);
            auto result = transaction_->exec(query);
            results.push_back(result);
        }
        
        LOG_DEBUG("Batch execution completed, {} queries executed", queries.size());
    } catch (const std::exception& e) {
        LOG_ERROR("Batch execution failed: {}", e.what());
        throw;
    }
    
    return results;
}

std::string DatabaseTransaction::getLastError() const {
    // pqxx doesn't provide direct access to last error message
    // This is a placeholder implementation
    return "No error information available";
}

bool DatabaseTransaction::hasPreparedStatement(const std::string& name) const {
    return prepared_statements_.find(name) != prepared_statements_.end();
}

std::vector<std::string> DatabaseTransaction::getPreparedStatementNames() const {
    std::vector<std::string> names;
    names.reserve(prepared_statements_.size());
    
    for (const auto& pair : prepared_statements_) {
        names.push_back(pair.first);
    }
    
    return names;
}

void DatabaseTransaction::clearPreparedStatements() {
    prepared_statements_.clear();
    LOG_DEBUG("Prepared statements cleared");
}

void DatabaseTransaction::initializePreparedStatements() {
    // Initialize common prepared statements for the application
    
    // Statement for getting last final dataset record
    prepare("get_last_final_dataset", 
        "SELECT * FROM okx_data_set_record_data "
        "WHERE symbol_id = $1 "
        "ORDER BY timestamp DESC "
        "LIMIT 1");
    
    // Statement for getting order book snapshots
    prepare("get_order_book_snapshots",
        "SELECT * FROM okx_order_book_data "
        "WHERE symbol_id = $1 AND timestamp > $2 "
        "ORDER BY timestamp ASC "
        "LIMIT $3");
    
    // Statement for getting trades between timestamps
    prepare("get_trades_between",
        "SELECT * FROM okx_trade_data "
        "WHERE symbol_id = $1 AND timestamp > $2 AND timestamp <= $3 "
        "ORDER BY trade_id ASC");
    
    // Statement for inserting final dataset record
    prepare("insert_final_dataset",
        "INSERT INTO okx_data_set_record_data "
        "(symbol_id, symbol_name, timestamp, best_bid_price, best_ask_price, "
        "best_bid_size, best_ask_size, min_trade_price, max_trade_price, "
        "total_trade_volume, trade_count, mid_price, spread, spread_percentage, "
        "volume_weighted_average_price) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)");
    
    // Statement for getting order book updates between snapshots
    prepare("get_order_book_updates",
        "SELECT * FROM okx_order_book_data "
        "WHERE symbol_id = $1 AND timestamp > $2 AND timestamp <= $3 "
        "AND action_id = $4 "
        "ORDER BY timestamp ASC");
    
    LOG_DEBUG("Common prepared statements initialized");
}

void DatabaseTransaction::validateState() const {
    if (committed_) {
        throw std::runtime_error("Transaction already committed");
    }
    
    if (rolled_back_) {
        throw std::runtime_error("Transaction already rolled back");
    }
}

} // namespace database
} // namespace okx
