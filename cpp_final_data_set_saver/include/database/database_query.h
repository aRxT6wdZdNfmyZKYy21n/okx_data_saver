#pragma once

#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <functional>
#include <optional>
#include <sstream>
#include <chrono>
#include <ctime>
#include "data_structures.h"
#include "utils/logger.h"

namespace okx {
namespace database {

/**
 * @brief Database query executor with caching and optimization
 * 
 * This class provides a high-level interface for executing database queries
 * with built-in caching, query optimization, and result processing.
 */
class DatabaseQuery {
private:
    pqxx::connection& connection_;
    std::map<std::string, std::string> query_cache_;
    std::map<std::string, pqxx::result> result_cache_;
    bool caching_enabled_;
    size_t max_cache_size_;
    
public:
    /**
     * @brief Constructor
     * @param connection Database connection
     * @param enableCaching Enable query result caching
     * @param maxCacheSize Maximum number of cached results
     */
    explicit DatabaseQuery(pqxx::connection& connection, 
                          bool enableCaching = true, 
                          size_t maxCacheSize = 100);
    
    /**
     * @brief Destructor
     */
    ~DatabaseQuery() = default;
    
    // Disable copy constructor and assignment
    DatabaseQuery(const DatabaseQuery&) = delete;
    DatabaseQuery& operator=(const DatabaseQuery&) = delete;
    
    /**
     * @brief Execute a query and return results
     * @param query SQL query string
     * @param useCache Whether to use cached results
     * @return Query result
     */
    pqxx::result execute(const std::string& query, bool useCache = true);
    
    /**
     * @brief Execute a parameterized query
     * @param query SQL query string with placeholders
     * @param params Query parameters
     * @param useCache Whether to use cached results
     * @return Query result
     */
    template<typename... Args>
    pqxx::result execute(const std::string& query, bool useCache, Args&&... args) {
        if (useCache && caching_enabled_) {
            std::string cacheKey = generateCacheKey(query, args...);
            auto it = result_cache_.find(cacheKey);
            if (it != result_cache_.end()) {
                LOG_DEBUG("Using cached result for query: {}", query);
                return it->second;
            }
        }
        
        pqxx::work transaction(connection_);
        auto result = transaction.exec_params(query, std::forward<Args>(args)...);
        transaction.commit();
        
        if (useCache && caching_enabled_) {
            cacheResult(query, result, args...);
        }
        
        return result;
    }
    
    /**
     * @brief Execute a query and return a single value
     * @param query SQL query string
     * @param defaultValue Default value if no result
     * @return Single value result
     */
    template<typename T>
    std::optional<T> executeScalar(const std::string& query, const T& defaultValue = T{}) {
        auto result = execute(query);
        if (result.empty() || result[0].empty()) {
            return defaultValue;
        }
        
        return result[0][0].as<T>();
    }
    
    /**
     * @brief Execute a query and return a single row
     * @param query SQL query string
     * @return Single row result
     */
    std::optional<pqxx::row> executeSingleRow(const std::string& query) {
        auto result = execute(query);
        if (result.empty()) {
            return std::nullopt;
        }
        
        return result[0];
    }
    
    /**
     * @brief Execute a query and return all rows
     * @param query SQL query string
     * @return All rows result
     */
    std::vector<pqxx::row> executeAllRows(const std::string& query) {
        auto result = execute(query);
        std::vector<pqxx::row> rows;
        rows.reserve(result.size());
        
        for (const auto& row : result) {
            rows.push_back(row);
        }
        
        return rows;
    }
    
    /**
     * @brief Execute multiple queries in a transaction
     * @param queries Vector of SQL queries
     * @return Vector of query results
     */
    std::vector<pqxx::result> executeBatch(const std::vector<std::string>& queries);
    
    /**
     * @brief Execute a query with retry logic
     * @param query SQL query string
     * @param maxRetries Maximum number of retries
     * @param retryDelay Delay between retries in milliseconds
     * @return Query result
     */
    pqxx::result executeWithRetry(const std::string& query, 
                                 int maxRetries = 3, 
                                 int retryDelay = 1000);
    
    /**
     * @brief Check if a query exists in cache
     * @param query SQL query string
     * @return True if cached
     */
    bool isCached(const std::string& query) const;
    
    /**
     * @brief Clear query cache
     */
    void clearCache();
    
    /**
     * @brief Enable or disable caching
     * @param enable Whether to enable caching
     */
    void setCachingEnabled(bool enable) { caching_enabled_ = enable; }
    
    /**
     * @brief Check if caching is enabled
     * @return True if enabled
     */
    bool isCachingEnabled() const { return caching_enabled_; }
    
    /**
     * @brief Get cache statistics
     * @return Cache statistics
     */
    struct CacheStats {
        size_t query_count;
        size_t result_count;
        size_t max_size;
        double hit_ratio;
    };
    
    CacheStats getCacheStats() const;
    
    // Specialized query methods for the application
    
    /**
     * @brief Get the last final dataset record for a symbol
     * @param symbolId Symbol ID
     * @return Last record or nullopt if not found
     */
    std::optional<OKXDataSetRecordData> getLastFinalDatasetRecord(const std::string& symbolId);
    
    /**
     * @brief Get order book snapshots after a timestamp
     * @param symbolId Symbol ID
     * @param afterTimestamp Timestamp to search after
     * @param limit Maximum number of records
     * @return Vector of order book snapshots
     */
    std::vector<OrderBookSnapshot> getOrderBookSnapshots(const std::string& symbolId,
                                                        const std::chrono::system_clock::time_point& afterTimestamp,
                                                        int limit = 2);
    
    /**
     * @brief Get trades between two timestamps
     * @param symbolId Symbol ID
     * @param startTimestamp Start timestamp
     * @param endTimestamp End timestamp
     * @return Vector of trade data
     */
    std::vector<TradeData> getTradesBetween(const std::string& symbolId,
                                           const std::chrono::system_clock::time_point& startTimestamp,
                                           const std::chrono::system_clock::time_point& endTimestamp);
    
    /**
     * @brief Get order book updates between two timestamps
     * @param symbolId Symbol ID
     * @param startTimestamp Start timestamp
     * @param endTimestamp End timestamp
     * @return Vector of order book updates
     */
    std::vector<OrderBookSnapshot> getOrderBookUpdates(const std::string& symbolId,
                                                      const std::chrono::system_clock::time_point& startTimestamp,
                                                      const std::chrono::system_clock::time_point& endTimestamp);
    
    /**
     * @brief Insert a final dataset record
     * @param record Record to insert
     * @return True if successful
     */
    bool insertFinalDatasetRecord(const OKXDataSetRecordData& record);
    
    /**
     * @brief Get database connection status
     * @return True if connected
     */
    bool isConnected() const;
    
    /**
     * @brief Get connection information
     * @return Connection info string
     */
    std::string getConnectionInfo() const;
    
private:
    /**
     * @brief Generate cache key for a query with parameters
     * @param query SQL query string
     * @param args Query parameters
     * @return Cache key
     */
    template<typename... Args>
    std::string generateCacheKey(const std::string& query, Args&&... args) const {
        std::ostringstream oss;
        oss << query;
        generateCacheKeyImpl(oss, std::forward<Args>(args)...);
        return oss.str();
    }
    
private:
    /**
     * @brief Helper function to generate cache key parameters
     */
    template<typename T>
    void generateCacheKeyImpl(std::ostringstream& oss, const T& arg) const {
        oss << "|" << convertToString(arg);
    }
    
    template<typename T, typename... Args>
    void generateCacheKeyImpl(std::ostringstream& oss, const T& arg, Args&&... args) const {
        oss << "|" << convertToString(arg);
        generateCacheKeyImpl(oss, std::forward<Args>(args)...);
    }
    
    /**
     * @brief Convert various types to string for cache key
     */
    template<typename T>
    std::string convertToString(const T& value) const {
        if constexpr (std::is_same_v<T, std::chrono::system_clock::time_point>) {
            auto time_t = std::chrono::system_clock::to_time_t(value);
            return std::to_string(time_t);
        } else {
            std::ostringstream oss;
            oss << value;
            return oss.str();
        }
    }
    
    /**
     * @brief Cache a query result
     * @param query SQL query string
     * @param result Query result
     * @param args Query parameters
     */
    template<typename... Args>
    void cacheResult(const std::string& query, const pqxx::result& result, Args&&... args) {
        if (!caching_enabled_) {
            return;
        }
        
        std::string cacheKey = generateCacheKey(query, args...);
        
        // Check cache size limit
        if (result_cache_.size() >= max_cache_size_) {
            // Remove oldest entry (simple FIFO)
            result_cache_.erase(result_cache_.begin());
        }
        
        result_cache_[cacheKey] = result;
        LOG_DEBUG("Cached result for query: {}", query);
    }
    
    /**
     * @brief Convert pqxx::row to OKXDataSetRecordData
     * @param row Database row
     * @return OKXDataSetRecordData object
     */
    OKXDataSetRecordData rowToDataSetRecord(const pqxx::row& row) const;
    
    /**
     * @brief Convert pqxx::row to OrderBookSnapshot
     * @param row Database row
     * @return OrderBookSnapshot object
     */
    OrderBookSnapshot rowToOrderBookSnapshot(const pqxx::row& row) const;
    
    /**
     * @brief Convert pqxx::row to TradeData
     * @param row Database row
     * @return TradeData object
     */
    TradeData rowToTradeData(const pqxx::row& row) const;
};

} // namespace database
} // namespace okx
