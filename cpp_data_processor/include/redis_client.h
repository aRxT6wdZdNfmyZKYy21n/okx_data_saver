#pragma once

#include <memory>
#include <string>
#include <pybind11/pybind11.h>
#include "data_structures.h"

namespace okx_data_processor {

/**
 * @brief Redis client wrapper for C++ data processor
 * 
 * This class provides a C++ interface to Python Redis service.
 * It delegates all Redis operations to the existing Python RedisDataService.
 */
class RedisClient {
public:
    RedisClient();
    ~RedisClient() = default;

    // Disable copy constructor and assignment operator
    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    /**
     * @brief Initialize Redis connection by importing Python service
     * 
     * @return true if initialization successful, false otherwise
     */
    bool initialize();

    /**
     * @brief Check if Redis client is connected
     * 
     * @return true if connected, false otherwise
     */
    bool is_connected() const { return connected_; }

    /**
     * @brief Save DataFrame to Redis using Python service
     * 
     * @param symbol_id Symbol identifier
     * @param data_type Type of data (e.g., "trades", "bollinger", "rsi")
     * @param dataframe Polars DataFrame to save
     * @param additional_params Additional parameters for specific data types
     * @return true if save successful, false otherwise
     */
    bool save_dataframe(SymbolId symbol_id, 
                       const std::string& data_type, 
                       const pybind11::object& dataframe,
                       const pybind11::dict& additional_params = pybind11::dict());

    /**
     * @brief Load DataFrame from Redis using Python service
     * 
     * @param symbol_id Symbol identifier
     * @param data_type Type of data to load
     * @param additional_params Additional parameters for specific data types
     * @return DataFrame if found, None otherwise
     */
    pybind11::object load_dataframe(SymbolId symbol_id, 
                                   const std::string& data_type,
                                   const pybind11::dict& additional_params = pybind11::dict());

    /**
     * @brief Check if data exists in Redis
     * 
     * @param symbol_id Symbol identifier
     * @param data_type Type of data
     * @param additional_params Additional parameters for specific data types
     * @return true if exists, false otherwise
     */
    bool data_exists(SymbolId symbol_id, 
                    const std::string& data_type,
                    const pybind11::dict& additional_params = pybind11::dict());

    /**
     * @brief Delete data from Redis
     * 
     * @param symbol_id Symbol identifier
     * @param data_type Type of data
     * @param additional_params Additional parameters for specific data types
     * @return true if deletion successful, false otherwise
     */
    bool delete_data(SymbolId symbol_id, 
                    const std::string& data_type,
                    const pybind11::dict& additional_params = pybind11::dict());

private:
    bool connected_;
    pybind11::object redis_service_;  // Python RedisDataService instance
};

} // namespace okx_data_processor