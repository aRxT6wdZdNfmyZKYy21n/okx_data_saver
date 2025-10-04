#pragma once

#include "data_structures.h"
#include "redis_client.h"
#include <memory>
#include <string>

namespace okx_data_processor {

// Forward declarations
class BollingerBandsProcessor;
class CandlesProcessor;
class RSICalculator;
class SmoothingProcessor;
class ExtremeLinesProcessor;
class OrderBookProcessor;
class VelocityCalculator;

/**
 * @brief Main data processor class
 * 
 * This class coordinates all data processing operations and provides
 * the main interface for processing trades data.
 */
class DataProcessor {
public:
    DataProcessor();
    ~DataProcessor() = default;

    // Disable copy constructor and assignment operator
    DataProcessor(const DataProcessor&) = delete;
    DataProcessor& operator=(const DataProcessor&) = delete;

    /**
     * @brief Process trades data and create all derived data
     * 
     * @param symbol_id Symbol identifier
     * @param polars_dataframe Polars DataFrame containing trades data
     * @return ProcessingResult Result of the processing operation
     */
    ProcessingResult process_trades_data(SymbolId symbol_id, const pybind11::object& polars_dataframe);

    /**
     * @brief Process trades data asynchronously
     * 
     * @param symbol_id Symbol identifier
     * @param polars_dataframe Polars DataFrame containing trades data
     * @param callback Python callback function for async processing
     */
    void process_trades_data_async(SymbolId symbol_id, const pybind11::object& polars_dataframe, 
                                  const pybind11::function& callback);

    /**
     * @brief Get processing statistics
     * 
     * @return pybind11::dict Dictionary containing processing statistics
     */
    pybind11::dict get_processing_stats() const;

    /**
     * @brief Reset processing statistics
     */
    void reset_stats();

    /**
     * @brief Set processing parameters
     * 
     * @param params Dictionary containing processing parameters
     */
    void set_processing_params(const pybind11::dict& params);


    /**
     * @brief Save processing results to Redis
     * 
     * @param symbol_id Symbol identifier
     * @param data_type Type of data to save
     * @param data Data to save (Python object)
     */
    void save_results_to_redis(SymbolId symbol_id, const std::string& data_type, const pybind11::object& data, const pybind11::dict& additional_params = pybind11::dict());

    /**
     * @brief Load data from Redis
     * 
     * @param symbol_id Symbol identifier
     * @param data_type Type of data to load
     * @return Loaded data as Python object, or None if not found
     */
    pybind11::object load_data_from_redis(SymbolId symbol_id, const std::string& data_type);

    /**
     * @brief Check if Redis client is connected
     * 
     * @return true if connected, false otherwise
     */
    bool is_redis_connected() const;

private:
    // Processor components
    std::unique_ptr<BollingerBandsProcessor> bollinger_processor_;
    std::unique_ptr<CandlesProcessor> candles_processor_;
    std::unique_ptr<RSICalculator> rsi_calculator_;
    std::unique_ptr<SmoothingProcessor> smoothing_processor_;
    std::unique_ptr<ExtremeLinesProcessor> extreme_lines_processor_;
    std::unique_ptr<OrderBookProcessor> order_book_processor_;
    std::unique_ptr<VelocityCalculator> velocity_calculator_;
    
    // Redis client
    std::unique_ptr<RedisClient> redis_client_;

    // Processing statistics
    mutable std::atomic<uint64_t> total_trades_processed_{0};
    mutable std::atomic<uint64_t> total_processing_time_ms_{0};
    mutable std::atomic<uint64_t> successful_operations_{0};
    mutable std::atomic<uint64_t> failed_operations_{0};

    // Processing parameters
    struct ProcessingParams {
        bool enable_bollinger_bands = true;
        bool enable_candles = true;
        bool enable_rsi = true;
        bool enable_smoothing = true;
        bool enable_extreme_lines = true;
        bool enable_order_book_volumes = true;
        bool enable_velocity = true;
        
        int32_t bollinger_period = 20;
        int32_t rsi_period = 14;
        std::vector<std::string> candle_intervals = {"1m", "5m", "15m", "1h", "4h", "1d"};
        std::vector<std::string> smoothing_levels = {"Raw (0)", "Smoothed (1)"};
    } processing_params_;

    /**
     * @brief Initialize processor components
     */
    void initialize_components();

    /**
     * @brief Process Bollinger Bands
     */
    ProcessingResult process_bollinger_bands(SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process candles data
     */
    ProcessingResult process_candles_data(SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process RSI data
     */
    ProcessingResult process_rsi_data(SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process smoothed data
     */
    ProcessingResult process_smoothed_data(SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process extreme lines
     */
    ProcessingResult process_extreme_lines(SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process order book volumes
     */
    ProcessingResult process_order_book_volumes(SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process velocity data
     */
    ProcessingResult process_velocity_data(SymbolId symbol_id, const std::vector<TradeData>& trades);

};

} // namespace okx_data_processor
