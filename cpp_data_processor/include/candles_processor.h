#pragma once

#include "data_structures.h"
#include <vector>
#include <string>
#include <map>
#include <chrono>

namespace okx_data_processor {

/**
 * @brief Candle data processor
 * 
 * Processes trade data into candlestick data for different time intervals.
 * Supports multiple intervals and incremental updates.
 */
class CandlesProcessor {
public:
    CandlesProcessor();
    ~CandlesProcessor() = default;

    // Disable copy constructor and assignment operator
    CandlesProcessor(const CandlesProcessor&) = delete;
    CandlesProcessor& operator=(const CandlesProcessor&) = delete;

    /**
     * @brief Process trades into candles for all configured intervals
     * 
     * @param symbol_id Symbol identifier
     * @param trades Vector of trade data
     * @return std::map<std::string, std::vector<CandleData>> Candles data by interval
     */
    std::map<std::string, std::vector<CandleData>> process_trades(
        SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process trades into candles for specific interval
     * 
     * @param symbol_id Symbol identifier
     * @param trades Vector of trade data
     * @param interval_name Interval name (e.g., "1m", "5m", "1h")
     * @return std::vector<CandleData> Candles data for the interval
     */
    std::vector<CandleData> process_trades_for_interval(
        SymbolId symbol_id, const std::vector<TradeData>& trades, const std::string& interval_name);

    /**
     * @brief Add interval configuration
     * 
     * @param interval_name Interval name
     * @param duration_ms Duration in milliseconds
     */
    void add_interval(const std::string& interval_name, int64_t duration_ms);

    /**
     * @brief Get configured intervals
     * 
     * @return std::vector<std::string> List of configured interval names
     */
    std::vector<std::string> get_configured_intervals() const;

    /**
     * @brief Set minimum trade ID for incremental processing
     * 
     * @param symbol_id Symbol identifier
     * @param interval_name Interval name
     * @param min_trade_id Minimum trade ID
     */
    void set_min_trade_id(SymbolId symbol_id, const std::string& interval_name, int64_t min_trade_id);

    /**
     * @brief Get minimum trade ID for incremental processing
     * 
     * @param symbol_id Symbol identifier
     * @param interval_name Interval name
     * @return int64_t Minimum trade ID
     */
    int64_t get_min_trade_id(SymbolId symbol_id, const std::string& interval_name) const;

private:
    // Interval configurations
    std::map<std::string, int64_t> interval_durations_;
    
    // Minimum trade IDs for incremental processing
    std::map<std::pair<SymbolId, std::string>, int64_t> min_trade_ids_;

    // Default intervals
    static const std::map<std::string, int64_t> DEFAULT_INTERVALS;

    /**
     * @brief Initialize default intervals
     */
    void initialize_default_intervals();

    /**
     * @brief Calculate candle start timestamp
     * 
     * @param timestamp_ms Trade timestamp in milliseconds
     * @param interval_duration_ms Interval duration in milliseconds
     * @return int64_t Candle start timestamp in milliseconds
     */
    int64_t calculate_candle_start_timestamp(int64_t timestamp_ms, int64_t interval_duration_ms) const;

    /**
     * @brief Process trades for a single interval
     * 
     * @param trades Vector of trade data
     * @param interval_duration_ms Interval duration in milliseconds
     * @param min_trade_id Minimum trade ID to process
     * @return std::vector<CandleData> Processed candles
     */
    std::vector<CandleData> process_trades_for_interval_impl(
        const std::vector<TradeData>& trades, int64_t interval_duration_ms, int64_t min_trade_id) const;

    /**
     * @brief Create a new candle from trade data
     * 
     * @param trade Trade data
     * @param interval_duration_ms Interval duration in milliseconds
     * @return CandleData New candle
     */
    CandleData create_candle_from_trade(const TradeData& trade, int64_t interval_duration_ms) const;

    /**
     * @brief Update existing candle with new trade data
     * 
     * @param candle Existing candle to update
     * @param trade New trade data
     */
    void update_candle_with_trade(CandleData& candle, const TradeData& trade) const;

    /**
     * @brief Filter trades by minimum trade ID
     * 
     * @param trades Vector of trade data
     * @param min_trade_id Minimum trade ID
     * @return std::vector<TradeData> Filtered trades
     */
    std::vector<TradeData> filter_trades_by_min_id(
        const std::vector<TradeData>& trades, int64_t min_trade_id) const;

    /**
     * @brief Sort candles by start trade ID
     * 
     * @param candles Vector of candles to sort
     */
    void sort_candles_by_start_trade_id(std::vector<CandleData>& candles) const;
};

} // namespace okx_data_processor
