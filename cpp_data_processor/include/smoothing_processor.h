#pragma once

#include "data_structures.h"
#include <vector>
#include <string>
#include <map>

namespace okx_data_processor {

/**
 * @brief Data smoothing processor
 * 
 * Implements various smoothing algorithms for financial data,
 * including level-based smoothing as used in the original Python processor.
 */
class SmoothingProcessor {
public:
    SmoothingProcessor();
    ~SmoothingProcessor() = default;

    // Disable copy constructor and assignment operator
    SmoothingProcessor(const SmoothingProcessor&) = delete;
    SmoothingProcessor& operator=(const SmoothingProcessor&) = delete;

    /**
     * @brief Process smoothed data for all configured levels
     * 
     * @param symbol_id Symbol identifier
     * @param trades Vector of trade data
     * @return std::map<std::string, std::vector<SmoothedLine>> Smoothed lines by level
     */
    std::map<std::string, std::vector<SmoothedLine>> process_smoothed_data(
        SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process smoothed data for specific level
     * 
     * @param symbol_id Symbol identifier
     * @param trades Vector of trade data
     * @param level Smoothing level (e.g., "Smoothed (1)")
     * @return std::vector<SmoothedLine> Smoothed lines for the level
     */
    std::vector<SmoothedLine> process_level_data(
        SymbolId symbol_id, const std::vector<TradeData>& trades, const std::string& level);

    /**
     * @brief Process smoothed data points for all configured levels
     * 
     * @param symbol_id Symbol identifier
     * @param trades Vector of trade data
     * @return std::map<std::string, std::vector<SmoothedDataPoint>> Smoothed data points by level
     */
    std::map<std::string, std::vector<SmoothedDataPoint>> process_smoothed_data_points(
        SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process smoothed data points for specific level
     * 
     * @param symbol_id Symbol identifier
     * @param trades Vector of trade data
     * @param level Smoothing level (e.g., "Smoothed (1)")
     * @return std::vector<SmoothedDataPoint> Smoothed data points for the level
     */
    std::vector<SmoothedDataPoint> process_level_data_points(
        SymbolId symbol_id, const std::vector<TradeData>& trades, const std::string& level);

    /**
     * @brief Add smoothing level configuration
     * 
     * @param level_name Level name
     * @param level_number Level number
     */
    void add_smoothing_level(const std::string& level_name, int32_t level_number);

    /**
     * @brief Get configured smoothing levels
     * 
     * @return std::vector<std::string> List of configured level names
     */
    std::vector<std::string> get_configured_levels() const;

    /**
     * @brief Set minimum trade ID for incremental processing
     * 
     * @param symbol_id Symbol identifier
     * @param level_name Level name
     * @param min_trade_id Minimum trade ID
     */
    void set_min_trade_id(SymbolId symbol_id, const std::string& level_name, int64_t min_trade_id);

    /**
     * @brief Get minimum trade ID for incremental processing
     * 
     * @param symbol_id Symbol identifier
     * @param level_name Level name
     * @return int64_t Minimum trade ID
     */
    int64_t get_min_trade_id(SymbolId symbol_id, const std::string& level_name) const;

private:
    // Smoothing level configurations
    std::map<std::string, int32_t> smoothing_levels_;
    
    // Minimum trade IDs for incremental processing
    std::map<std::pair<SymbolId, std::string>, int64_t> min_trade_ids_;

    // Default smoothing levels
    static const std::map<std::string, int32_t> DEFAULT_SMOOTHING_LEVELS;

    /**
     * @brief Initialize default smoothing levels
     */
    void initialize_default_levels();

    /**
     * @brief Process level 1 smoothing (lines and smoothed data)
     * 
     * @param trades Vector of trade data
     * @return std::vector<SmoothedLine> Processed lines
     */
    std::vector<SmoothedLine> process_level_1_smoothing(const std::vector<TradeData>& trades) const;

    /**
     * @brief Calculate level 1 lines from trades
     * 
     * @param trades Vector of trade data
     * @return std::vector<SmoothedLine> Calculated lines
     */
    std::vector<SmoothedLine> calculate_level_1_lines(const std::vector<TradeData>& trades) const;

    /**
     * @brief Calculate smoothed data from lines
     * 
     * @param lines Vector of smoothed lines
     * @return std::vector<SmoothedLine> Smoothed data points
     */
    std::vector<SmoothedLine> calculate_smoothed_from_lines(const std::vector<SmoothedLine>& lines) const;

    /**
     * @brief Calculate smoothed data points from lines
     * 
     * @param lines Vector of smoothed lines
     * @return std::vector<SmoothedDataPoint> Smoothed data points
     */
    std::vector<SmoothedDataPoint> calculate_smoothed_data_points_from_lines(const std::vector<SmoothedLine>& lines) const;

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
     * @brief Sort lines by start trade ID
     * 
     * @param lines Vector of lines to sort
     */
    void sort_lines_by_start_trade_id(std::vector<SmoothedLine>& lines) const;

    /**
     * @brief Create a new line from trade data
     * 
     * @param trade Trade data
     * @return SmoothedLine New line
     */
    SmoothedLine create_line_from_trade(const TradeData& trade) const;

    /**
     * @brief Update existing line with new trade data
     * 
     * @param line Existing line to update
     * @param trade New trade data
     */
    void update_line_with_trade(SmoothedLine& line, const TradeData& trade) const;

    /**
     * @brief Check if trade should continue current line
     * 
     * @param line Current line
     * @param trade New trade
     * @return bool True if trade should continue the line
     */
    bool should_continue_line(const SmoothedLine& line, const TradeData& trade) const;
};

} // namespace okx_data_processor
