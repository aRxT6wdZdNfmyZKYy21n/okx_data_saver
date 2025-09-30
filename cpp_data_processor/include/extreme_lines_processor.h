#pragma once

#include "data_structures.h"
#include <vector>
#include <string>
#include <map>
#include <pybind11/pybind11.h>

namespace okx_data_processor {

/**
 * @brief Extreme lines processor
 * 
 * Processes smoothed lines to identify extreme price levels
 * and create extreme lines arrays for visualization.
 */
class ExtremeLinesProcessor {
public:
    ExtremeLinesProcessor();
    ~ExtremeLinesProcessor() = default;

    // Disable copy constructor and assignment operator
    ExtremeLinesProcessor(const ExtremeLinesProcessor&) = delete;
    ExtremeLinesProcessor& operator=(const ExtremeLinesProcessor&) = delete;

    /**
     * @brief Process extreme lines from smoothed lines
     * 
     * @param symbol_id Symbol identifier
     * @param smoothed_lines Vector of smoothed lines
     * @param trades Vector of trade data for context
     * @return std::vector<ExtremeLine> Processed extreme lines
     */
    std::vector<ExtremeLine> process_extreme_lines(
        SymbolId symbol_id, 
        const std::vector<SmoothedLine>& smoothed_lines,
        const std::vector<TradeData>& trades);

    /**
     * @brief Create extreme lines array for visualization
     * 
     * @param extreme_lines Vector of extreme lines
     * @param trades Vector of trade data for context
     * @param width Array width
     * @param height Array height
     * @return std::vector<std::vector<double>> 2D array of extreme lines
     */
    std::vector<std::vector<double>> create_extreme_lines_array(
        const std::vector<ExtremeLine>& extreme_lines,
        const std::vector<TradeData>& trades,
        int32_t width, int32_t height);

    /**
     * @brief Calculate array dimensions based on trade data
     * 
     * @param trades Vector of trade data
     * @param height Desired height
     * @return std::pair<int32_t, int32_t> Width and height
     */
    std::pair<int32_t, int32_t> calculate_array_dimensions(
        const std::vector<TradeData>& trades, int32_t height = 100) const;

    /**
     * @brief Calculate scale factor for array coordinates
     * 
     * @param trades Vector of trade data
     * @param width Array width
     * @param height Array height
     * @return double Scale factor
     */
    double calculate_scale_factor(
        const std::vector<TradeData>& trades, int32_t width, int32_t height) const;

    /**
     * @brief Set processing parameters
     * 
     * @param params Dictionary of parameters
     */
    void set_processing_params(const pybind11::dict& params);

    /**
     * @brief Get processing parameters
     * 
     * @return pybind11::dict Dictionary of parameters
     */
    pybind11::dict get_processing_params() const;

private:
    // Processing parameters
    struct ProcessingParams {
        int32_t default_height = 100;
        double min_price_delta = 0.001;
        double min_trade_id_delta = 1.0;
    } params_;

    /**
     * @brief Find extreme price levels from smoothed lines
     * 
     * @param smoothed_lines Vector of smoothed lines
     * @return std::vector<double> Extreme price levels
     */
    std::vector<double> find_extreme_prices(const std::vector<SmoothedLine>& smoothed_lines) const;

    /**
     * @brief Process extreme lines with intersection detection
     * 
     * @param extreme_prices Vector of extreme prices
     * @param smoothed_lines Vector of smoothed lines
     * @return std::vector<ExtremeLine> Processed extreme lines
     */
    std::vector<ExtremeLine> process_extreme_lines_with_intersections(
        const std::vector<double>& extreme_prices,
        const std::vector<SmoothedLine>& smoothed_lines) const;

    /**
     * @brief Check if price is within line range
     * 
     * @param price Price to check
     * @param start_price Line start price
     * @param end_price Line end price
     * @return bool True if price is within range
     */
    bool is_price_in_range(double price, double start_price, double end_price) const;

    /**
     * @brief Find intersections between extreme lines and smoothed lines
     * 
     * @param extreme_price Extreme price level
     * @param smoothed_lines Vector of smoothed lines
     * @return std::vector<int64_t> Trade IDs where intersections occur
     */
    std::vector<int64_t> find_intersections(
        double extreme_price, const std::vector<SmoothedLine>& smoothed_lines) const;

    /**
     * @brief Fill extreme lines array
     * 
     * @param extreme_lines Vector of extreme lines
     * @param array 2D array to fill
     * @param width Array width
     * @param height Array height
     * @param scale Scale factor
     * @param min_trade_id Minimum trade ID
     * @param min_price Minimum price
     */
    void fill_extreme_lines_array(
        const std::vector<ExtremeLine>& extreme_lines,
        std::vector<std::vector<double>>& array,
        int32_t width, int32_t height, double scale,
        int64_t min_trade_id, double min_price) const;

    /**
     * @brief Get price and trade ID ranges from trades
     * 
     * @param trades Vector of trade data
     * @return std::pair<std::pair<double, double>, std::pair<int64_t, int64_t>> 
     *         Price range (min, max) and trade ID range (min, max)
     */
    std::pair<std::pair<double, double>, std::pair<int64_t, int64_t>> get_ranges(
        const std::vector<TradeData>& trades) const;
};

} // namespace okx_data_processor
