#pragma once

#include "data_structures.h"
#include <vector>
#include <string>
#include <map>
#include <pybind11/pybind11.h>

namespace okx_data_processor {

/**
 * @brief Velocity calculator
 * 
 * Calculates trading velocity metrics from candles data
 * and other trading indicators.
 */
class VelocityCalculator {
public:
    VelocityCalculator();
    ~VelocityCalculator() = default;

    // Disable copy constructor and assignment operator
    VelocityCalculator(const VelocityCalculator&) = delete;
    VelocityCalculator& operator=(const VelocityCalculator&) = delete;

    /**
     * @brief Calculate velocity data from candles
     * 
     * @param symbol_id Symbol identifier
     * @param candles Vector of candle data
     * @param interval Interval name (e.g., "1m", "5m", "1h")
     * @return VelocityData Calculated velocity data
     */
    VelocityData calculate_velocity_from_candles(
        SymbolId symbol_id, const std::vector<CandleData>& candles, const std::string& interval);

    /**
     * @brief Calculate velocity data from trades
     * 
     * @param symbol_id Symbol identifier
     * @param trades Vector of trade data
     * @param interval Interval name
     * @return VelocityData Calculated velocity data
     */
    VelocityData calculate_velocity_from_trades(
        SymbolId symbol_id, const std::vector<TradeData>& trades, const std::string& interval);

    /**
     * @brief Calculate velocity for multiple intervals
     * 
     * @param symbol_id Symbol identifier
     * @param candles_map Map of candles by interval
     * @return std::map<std::string, VelocityData> Velocity data by interval
     */
    std::map<std::string, VelocityData> calculate_velocity_for_intervals(
        SymbolId symbol_id, const std::map<std::string, std::vector<CandleData>>& candles_map);

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
     * @brief Set calculation parameters
     * 
     * @param params Dictionary of parameters
     */
    void set_calculation_params(const pybind11::dict& params);

    /**
     * @brief Get calculation parameters
     * 
     * @return pybind11::dict Dictionary of parameters
     */
    pybind11::dict get_calculation_params() const;

private:
    // Interval configurations
    std::map<std::string, int64_t> interval_durations_;
    
    // Calculation parameters
    struct CalculationParams {
        bool enable_trades_count_velocity = true;
        bool enable_volume_velocity = true;
        bool enable_price_velocity = true;
        double smoothing_factor = 0.1;
        int32_t min_data_points = 5;
    } params_;

    // Default intervals
    static const std::map<std::string, int64_t> DEFAULT_INTERVALS;

    /**
     * @brief Initialize default intervals
     */
    void initialize_default_intervals();

    /**
     * @brief Calculate trades count velocity
     * 
     * @param candles Vector of candle data
     * @return std::vector<double> Trades count velocity values
     */
    std::vector<double> calculate_trades_count_velocity(const std::vector<CandleData>& candles) const;

    /**
     * @brief Calculate volume velocity
     * 
     * @param candles Vector of candle data
     * @return std::vector<double> Volume velocity values
     */
    std::vector<double> calculate_volume_velocity(const std::vector<CandleData>& candles) const;

    /**
     * @brief Calculate price velocity
     * 
     * @param candles Vector of candle data
     * @return std::vector<double> Price velocity values
     */
    std::vector<double> calculate_price_velocity(const std::vector<CandleData>& candles) const;

    /**
     * @brief Calculate velocity from trades data
     * 
     * @param trades Vector of trade data
     * @param interval_duration_ms Interval duration in milliseconds
     * @return std::vector<double> Velocity values
     */
    std::vector<double> calculate_velocity_from_trades_impl(
        const std::vector<TradeData>& trades, int64_t interval_duration_ms) const;

    /**
     * @brief Apply smoothing to velocity values
     * 
     * @param values Vector of velocity values
     * @return std::vector<double> Smoothed velocity values
     */
    std::vector<double> apply_smoothing(const std::vector<double>& values) const;

    /**
     * @brief Calculate exponential moving average
     * 
     * @param values Vector of values
     * @param alpha Smoothing factor
     * @return std::vector<double> Smoothed values
     */
    std::vector<double> calculate_ema(const std::vector<double>& values, double alpha) const;

    /**
     * @brief Calculate simple moving average
     * 
     * @param values Vector of values
     * @param window_size Window size
     * @return std::vector<double> Smoothed values
     */
    std::vector<double> calculate_sma(const std::vector<double>& values, int32_t window_size) const;

    /**
     * @brief Calculate velocity between consecutive values
     * 
     * @param values Vector of values
     * @return std::vector<double> Velocity values
     */
    std::vector<double> calculate_velocity_between_values(const std::vector<double>& values) const;

    /**
     * @brief Calculate trades count per interval from trades data
     * 
     * @param trades Vector of trade data
     * @param interval_duration_ms Interval duration in milliseconds
     * @return std::vector<int32_t> Trades count per interval
     */
    std::vector<int32_t> calculate_trades_count_per_interval(
        const std::vector<TradeData>& trades, int64_t interval_duration_ms) const;

    /**
     * @brief Calculate volume per interval from trades data
     * 
     * @param trades Vector of trade data
     * @param interval_duration_ms Interval duration in milliseconds
     * @return std::vector<double> Volume per interval
     */
    std::vector<double> calculate_volume_per_interval(
        const std::vector<TradeData>& trades, int64_t interval_duration_ms) const;

    /**
     * @brief Calculate price change per interval from trades data
     * 
     * @param trades Vector of trade data
     * @param interval_duration_ms Interval duration in milliseconds
     * @return std::vector<double> Price change per interval
     */
    std::vector<double> calculate_price_change_per_interval(
        const std::vector<TradeData>& trades, int64_t interval_duration_ms) const;

    /**
     * @brief Group trades by time intervals
     * 
     * @param trades Vector of trade data
     * @param interval_duration_ms Interval duration in milliseconds
     * @return std::map<int64_t, std::vector<TradeData>> Trades grouped by interval
     */
    std::map<int64_t, std::vector<TradeData>> group_trades_by_interval(
        const std::vector<TradeData>& trades, int64_t interval_duration_ms) const;

    /**
     * @brief Calculate interval start timestamp
     * 
     * @param timestamp_ms Trade timestamp in milliseconds
     * @param interval_duration_ms Interval duration in milliseconds
     * @return int64_t Interval start timestamp in milliseconds
     */
    int64_t calculate_interval_start_timestamp(int64_t timestamp_ms, int64_t interval_duration_ms) const;
};

} // namespace okx_data_processor
