#pragma once

#include "data_structures.h"
#include <vector>

namespace okx_data_processor {

/**
 * @brief RSI (Relative Strength Index) calculator
 * 
 * Calculates RSI using the standard formula with configurable period.
 * RSI is calculated as: RSI = 100 - (100 / (1 + RS))
 * where RS = Average Gain / Average Loss
 */
class RSICalculator {
public:
    RSICalculator();
    explicit RSICalculator(int32_t period);
    ~RSICalculator() = default;

    // Disable copy constructor and assignment operator
    RSICalculator(const RSICalculator&) = delete;
    RSICalculator& operator=(const RSICalculator&) = delete;

    /**
     * @brief Calculate RSI for given price data
     * 
     * @param prices Vector of price values
     * @return RSIData Calculated RSI data
     */
    RSIData calculate(const std::vector<double>& prices);

    /**
     * @brief Calculate RSI for trade data
     * 
     * @param trades Vector of trade data
     * @return RSIData Calculated RSI data
     */
    RSIData calculate_from_trades(const std::vector<TradeData>& trades);

    /**
     * @brief Calculate RSI from candle data
     * 
     * @param candles Vector of candle data
     * @return RSIData Calculated RSI data
     */
    RSIData calculate_from_candles(const std::vector<CandleData>& candles);

    /**
     * @brief Set calculation period
     * 
     * @param period RSI calculation period
     */
    void set_period(int32_t period);

    /**
     * @brief Get current period
     * 
     * @return int32_t Current RSI period
     */
    int32_t get_period() const;

    /**
     * @brief Check if enough data is available for calculation
     * 
     * @param data_size Size of input data
     * @return bool True if enough data is available
     */
    bool has_enough_data(size_t data_size) const;

    /**
     * @brief Calculate smoothed moving average
     * 
     * @param values Vector of values
     * @param period Period for smoothing
     * @return std::vector<double> Smoothed values
     */
    static std::vector<double> calculate_smoothed_ma(const std::vector<double>& values, int32_t period);

private:
    int32_t period_;

    /**
     * @brief Calculate price changes
     * 
     * @param prices Vector of price values
     * @return std::vector<double> Price changes
     */
    std::vector<double> calculate_price_changes(const std::vector<double>& prices) const;

    /**
     * @brief Separate gains and losses from price changes
     * 
     * @param changes Vector of price changes
     * @return std::pair<std::vector<double>, std::vector<double>> Gains and losses
     */
    std::pair<std::vector<double>, std::vector<double>> separate_gains_losses(
        const std::vector<double>& changes) const;

    /**
     * @brief Calculate initial average gain and loss
     * 
     * @param gains Vector of gains
     * @param losses Vector of losses
     * @param start_idx Starting index
     * @return std::pair<double, double> Average gain and loss
     */
    std::pair<double, double> calculate_initial_averages(
        const std::vector<double>& gains, const std::vector<double>& losses, 
        size_t start_idx) const;

    /**
     * @brief Calculate smoothed average gain and loss
     * 
     * @param gains Vector of gains
     * @param losses Vector of losses
     * @param initial_avg_gain Initial average gain
     * @param initial_avg_loss Initial average loss
     * @param start_idx Starting index
     * @return std::pair<std::vector<double>, std::vector<double>> Smoothed averages
     */
    std::pair<std::vector<double>, std::vector<double>> calculate_smoothed_averages(
        const std::vector<double>& gains, const std::vector<double>& losses,
        double initial_avg_gain, double initial_avg_loss, size_t start_idx) const;

    /**
     * @brief Calculate RSI values from smoothed averages
     * 
     * @param avg_gains Vector of average gains
     * @param avg_losses Vector of average losses
     * @return std::vector<double> RSI values
     */
    std::vector<double> calculate_rsi_values(
        const std::vector<double>& avg_gains, const std::vector<double>& avg_losses) const;

    /**
     * @brief Extract close prices from trade data
     * 
     * @param trades Vector of trade data
     * @return std::vector<double> Vector of close prices
     */
    std::vector<double> extract_close_prices(const std::vector<TradeData>& trades) const;

    /**
     * @brief Extract close prices from candle data
     * 
     * @param candles Vector of candle data
     * @return std::vector<double> Vector of close prices
     */
    std::vector<double> extract_close_prices_from_candles(const std::vector<CandleData>& candles) const;
};

} // namespace okx_data_processor
