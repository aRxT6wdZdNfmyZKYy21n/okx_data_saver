#pragma once

#include "data_structures.h"
#include <vector>

namespace okx_data_processor {

/**
 * @brief Bollinger Bands processor
 * 
 * Calculates Bollinger Bands using Simple Moving Average (SMA)
 * with configurable period and standard deviation multiplier.
 */
class BollingerBandsProcessor {
public:
    BollingerBandsProcessor();
    explicit BollingerBandsProcessor(int32_t period, double std_dev_multiplier = 2.0);
    ~BollingerBandsProcessor() = default;

    // Disable copy constructor and assignment operator
    BollingerBandsProcessor(const BollingerBandsProcessor&) = delete;
    BollingerBandsProcessor& operator=(const BollingerBandsProcessor&) = delete;

    /**
     * @brief Calculate Bollinger Bands for given price data
     * 
     * @param prices Vector of price values
     * @return BollingerBands Calculated Bollinger Bands
     */
    BollingerBands calculate(const std::vector<double>& prices);

    /**
     * @brief Calculate Bollinger Bands for trade data
     * 
     * @param trades Vector of trade data
     * @return BollingerBands Calculated Bollinger Bands
     */
    BollingerBands calculate_from_trades(const std::vector<TradeData>& trades);

    /**
     * @brief Set calculation parameters
     * 
     * @param period Moving average period
     * @param std_dev_multiplier Standard deviation multiplier
     */
    void set_parameters(int32_t period, double std_dev_multiplier = 2.0);

    /**
     * @brief Get current parameters
     * 
     * @return std::pair<int32_t, double> Period and standard deviation multiplier
     */
    std::pair<int32_t, double> get_parameters() const;

    /**
     * @brief Check if enough data is available for calculation
     * 
     * @param data_size Size of input data
     * @return bool True if enough data is available
     */
    bool has_enough_data(size_t data_size) const;

private:
    int32_t period_;
    double std_dev_multiplier_;

    /**
     * @brief Calculate Simple Moving Average
     * 
     * @param prices Vector of price values
     * @param start_idx Starting index
     * @param length Length of the window
     * @return double Moving average value
     */
    double calculate_sma(const std::vector<double>& prices, size_t start_idx, size_t length) const;

    /**
     * @brief Calculate standard deviation
     * 
     * @param prices Vector of price values
     * @param start_idx Starting index
     * @param length Length of the window
     * @param mean Mean value for the window
     * @return double Standard deviation value
     */
    double calculate_std_dev(const std::vector<double>& prices, size_t start_idx, 
                            size_t length, double mean) const;

    /**
     * @brief Extract prices from trade data
     * 
     * @param trades Vector of trade data
     * @return std::vector<double> Vector of prices
     */
    std::vector<double> extract_prices(const std::vector<TradeData>& trades) const;
};

} // namespace okx_data_processor
