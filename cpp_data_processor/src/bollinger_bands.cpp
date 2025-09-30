#include "bollinger_bands.h"
#include <cmath>
#include <algorithm>
#include <numeric>

namespace okx_data_processor {

BollingerBandsProcessor::BollingerBandsProcessor() 
    : period_(20), std_dev_multiplier_(2.0) {
}

BollingerBandsProcessor::BollingerBandsProcessor(int32_t period, double std_dev_multiplier)
    : period_(period), std_dev_multiplier_(std_dev_multiplier) {
}

BollingerBands BollingerBandsProcessor::calculate(const std::vector<double>& prices) {
    BollingerBands result(period_);
    
    if (!has_enough_data(prices.size())) {
        return result; // Return empty result if not enough data
    }
    
    size_t data_size = prices.size();
    result.upper_band.reserve(data_size);
    result.middle_band.reserve(data_size);
    result.lower_band.reserve(data_size);
    
    // Calculate Bollinger Bands for each point
    for (size_t i = 0; i < data_size; ++i) {
        if (i < static_cast<size_t>(period_ - 1)) {
            // Not enough data for this point
            result.upper_band.push_back(std::numeric_limits<double>::quiet_NaN());
            result.middle_band.push_back(std::numeric_limits<double>::quiet_NaN());
            result.lower_band.push_back(std::numeric_limits<double>::quiet_NaN());
        } else {
            // Calculate SMA (middle band)
            double sma = calculate_sma(prices, i - period_ + 1, period_);
            result.middle_band.push_back(sma);
            
            // Calculate standard deviation
            double std_dev = calculate_std_dev(prices, i - period_ + 1, period_, sma);
            
            // Calculate upper and lower bands
            double upper = sma + (std_dev_multiplier_ * std_dev);
            double lower = sma - (std_dev_multiplier_ * std_dev);
            
            result.upper_band.push_back(upper);
            result.lower_band.push_back(lower);
        }
    }
    
    return result;
}

BollingerBands BollingerBandsProcessor::calculate_from_trades(const std::vector<TradeData>& trades) {
    std::vector<double> prices = extract_prices(trades);
    return calculate(prices);
}

void BollingerBandsProcessor::set_parameters(int32_t period, double std_dev_multiplier) {
    period_ = period;
    std_dev_multiplier_ = std_dev_multiplier;
}

std::pair<int32_t, double> BollingerBandsProcessor::get_parameters() const {
    return std::make_pair(period_, std_dev_multiplier_);
}

bool BollingerBandsProcessor::has_enough_data(size_t data_size) const {
    return data_size >= static_cast<size_t>(period_);
}

double BollingerBandsProcessor::calculate_sma(const std::vector<double>& prices, 
                                            size_t start_idx, size_t length) const {
    double sum = 0.0;
    for (size_t i = 0; i < length; ++i) {
        sum += prices[start_idx + i];
    }
    return sum / static_cast<double>(length);
}

double BollingerBandsProcessor::calculate_std_dev(const std::vector<double>& prices, 
                                                 size_t start_idx, size_t length, 
                                                 double mean) const {
    double sum_squared_diff = 0.0;
    for (size_t i = 0; i < length; ++i) {
        double diff = prices[start_idx + i] - mean;
        sum_squared_diff += diff * diff;
    }
    double variance = sum_squared_diff / static_cast<double>(length);
    return std::sqrt(variance);
}

std::vector<double> BollingerBandsProcessor::extract_prices(const std::vector<TradeData>& trades) const {
    std::vector<double> prices;
    prices.reserve(trades.size());
    
    for (const auto& trade : trades) {
        prices.push_back(trade.price);
    }
    
    return prices;
}

} // namespace okx_data_processor
