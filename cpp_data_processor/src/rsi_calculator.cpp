#include "rsi_calculator.h"
#include <algorithm>
#include <numeric>
#include <cmath>

namespace okx_data_processor {

RSICalculator::RSICalculator() : period_(14) {
}

RSICalculator::RSICalculator(int32_t period) : period_(period) {
}

RSIData RSICalculator::calculate(const std::vector<double>& prices) {
    RSIData result(period_);
    
    if (!has_enough_data(prices.size())) {
        return result; // Return empty result if not enough data
    }
    
    // Calculate price changes
    std::vector<double> changes = calculate_price_changes(prices);
    
    // Separate gains and losses
    auto [gains, losses] = separate_gains_losses(changes);
    
    // Calculate initial averages
    auto [initial_avg_gain, initial_avg_loss] = calculate_initial_averages(gains, losses, period_ - 1);
    
    // Calculate smoothed averages
    auto [avg_gains, avg_losses] = calculate_smoothed_averages(
        gains, losses, initial_avg_gain, initial_avg_loss, period_);
    
    // Calculate RSI values
    result.rsi_values = calculate_rsi_values(avg_gains, avg_losses);
    
    return result;
}

RSIData RSICalculator::calculate_from_trades(const std::vector<TradeData>& trades) {
    std::vector<double> prices = extract_close_prices(trades);
    return calculate(prices);
}

RSIData RSICalculator::calculate_from_candles(const std::vector<CandleData>& candles) {
    std::vector<double> prices = extract_close_prices_from_candles(candles);
    return calculate(prices);
}

void RSICalculator::set_period(int32_t period) {
    period_ = period;
}

int32_t RSICalculator::get_period() const {
    return period_;
}

bool RSICalculator::has_enough_data(size_t data_size) const {
    return data_size > static_cast<size_t>(period_);
}

std::vector<double> RSICalculator::calculate_smoothed_ma(const std::vector<double>& values, int32_t period) {
    std::vector<double> smoothed;
    smoothed.reserve(values.size());
    
    if (values.empty() || period <= 0) {
        return smoothed;
    }
    
    // First value is the first value itself
    smoothed.push_back(values[0]);
    
    // Calculate smoothed moving average
    for (size_t i = 1; i < values.size(); ++i) {
        double smoothed_value = (smoothed[i - 1] * (period - 1) + values[i]) / period;
        smoothed.push_back(smoothed_value);
    }
    
    return smoothed;
}

std::vector<double> RSICalculator::calculate_price_changes(const std::vector<double>& prices) const {
    std::vector<double> changes;
    changes.reserve(prices.size() - 1);
    
    for (size_t i = 1; i < prices.size(); ++i) {
        changes.push_back(prices[i] - prices[i - 1]);
    }
    
    return changes;
}

std::pair<std::vector<double>, std::vector<double>> RSICalculator::separate_gains_losses(
    const std::vector<double>& changes) const {
    
    std::vector<double> gains, losses;
    gains.reserve(changes.size());
    losses.reserve(changes.size());
    
    for (double change : changes) {
        if (change > 0) {
            gains.push_back(change);
            losses.push_back(0.0);
        } else if (change < 0) {
            gains.push_back(0.0);
            losses.push_back(-change); // Store as positive value
        } else {
            gains.push_back(0.0);
            losses.push_back(0.0);
        }
    }
    
    return std::make_pair(gains, losses);
}

std::pair<double, double> RSICalculator::calculate_initial_averages(
    const std::vector<double>& gains, const std::vector<double>& losses, 
    size_t start_idx) const {
    
    double sum_gains = 0.0;
    double sum_losses = 0.0;
    
    for (size_t i = start_idx - period_ + 1; i <= start_idx; ++i) {
        sum_gains += gains[i];
        sum_losses += losses[i];
    }
    
    double avg_gain = sum_gains / period_;
    double avg_loss = sum_losses / period_;
    
    return std::make_pair(avg_gain, avg_loss);
}

std::pair<std::vector<double>, std::vector<double>> RSICalculator::calculate_smoothed_averages(
    const std::vector<double>& gains, const std::vector<double>& losses,
    double initial_avg_gain, double initial_avg_loss, size_t start_idx) const {
    
    std::vector<double> avg_gains, avg_losses;
    avg_gains.reserve(gains.size() - start_idx);
    avg_losses.reserve(losses.size() - start_idx);
    
    // Initialize with the first smoothed averages
    avg_gains.push_back(initial_avg_gain);
    avg_losses.push_back(initial_avg_loss);
    
    // Calculate subsequent smoothed averages using Wilder's smoothing
    for (size_t i = start_idx + 1; i < gains.size(); ++i) {
        double prev_avg_gain = avg_gains.back();
        double prev_avg_loss = avg_losses.back();
        
        double new_avg_gain = (prev_avg_gain * (period_ - 1) + gains[i]) / period_;
        double new_avg_loss = (prev_avg_loss * (period_ - 1) + losses[i]) / period_;
        
        avg_gains.push_back(new_avg_gain);
        avg_losses.push_back(new_avg_loss);
    }
    
    return std::make_pair(avg_gains, avg_losses);
}

std::vector<double> RSICalculator::calculate_rsi_values(
    const std::vector<double>& avg_gains, const std::vector<double>& avg_losses) const {
    
    std::vector<double> rsi_values;
    rsi_values.reserve(avg_gains.size());
    
    for (size_t i = 0; i < avg_gains.size(); ++i) {
        double avg_gain = avg_gains[i];
        double avg_loss = avg_losses[i];
        
        if (avg_loss == 0.0) {
            // If average loss is 0, RSI is 100
            rsi_values.push_back(100.0);
        } else {
            double rs = avg_gain / avg_loss;
            double rsi = 100.0 - (100.0 / (1.0 + rs));
            rsi_values.push_back(rsi);
        }
    }
    
    return rsi_values;
}

std::vector<double> RSICalculator::extract_close_prices(const std::vector<TradeData>& trades) const {
    std::vector<double> prices;
    prices.reserve(trades.size());
    
    for (const auto& trade : trades) {
        prices.push_back(trade.price);
    }
    
    return prices;
}

std::vector<double> RSICalculator::extract_close_prices_from_candles(const std::vector<CandleData>& candles) const {
    std::vector<double> prices;
    prices.reserve(candles.size());
    
    for (const auto& candle : candles) {
        prices.push_back(candle.close_price);
    }
    
    return prices;
}

} // namespace okx_data_processor
