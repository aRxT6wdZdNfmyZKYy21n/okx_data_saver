#include "velocity_calculator.h"
#include <algorithm>
#include <cmath>
#include <chrono>

namespace okx_data_processor {

// Default intervals
const std::map<std::string, int64_t> VelocityCalculator::DEFAULT_INTERVALS = {
    {"1m", 60 * 1000},           // 1 minute
    {"5m", 5 * 60 * 1000},       // 5 minutes
    {"15m", 15 * 60 * 1000},     // 15 minutes
    {"1h", 60 * 60 * 1000},      // 1 hour
    {"4h", 4 * 60 * 60 * 1000},  // 4 hours
    {"1d", 24 * 60 * 60 * 1000}  // 1 day
};

VelocityCalculator::VelocityCalculator() {
    initialize_default_intervals();
}

VelocityData VelocityCalculator::calculate_velocity_from_candles(
    SymbolId symbol_id, const std::vector<CandleData>& candles, const std::string& interval) {
    
    VelocityData result(interval);
    
    if (candles.empty()) {
        return result;
    }
    
    // Calculate different types of velocity
    std::vector<double> trades_count_velocity;
    std::vector<double> volume_velocity;
    std::vector<double> price_velocity;
    
    if (params_.enable_trades_count_velocity) {
        trades_count_velocity = calculate_trades_count_velocity(candles);
    }
    
    if (params_.enable_volume_velocity) {
        volume_velocity = calculate_volume_velocity(candles);
    }
    
    if (params_.enable_price_velocity) {
        price_velocity = calculate_price_velocity(candles);
    }
    
    // Combine velocity values (simple average for now)
    size_t max_size = std::max({trades_count_velocity.size(), volume_velocity.size(), price_velocity.size()});
    result.velocity_values.reserve(max_size);
    
    for (size_t i = 0; i < max_size; ++i) {
        double combined_velocity = 0.0;
        int count = 0;
        
        if (i < trades_count_velocity.size()) {
            combined_velocity += trades_count_velocity[i];
            count++;
        }
        
        if (i < volume_velocity.size()) {
            combined_velocity += volume_velocity[i];
            count++;
        }
        
        if (i < price_velocity.size()) {
            combined_velocity += price_velocity[i];
            count++;
        }
        
        if (count > 0) {
            result.velocity_values.push_back(combined_velocity / count);
        } else {
            result.velocity_values.push_back(0.0);
        }
    }
    
    // Apply smoothing if enabled
    if (params_.smoothing_factor > 0.0) {
        result.velocity_values = apply_smoothing(result.velocity_values);
    }
    
    return result;
}

VelocityData VelocityCalculator::calculate_velocity_from_trades(
    SymbolId symbol_id, const std::vector<TradeData>& trades, const std::string& interval) {
    
    VelocityData result(interval);
    
    if (trades.empty()) {
        return result;
    }
    
    // Get interval duration
    auto it = interval_durations_.find(interval);
    if (it == interval_durations_.end()) {
        return result; // Unknown interval
    }
    
    int64_t interval_duration_ms = it->second;
    
    // Calculate velocity from trades
    result.velocity_values = calculate_velocity_from_trades_impl(trades, interval_duration_ms);
    
    // Apply smoothing if enabled
    if (params_.smoothing_factor > 0.0) {
        result.velocity_values = apply_smoothing(result.velocity_values);
    }
    
    return result;
}

std::map<std::string, VelocityData> VelocityCalculator::calculate_velocity_for_intervals(
    SymbolId symbol_id, const std::map<std::string, std::vector<CandleData>>& candles_map) {
    
    std::map<std::string, VelocityData> result;
    
    for (const auto& pair : candles_map) {
        const std::string& interval_name = pair.first;
        const std::vector<CandleData>& candles = pair.second;
        
        if (!candles.empty()) {
            result[interval_name] = calculate_velocity_from_candles(symbol_id, candles, interval_name);
        }
    }
    
    return result;
}

void VelocityCalculator::add_interval(const std::string& interval_name, int64_t duration_ms) {
    interval_durations_[interval_name] = duration_ms;
}

std::vector<std::string> VelocityCalculator::get_configured_intervals() const {
    std::vector<std::string> intervals;
    intervals.reserve(interval_durations_.size());
    
    for (const auto& pair : interval_durations_) {
        intervals.push_back(pair.first);
    }
    
    return intervals;
}

void VelocityCalculator::set_calculation_params(const pybind11::dict& params) {
    try {
        if (params.contains("enable_trades_count_velocity")) {
            params_.enable_trades_count_velocity = params["enable_trades_count_velocity"].cast<bool>();
        }
        if (params.contains("enable_volume_velocity")) {
            params_.enable_volume_velocity = params["enable_volume_velocity"].cast<bool>();
        }
        if (params.contains("enable_price_velocity")) {
            params_.enable_price_velocity = params["enable_price_velocity"].cast<bool>();
        }
        if (params.contains("smoothing_factor")) {
            params_.smoothing_factor = params["smoothing_factor"].cast<double>();
        }
        if (params.contains("min_data_points")) {
            params_.min_data_points = params["min_data_points"].cast<int32_t>();
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to set calculation parameters: " + std::string(e.what()));
    }
}

pybind11::dict VelocityCalculator::get_calculation_params() const {
    pybind11::dict params;
    params["enable_trades_count_velocity"] = params_.enable_trades_count_velocity;
    params["enable_volume_velocity"] = params_.enable_volume_velocity;
    params["enable_price_velocity"] = params_.enable_price_velocity;
    params["smoothing_factor"] = params_.smoothing_factor;
    params["min_data_points"] = params_.min_data_points;
    return params;
}

void VelocityCalculator::initialize_default_intervals() {
    for (const auto& pair : DEFAULT_INTERVALS) {
        interval_durations_[pair.first] = pair.second;
    }
}

std::vector<double> VelocityCalculator::calculate_trades_count_velocity(const std::vector<CandleData>& candles) const {
    std::vector<double> trades_counts;
    trades_counts.reserve(candles.size());
    
    for (const auto& candle : candles) {
        trades_counts.push_back(static_cast<double>(candle.trades_count));
    }
    
    return calculate_velocity_between_values(trades_counts);
}

std::vector<double> VelocityCalculator::calculate_volume_velocity(const std::vector<CandleData>& candles) const {
    std::vector<double> volumes;
    volumes.reserve(candles.size());
    
    for (const auto& candle : candles) {
        volumes.push_back(candle.volume);
    }
    
    return calculate_velocity_between_values(volumes);
}

std::vector<double> VelocityCalculator::calculate_price_velocity(const std::vector<CandleData>& candles) const {
    std::vector<double> prices;
    prices.reserve(candles.size());
    
    for (const auto& candle : candles) {
        prices.push_back(candle.close_price);
    }
    
    return calculate_velocity_between_values(prices);
}

std::vector<double> VelocityCalculator::calculate_velocity_from_trades_impl(
    const std::vector<TradeData>& trades, int64_t interval_duration_ms) const {
    
    // Group trades by intervals
    auto grouped_trades = group_trades_by_interval(trades, interval_duration_ms);
    
    if (grouped_trades.empty()) {
        return std::vector<double>();
    }
    
    // Calculate trades count per interval
    std::vector<int32_t> trades_count_per_interval;
    for (const auto& pair : grouped_trades) {
        trades_count_per_interval.push_back(static_cast<int32_t>(pair.second.size()));
    }
    
    // Convert to double and calculate velocity
    std::vector<double> trades_count_double(trades_count_per_interval.begin(), trades_count_per_interval.end());
    return calculate_velocity_between_values(trades_count_double);
}

std::vector<double> VelocityCalculator::apply_smoothing(const std::vector<double>& values) const {
    if (values.empty() || params_.smoothing_factor <= 0.0) {
        return values;
    }
    
    return calculate_ema(values, params_.smoothing_factor);
}

std::vector<double> VelocityCalculator::calculate_ema(const std::vector<double>& values, double alpha) const {
    if (values.empty()) {
        return std::vector<double>();
    }
    
    std::vector<double> ema;
    ema.reserve(values.size());
    
    // First value is the same
    ema.push_back(values[0]);
    
    // Calculate EMA for subsequent values
    for (size_t i = 1; i < values.size(); ++i) {
        double ema_value = alpha * values[i] + (1.0 - alpha) * ema[i - 1];
        ema.push_back(ema_value);
    }
    
    return ema;
}

std::vector<double> VelocityCalculator::calculate_sma(const std::vector<double>& values, int32_t window_size) const {
    if (values.empty() || window_size <= 0) {
        return std::vector<double>();
    }
    
    std::vector<double> sma;
    sma.reserve(values.size());
    
    for (size_t i = 0; i < values.size(); ++i) {
        int32_t start_idx = std::max(0, static_cast<int32_t>(i) - window_size + 1);
        int32_t end_idx = static_cast<int32_t>(i) + 1;
        
        double sum = 0.0;
        int32_t count = 0;
        
        for (int32_t j = start_idx; j < end_idx; ++j) {
            sum += values[j];
            count++;
        }
        
        sma.push_back(count > 0 ? sum / count : 0.0);
    }
    
    return sma;
}

std::vector<double> VelocityCalculator::calculate_velocity_between_values(const std::vector<double>& values) const {
    if (values.size() < 2) {
        return std::vector<double>();
    }
    
    std::vector<double> velocity;
    velocity.reserve(values.size() - 1);
    
    for (size_t i = 1; i < values.size(); ++i) {
        double vel = values[i] - values[i - 1];
        velocity.push_back(vel);
    }
    
    return velocity;
}

std::vector<int32_t> VelocityCalculator::calculate_trades_count_per_interval(
    const std::vector<TradeData>& trades, int64_t interval_duration_ms) const {
    
    auto grouped_trades = group_trades_by_interval(trades, interval_duration_ms);
    
    std::vector<int32_t> trades_count;
    trades_count.reserve(grouped_trades.size());
    
    for (const auto& pair : grouped_trades) {
        trades_count.push_back(static_cast<int32_t>(pair.second.size()));
    }
    
    return trades_count;
}

std::vector<double> VelocityCalculator::calculate_volume_per_interval(
    const std::vector<TradeData>& trades, int64_t interval_duration_ms) const {
    
    auto grouped_trades = group_trades_by_interval(trades, interval_duration_ms);
    
    std::vector<double> volume_per_interval;
    volume_per_interval.reserve(grouped_trades.size());
    
    for (const auto& pair : grouped_trades) {
        double total_volume = 0.0;
        for (const auto& trade : pair.second) {
            total_volume += trade.price * trade.quantity;
        }
        volume_per_interval.push_back(total_volume);
    }
    
    return volume_per_interval;
}

std::vector<double> VelocityCalculator::calculate_price_change_per_interval(
    const std::vector<TradeData>& trades, int64_t interval_duration_ms) const {
    
    auto grouped_trades = group_trades_by_interval(trades, interval_duration_ms);
    
    std::vector<double> price_change_per_interval;
    price_change_per_interval.reserve(grouped_trades.size());
    
    for (const auto& pair : grouped_trades) {
        if (pair.second.empty()) {
            price_change_per_interval.push_back(0.0);
            continue;
        }
        
        // Sort trades by trade_id to get chronological order
        std::vector<TradeData> sorted_trades = pair.second;
        std::sort(sorted_trades.begin(), sorted_trades.end(), 
                 [](const TradeData& a, const TradeData& b) { return a.trade_id < b.trade_id; });
        
        double first_price = sorted_trades.front().price;
        double last_price = sorted_trades.back().price;
        
        price_change_per_interval.push_back(last_price - first_price);
    }
    
    return price_change_per_interval;
}

std::map<int64_t, std::vector<TradeData>> VelocityCalculator::group_trades_by_interval(
    const std::vector<TradeData>& trades, int64_t interval_duration_ms) const {
    
    std::map<int64_t, std::vector<TradeData>> grouped_trades;
    
    for (const auto& trade : trades) {
        // Convert datetime to milliseconds
        auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            trade.datetime.time_since_epoch()).count();
        
        // Calculate interval start timestamp
        int64_t interval_start = calculate_interval_start_timestamp(timestamp_ms, interval_duration_ms);
        
        grouped_trades[interval_start].push_back(trade);
    }
    
    return grouped_trades;
}

int64_t VelocityCalculator::calculate_interval_start_timestamp(int64_t timestamp_ms, int64_t interval_duration_ms) const {
    return timestamp_ms - (timestamp_ms % interval_duration_ms);
}

} // namespace okx_data_processor
