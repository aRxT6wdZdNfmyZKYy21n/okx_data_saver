#include "candles_processor.h"
#include <algorithm>
#include <chrono>
#include <stdexcept>

namespace okx_data_processor {

// Default intervals in milliseconds
const std::map<std::string, int64_t> CandlesProcessor::DEFAULT_INTERVALS = {
    {"1m", 60 * 1000},           // 1 minute
    {"5m", 5 * 60 * 1000},       // 5 minutes
    {"15m", 15 * 60 * 1000},     // 15 minutes
    {"1h", 60 * 60 * 1000},      // 1 hour
    {"4h", 4 * 60 * 60 * 1000},  // 4 hours
    {"1d", 24 * 60 * 60 * 1000}  // 1 day
};

CandlesProcessor::CandlesProcessor() {
    initialize_default_intervals();
}

std::map<std::string, std::vector<CandleData>> CandlesProcessor::process_trades(
    SymbolId symbol_id, const std::vector<TradeData>& trades) {
    
    std::map<std::string, std::vector<CandleData>> result;
    
    for (const auto& interval_pair : interval_durations_) {
        const std::string& interval_name = interval_pair.first;
        int64_t duration_ms = interval_pair.second;
        
        int64_t min_trade_id = get_min_trade_id(symbol_id, interval_name);
        auto candles = process_trades_for_interval_impl(trades, duration_ms, min_trade_id);
        
        if (!candles.empty()) {
            result[interval_name] = std::move(candles);
        }
    }
    
    return result;
}

std::vector<CandleData> CandlesProcessor::process_trades_for_interval(
    SymbolId symbol_id, const std::vector<TradeData>& trades, const std::string& interval_name) {
    
    auto it = interval_durations_.find(interval_name);
    if (it == interval_durations_.end()) {
        throw std::invalid_argument("Unknown interval: " + interval_name);
    }
    
    int64_t duration_ms = it->second;
    int64_t min_trade_id = get_min_trade_id(symbol_id, interval_name);
    
    return process_trades_for_interval_impl(trades, duration_ms, min_trade_id);
}

void CandlesProcessor::add_interval(const std::string& interval_name, int64_t duration_ms) {
    interval_durations_[interval_name] = duration_ms;
}

std::vector<std::string> CandlesProcessor::get_configured_intervals() const {
    std::vector<std::string> intervals;
    intervals.reserve(interval_durations_.size());
    
    for (const auto& pair : interval_durations_) {
        intervals.push_back(pair.first);
    }
    
    return intervals;
}

void CandlesProcessor::set_min_trade_id(SymbolId symbol_id, const std::string& interval_name, int64_t min_trade_id) {
    min_trade_ids_[std::make_pair(symbol_id, interval_name)] = min_trade_id;
}

int64_t CandlesProcessor::get_min_trade_id(SymbolId symbol_id, const std::string& interval_name) const {
    auto it = min_trade_ids_.find(std::make_pair(symbol_id, interval_name));
    return (it != min_trade_ids_.end()) ? it->second : 0;
}

void CandlesProcessor::initialize_default_intervals() {
    for (const auto& pair : DEFAULT_INTERVALS) {
        interval_durations_[pair.first] = pair.second;
    }
}

int64_t CandlesProcessor::calculate_candle_start_timestamp(int64_t timestamp_ms, int64_t interval_duration_ms) const {
    return timestamp_ms - (timestamp_ms % interval_duration_ms);
}

std::vector<CandleData> CandlesProcessor::process_trades_for_interval_impl(
    const std::vector<TradeData>& trades, int64_t interval_duration_ms, int64_t min_trade_id) const {
    
    std::vector<CandleData> candles;
    std::vector<TradeData> filtered_trades = filter_trades_by_min_id(trades, min_trade_id);
    
    if (filtered_trades.empty()) {
        return candles;
    }
    
    std::map<int64_t, CandleData> candle_map;
    
    for (const auto& trade : filtered_trades) {
        // Convert datetime to milliseconds
        auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            trade.datetime.time_since_epoch()).count();
        
        int64_t candle_start_timestamp = calculate_candle_start_timestamp(timestamp_ms, interval_duration_ms);
        int64_t candle_end_timestamp = candle_start_timestamp + interval_duration_ms;
        
        auto it = candle_map.find(candle_start_timestamp);
        if (it != candle_map.end()) {
            // Update existing candle
            update_candle_with_trade(it->second, trade);
        } else {
            // Create new candle
            CandleData new_candle = create_candle_from_trade(trade, interval_duration_ms);
            candle_map[candle_start_timestamp] = new_candle;
        }
    }
    
    // Convert map to vector and sort
    candles.reserve(candle_map.size());
    for (auto& pair : candle_map) {
        candles.push_back(std::move(pair.second));
    }
    
    sort_candles_by_start_trade_id(candles);
    
    return candles;
}

CandleData CandlesProcessor::create_candle_from_trade(const TradeData& trade, int64_t interval_duration_ms) const {
    auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        trade.datetime.time_since_epoch()).count();
    
    int64_t candle_start_timestamp = calculate_candle_start_timestamp(timestamp_ms, interval_duration_ms);
    int64_t candle_end_timestamp = candle_start_timestamp + interval_duration_ms;
    double volume = trade.price * trade.quantity;
    
    return CandleData(
        trade.price,  // open_price
        trade.price,  // high_price
        trade.price,  // low_price
        trade.price,  // close_price
        volume,       // volume
        trade.trade_id,  // start_trade_id
        trade.trade_id,  // end_trade_id
        candle_start_timestamp,  // start_timestamp_ms
        candle_end_timestamp,    // end_timestamp_ms
        1  // trades_count
    );
}

void CandlesProcessor::update_candle_with_trade(CandleData& candle, const TradeData& trade) const {
    // Update high price
    if (trade.price > candle.high_price) {
        candle.high_price = trade.price;
    }
    
    // Update low price
    if (trade.price < candle.low_price) {
        candle.low_price = trade.price;
    }
    
    // Update close price
    candle.close_price = trade.price;
    
    // Update volume
    candle.volume += trade.price * trade.quantity;
    
    // Update end trade ID
    candle.end_trade_id = trade.trade_id;
    
    // Update trades count
    candle.trades_count++;
}

std::vector<TradeData> CandlesProcessor::filter_trades_by_min_id(
    const std::vector<TradeData>& trades, int64_t min_trade_id) const {
    
    std::vector<TradeData> filtered_trades;
    filtered_trades.reserve(trades.size());
    
    for (const auto& trade : trades) {
        if (trade.trade_id >= min_trade_id) {
            filtered_trades.push_back(trade);
        }
    }
    
    return filtered_trades;
}

void CandlesProcessor::sort_candles_by_start_trade_id(std::vector<CandleData>& candles) const {
    std::sort(candles.begin(), candles.end(), 
              [](const CandleData& a, const CandleData& b) {
                  return a.start_trade_id < b.start_trade_id;
              });
}

} // namespace okx_data_processor
