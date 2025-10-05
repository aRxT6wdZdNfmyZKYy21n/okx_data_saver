#include "smoothing_processor.h"
#include <algorithm>
#include <stdexcept>

namespace okx_data_processor {

// Default smoothing levels
const std::map<std::string, int32_t> SmoothingProcessor::DEFAULT_SMOOTHING_LEVELS = {
    {"Raw (0)", 0},
    {"Smoothed (1)", 1}
};

SmoothingProcessor::SmoothingProcessor() {
    initialize_default_levels();
}

std::map<std::string, std::vector<SmoothedLine>> SmoothingProcessor::process_smoothed_data(
    SymbolId symbol_id, const std::vector<TradeData>& trades) {
    
    std::map<std::string, std::vector<SmoothedLine>> result;
    
    for (const auto& level_pair : smoothing_levels_) {
        const std::string& level_name = level_pair.first;
        int32_t level_number = level_pair.second;
        
        // Skip raw level (0)
        if (level_number == 0) {
            continue;
        }
        
        int64_t min_trade_id = get_min_trade_id(symbol_id, level_name);
        auto filtered_trades = filter_trades_by_min_id(trades, min_trade_id);
        
        if (filtered_trades.empty()) {
            continue;
        }
        
        std::vector<SmoothedLine> lines;
        
        if (level_number == 1) {
            lines = process_level_1_smoothing(filtered_trades);
        } else {
            // For now, only level 1 is implemented
            // Higher levels would be implemented here
            continue;
        }
        
        if (!lines.empty()) {
            result[level_name] = std::move(lines);
        }
    }
    
    return result;
}

std::map<std::string, std::vector<SmoothedDataPoint>> SmoothingProcessor::process_smoothed_data_points(
    SymbolId symbol_id, const std::vector<TradeData>& trades) {
    
    std::map<std::string, std::vector<SmoothedDataPoint>> result;
    
    for (const auto& level_pair : smoothing_levels_) {
        const std::string& level_name = level_pair.first;
        int32_t level_number = level_pair.second;
        
        // Skip raw level (0)
        if (level_number == 0) {
            continue;
        }
        
        int64_t min_trade_id = get_min_trade_id(symbol_id, level_name);
        auto filtered_trades = filter_trades_by_min_id(trades, min_trade_id);
        
        if (filtered_trades.empty()) {
            continue;
        }
        
        std::vector<SmoothedDataPoint> data_points;
        
        if (level_number == 1) {
            // First calculate lines
            auto lines = process_level_1_smoothing(filtered_trades);
            // Then convert to data points
            data_points = calculate_smoothed_data_points_from_lines(lines);
        } else {
            // For now, only level 1 is implemented
            // Higher levels would be implemented here
            continue;
        }
        
        if (!data_points.empty()) {
            result[level_name] = std::move(data_points);
        }
    }
    
    return result;
}

std::vector<SmoothedDataPoint> SmoothingProcessor::process_level_data_points(
    SymbolId symbol_id, const std::vector<TradeData>& trades, const std::string& level) {
    
    auto it = smoothing_levels_.find(level);
    if (it == smoothing_levels_.end()) {
        throw std::invalid_argument("Unknown smoothing level: " + level);
    }
    
    int32_t level_number = it->second;
    int64_t min_trade_id = get_min_trade_id(symbol_id, level);
    auto filtered_trades = filter_trades_by_min_id(trades, min_trade_id);
    
    if (filtered_trades.empty()) {
        return std::vector<SmoothedDataPoint>();
    }
    
    if (level_number == 1) {
        // First calculate lines
        auto lines = process_level_1_smoothing(filtered_trades);
        // Then convert to data points
        return calculate_smoothed_data_points_from_lines(lines);
    } else {
        // For now, only level 1 is implemented
        return std::vector<SmoothedDataPoint>();
    }
}

std::vector<SmoothedLine> SmoothingProcessor::process_level_data(
    SymbolId symbol_id, const std::vector<TradeData>& trades, const std::string& level) {
    
    auto it = smoothing_levels_.find(level);
    if (it == smoothing_levels_.end()) {
        throw std::invalid_argument("Unknown smoothing level: " + level);
    }
    
    int32_t level_number = it->second;
    int64_t min_trade_id = get_min_trade_id(symbol_id, level);
    auto filtered_trades = filter_trades_by_min_id(trades, min_trade_id);
    
    if (filtered_trades.empty()) {
        return std::vector<SmoothedLine>();
    }
    
    if (level_number == 1) {
        return process_level_1_smoothing(filtered_trades);
    } else {
        // For now, only level 1 is implemented
        return std::vector<SmoothedLine>();
    }
}

void SmoothingProcessor::add_smoothing_level(const std::string& level_name, int32_t level_number) {
    smoothing_levels_[level_name] = level_number;
}

std::vector<std::string> SmoothingProcessor::get_configured_levels() const {
    std::vector<std::string> levels;
    levels.reserve(smoothing_levels_.size());
    
    for (const auto& pair : smoothing_levels_) {
        levels.push_back(pair.first);
    }
    
    return levels;
}

void SmoothingProcessor::set_min_trade_id(SymbolId symbol_id, const std::string& level_name, int64_t min_trade_id) {
    min_trade_ids_[std::make_pair(symbol_id, level_name)] = min_trade_id;
}

int64_t SmoothingProcessor::get_min_trade_id(SymbolId symbol_id, const std::string& level_name) const {
    auto it = min_trade_ids_.find(std::make_pair(symbol_id, level_name));
    return (it != min_trade_ids_.end()) ? it->second : 0;
}

void SmoothingProcessor::initialize_default_levels() {
    for (const auto& pair : DEFAULT_SMOOTHING_LEVELS) {
        smoothing_levels_[pair.first] = pair.second;
    }
}

std::vector<SmoothedLine> SmoothingProcessor::process_level_1_smoothing(const std::vector<TradeData>& trades) const {
    // Calculate lines from trades
    std::vector<SmoothedLine> lines = calculate_level_1_lines(trades);
    
    // Sort lines by start trade ID
    sort_lines_by_start_trade_id(lines);
    
    return lines;
}

std::vector<SmoothedLine> SmoothingProcessor::calculate_level_1_lines(const std::vector<TradeData>& trades) const {
    std::vector<SmoothedLine> lines;
    SmoothedLine* current_line = nullptr;
    
    for (const auto& trade : trades) {
        if (current_line == nullptr) {
            // Start a new line
            lines.emplace_back(create_line_from_trade(trade));
            current_line = &lines.back();
        } else if (should_continue_line(*current_line, trade)) {
            // Continue current line
            update_line_with_trade(*current_line, trade);
        } else {
            // Start a new line
            lines.emplace_back(create_line_from_trade(trade));
            current_line = &lines.back();
        }
    }
    
    return lines;
}

std::vector<SmoothedLine> SmoothingProcessor::calculate_smoothed_from_lines(const std::vector<SmoothedLine>& lines) const {
    std::vector<SmoothedLine> smoothed_data;
    smoothed_data.reserve(lines.size() * 2); // Each line contributes start and end points
    
    for (const auto& line : lines) {
        // Add start point
        smoothed_data.emplace_back(
            line.is_buy,
            line.start_price,
            line.start_price, // Same as start price for start point
            line.quantity,
            line.volume,
            line.start_trade_id,
            line.start_trade_id, // Same as start trade ID for start point
            line.start_datetime,
            line.start_datetime  // Same as start datetime for start point
        );
        
        // Add end point
        smoothed_data.emplace_back(
            line.is_buy,
            line.end_price,
            line.end_price, // Same as end price for end point
            line.quantity,
            line.volume,
            line.end_trade_id,
            line.end_trade_id, // Same as end trade ID for end point
            line.end_datetime,
            line.end_datetime  // Same as end datetime for end point
        );
    }
    
    return smoothed_data;
}

std::vector<SmoothedDataPoint> SmoothingProcessor::calculate_smoothed_data_points_from_lines(const std::vector<SmoothedLine>& lines) const {
    std::vector<SmoothedDataPoint> smoothed_data_points;
    smoothed_data_points.reserve(lines.size() * 2); // Each line contributes start and end points
    
    for (const auto& line : lines) {
        // Add start point
        smoothed_data_points.emplace_back(
            line.start_trade_id,
            line.start_price,
            line.start_datetime
        );
        
        // Add end point
        smoothed_data_points.emplace_back(
            line.end_trade_id,
            line.end_price,
            line.end_datetime
        );
    }
    
    return smoothed_data_points;
}

std::vector<TradeData> SmoothingProcessor::filter_trades_by_min_id(
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

void SmoothingProcessor::sort_lines_by_start_trade_id(std::vector<SmoothedLine>& lines) const {
    std::sort(lines.begin(), lines.end(), 
              [](const SmoothedLine& a, const SmoothedLine& b) {
                  return a.start_trade_id < b.start_trade_id;
              });
}

SmoothedLine SmoothingProcessor::create_line_from_trade(const TradeData& trade) const {
    double volume = trade.price * trade.quantity;
    
    return SmoothedLine(
        trade.is_buy,
        trade.price,  // start_price
        trade.price,  // end_price (same as start for single trade)
        trade.quantity,
        volume,
        trade.trade_id,  // start_trade_id
        trade.trade_id,  // end_trade_id (same as start for single trade)
        trade.datetime,  // start_datetime
        trade.datetime   // end_datetime (same as start for single trade)
    );
}

void SmoothingProcessor::update_line_with_trade(SmoothedLine& line, const TradeData& trade) const {
    // Update end price
    line.end_price = trade.price;
    
    // Update end trade ID
    line.end_trade_id = trade.trade_id;
    
    // Update end datetime
    line.end_datetime = trade.datetime;
    
    // Update quantity and volume
    line.quantity += trade.quantity;
    line.volume += trade.price * trade.quantity;
}

bool SmoothingProcessor::should_continue_line(const SmoothedLine& line, const TradeData& trade) const {
    // Continue the line if the trade has the same buy/sell direction
    return line.is_buy == trade.is_buy;
}

} // namespace okx_data_processor
