#include "extreme_lines_processor.h"
#include <algorithm>
#include <cmath>
#include <limits>

namespace okx_data_processor {

ExtremeLinesProcessor::ExtremeLinesProcessor() {
}

std::vector<ExtremeLine> ExtremeLinesProcessor::process_extreme_lines(
    SymbolId /* symbol_id */,
    const std::vector<SmoothedLine>& smoothed_lines,
    const std::vector<TradeData>& trades) {
    
    if (smoothed_lines.empty()) {
        return std::vector<ExtremeLine>();
    }
    
    // Get trade ID ranges for completion
    int64_t max_trade_id = 0;
    if (!trades.empty()) {
        max_trade_id = trades.back().trade_id;
    }
    
    // Process extreme lines using Python-like algorithm
    return process_extreme_lines_python_style(smoothed_lines, max_trade_id);
}

std::vector<std::vector<double>> ExtremeLinesProcessor::create_extreme_lines_array(
    const std::vector<ExtremeLine>& extreme_lines,
    const std::vector<TradeData>& trades,
    int32_t width, int32_t height) {
    
    // Calculate scale factor
    double scale = calculate_scale_factor(trades, width, height);
    
    // Get ranges
    auto [price_range, trade_id_range] = get_ranges(trades);
    double min_price = price_range.first;
    int64_t min_trade_id = trade_id_range.first;
    
    // Create 2D array
    std::vector<std::vector<double>> array(width, std::vector<double>(height, 0.0));
    
    // Fill array
    fill_extreme_lines_array(extreme_lines, array, width, height, scale, min_trade_id, min_price);
    
    return array;
}

std::pair<int32_t, int32_t> ExtremeLinesProcessor::calculate_array_dimensions(
    const std::vector<TradeData>& trades, int32_t height) const {
    
    if (trades.empty()) {
        return std::make_pair(0, 0);
    }
    
    // Get ranges
    auto [price_range, trade_id_range] = get_ranges(trades);
    double min_price = price_range.first;
    double max_price = price_range.second;
    int64_t min_trade_id = trade_id_range.first;
    int64_t max_trade_id = trade_id_range.second;
    
    double delta_price = max_price - min_price;
    int64_t delta_trade_id = max_trade_id - min_trade_id;
    
    if (delta_price <= 0 || delta_trade_id <= 0) {
        return std::make_pair(0, 0);
    }
    
    double aspect_ratio = static_cast<double>(delta_trade_id) / delta_price;
    int32_t width = static_cast<int32_t>(height * aspect_ratio);
    
    return std::make_pair(width, height);
}

double ExtremeLinesProcessor::calculate_scale_factor(
    const std::vector<TradeData>& trades, int32_t /* width */, int32_t height) const {
    
    if (trades.empty()) {
        return 1.0;
    }
    
    // Get ranges
    auto [price_range, trade_id_range] = get_ranges(trades);
    double min_price = price_range.first;
    double max_price = price_range.second;
    int64_t min_trade_id = trade_id_range.first;
    int64_t max_trade_id = trade_id_range.second;
    
    double delta_price = max_price - min_price;
    int64_t delta_trade_id = max_trade_id - min_trade_id;
    
    if (delta_price <= 0 || delta_trade_id <= 0) {
        return 1.0;
    }
    
    // Python logic: extreme_lines_scale = delta_price / height
    double scale = delta_price / height;
    
    return scale;
}

void ExtremeLinesProcessor::set_processing_params(const pybind11::dict& params) {
    try {
        if (params.contains("default_height")) {
            params_.default_height = params["default_height"].cast<int32_t>();
        }
        if (params.contains("min_price_delta")) {
            params_.min_price_delta = params["min_price_delta"].cast<double>();
        }
        if (params.contains("min_trade_id_delta")) {
            params_.min_trade_id_delta = params["min_trade_id_delta"].cast<double>();
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to set processing parameters: " + std::string(e.what()));
    }
}

pybind11::dict ExtremeLinesProcessor::get_processing_params() const {
    pybind11::dict params;
    params["default_height"] = params_.default_height;
    params["min_price_delta"] = params_.min_price_delta;
    params["min_trade_id_delta"] = params_.min_trade_id_delta;
    return params;
}

std::vector<double> ExtremeLinesProcessor::find_extreme_prices(const std::vector<SmoothedLine>& smoothed_lines) const {
    std::vector<double> extreme_prices;
    extreme_prices.reserve(smoothed_lines.size() * 2); // Start and end prices
    
    for (const auto& line : smoothed_lines) {
        extreme_prices.push_back(line.start_price);
        extreme_prices.push_back(line.end_price);
    }
    
    // Remove duplicates and sort
    std::sort(extreme_prices.begin(), extreme_prices.end());
    extreme_prices.erase(std::unique(extreme_prices.begin(), extreme_prices.end()), extreme_prices.end());
    
    return extreme_prices;
}

std::vector<ExtremeLine> ExtremeLinesProcessor::process_extreme_lines_python_style(
    const std::vector<SmoothedLine>& smoothed_lines,
    int64_t max_trade_id) const {
    
    std::vector<ExtremeLine> extreme_lines;
    
    // Map to track active extreme lines by price (like Python version)
    std::map<double, std::pair<int64_t, int64_t>> active_extreme_line_raw_data_by_price_map;
    
    for (const auto& line : smoothed_lines) {
        int64_t end_trade_id = line.end_trade_id;
        int64_t start_trade_id = line.start_trade_id;
        double end_price = line.end_price;
        double start_price = line.start_price;
        
        double left_price = std::min(end_price, start_price);
        double right_price = std::max(end_price, start_price);
        
        std::vector<double> active_extreme_line_prices_to_delete;
        
        // Check for intersections with active lines (Python logic)
        for (auto it = active_extreme_line_raw_data_by_price_map.begin(); 
             it != active_extreme_line_raw_data_by_price_map.end(); ++it) {
            
            double price = it->first;
            if (!(left_price <= price && price <= right_price)) {
                continue;
            }
            
            active_extreme_line_prices_to_delete.push_back(price);
            
            // Add to completed extreme lines
            extreme_lines.emplace_back(price, it->second.first, start_trade_id);
        }
        
        // Remove completed extreme lines
        for (double price : active_extreme_line_prices_to_delete) {
            active_extreme_line_raw_data_by_price_map.erase(price);
        }
        
        // Check for duplicate prices (Python assertions)
        if (active_extreme_line_raw_data_by_price_map.find(end_price) != active_extreme_line_raw_data_by_price_map.end()) {
            // Handle duplicate end_price
            continue;
        }
        
        if (active_extreme_line_raw_data_by_price_map.find(start_price) != active_extreme_line_raw_data_by_price_map.end()) {
            // Handle duplicate start_price
            continue;
        }
        
        // Add new active lines (Python logic)
        active_extreme_line_raw_data_by_price_map[end_price] = std::make_pair(end_trade_id, 0);
        active_extreme_line_raw_data_by_price_map[start_price] = std::make_pair(start_trade_id, 0);
    }
    
    // Complete remaining active lines
    for (const auto& [price, trade_data] : active_extreme_line_raw_data_by_price_map) {
        extreme_lines.emplace_back(price, trade_data.first, max_trade_id);
    }
    
    return extreme_lines;
}

bool ExtremeLinesProcessor::is_price_in_range(double price, double start_price, double end_price) const {
    double left_price = std::min(start_price, end_price);
    double right_price = std::max(start_price, end_price);
    return price >= left_price && price <= right_price;
}

std::vector<int64_t> ExtremeLinesProcessor::find_intersections(
    double extreme_price, const std::vector<SmoothedLine>& smoothed_lines) const {
    
    std::vector<int64_t> intersections;
    
    for (const auto& line : smoothed_lines) {
        if (is_price_in_range(extreme_price, line.start_price, line.end_price)) {
            // Find the trade ID where the intersection occurs
            // This is a simplified implementation
            intersections.push_back(line.start_trade_id);
        }
    }
    
    return intersections;
}

void ExtremeLinesProcessor::fill_extreme_lines_array(
    const std::vector<ExtremeLine>& extreme_lines,
    std::vector<std::vector<double>>& array,
    int32_t width, int32_t height, double scale,
    int64_t min_trade_id, double min_price) const {
    
    for (const auto& extreme_line : extreme_lines) {
        int64_t start_trade_id = extreme_line.start_trade_id;
        int64_t end_trade_id = extreme_line.end_trade_id;
        double price = extreme_line.price;
        
        // Calculate array coordinates (Python logic)
        int32_t end_x = static_cast<int32_t>((end_trade_id - min_trade_id) / scale);
        int32_t start_x = static_cast<int32_t>((start_trade_id - min_trade_id) / scale);
        int32_t y = std::min(static_cast<int32_t>((price - min_price) / scale), height - 1);
        
        // Clamp coordinates to array bounds
        start_x = std::max(0, std::min(start_x, width - 1));
        end_x = std::max(0, std::min(end_x, width - 1));
        y = std::max(0, std::min(y, height - 1));
        
        // Fill array with values from 0 to line length (Python logic: numpy.arange(end_x - start_x))
        for (int32_t x = start_x; x < end_x; ++x) {
            array[x][y] = static_cast<double>(x - start_x);
        }
    }
}

std::pair<std::pair<double, double>, std::pair<int64_t, int64_t>> ExtremeLinesProcessor::get_ranges(
    const std::vector<TradeData>& trades) const {
    
    if (trades.empty()) {
        return std::make_pair(
            std::make_pair(0.0, 0.0),
            std::make_pair(0, 0)
        );
    }
    
    double min_price = std::numeric_limits<double>::max();
    double max_price = std::numeric_limits<double>::lowest();
    int64_t min_trade_id = std::numeric_limits<int64_t>::max();
    int64_t max_trade_id = std::numeric_limits<int64_t>::lowest();
    
    for (const auto& trade : trades) {
        min_price = std::min(min_price, trade.price);
        max_price = std::max(max_price, trade.price);
        min_trade_id = std::min(min_trade_id, trade.trade_id);
        max_trade_id = std::max(max_trade_id, trade.trade_id);
    }
    
    return std::make_pair(
        std::make_pair(min_price, max_price),
        std::make_pair(min_trade_id, max_trade_id)
    );
}

} // namespace okx_data_processor
