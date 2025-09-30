#include "order_book_processor.h"
#include <algorithm>
#include <cmath>
#include <limits>

namespace okx_data_processor {

OrderBookProcessor::OrderBookProcessor() {
}

OrderBookVolumes OrderBookProcessor::process_order_book_volumes(
    SymbolId /* symbol_id */, const std::vector<TradeData>& trades) {
    
    if (trades.empty()) {
        return OrderBookVolumes(0, 0, 0.0, 0, 0.0);
    }
    
    // Calculate array dimensions
    auto [width, height] = calculate_array_dimensions(trades, params_.default_height);
    
    if (width <= 0 || height <= 0) {
        return OrderBookVolumes(0, 0, 0.0, 0, 0.0);
    }
    
    // Calculate scale factor
    double scale = calculate_scale_factor(trades, width, height);
    
    // Get ranges
    auto [price_range, trade_id_range] = get_ranges(trades);
    double min_price = price_range.first;
    int64_t min_trade_id = trade_id_range.first;
    
    // Process volumes from trades
    return process_volumes_from_trades(trades, width, height, scale, min_trade_id, min_price);
}

OrderBookVolumes OrderBookProcessor::process_order_book_volumes_with_snapshots(
    SymbolId /* symbol_id */, 
    const std::vector<TradeData>& trades,
    const std::vector<OrderBookSnapshot>& order_book_data) {
    
    if (trades.empty() && order_book_data.empty()) {
        return OrderBookVolumes(0, 0, 0.0, 0, 0.0);
    }
    
    // Use trades for dimension calculation if available, otherwise use order book data
    const auto& dimension_source = trades.empty() ? 
        std::vector<TradeData>() : trades;
    
    // Calculate array dimensions
    auto [width, height] = calculate_array_dimensions(dimension_source, params_.default_height);
    
    if (width <= 0 || height <= 0) {
        return OrderBookVolumes(0, 0, 0.0, 0, 0.0);
    }
    
    // Calculate scale factor
    double scale = calculate_scale_factor(dimension_source, width, height);
    
    // Get ranges
    auto [price_range, trade_id_range] = get_ranges(dimension_source);
    double min_price = price_range.first;
    int64_t min_trade_id = trade_id_range.first;
    
    // Process volumes with snapshots
    return process_volumes_with_snapshots(trades, order_book_data, width, height, scale, min_trade_id, min_price);
}

std::pair<int32_t, int32_t> OrderBookProcessor::calculate_array_dimensions(
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

double OrderBookProcessor::calculate_scale_factor(
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
    
    // double aspect_ratio = static_cast<double>(delta_trade_id) / delta_price;
    double scale = delta_price / height;
    
    return scale;
}

void OrderBookProcessor::set_processing_params(const pybind11::dict& params) {
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
        if (params.contains("enable_asks_processing")) {
            params_.enable_asks_processing = params["enable_asks_processing"].cast<bool>();
        }
        if (params.contains("enable_bids_processing")) {
            params_.enable_bids_processing = params["enable_bids_processing"].cast<bool>();
        }
        if (params.contains("volume_aggregation_factor")) {
            params_.volume_aggregation_factor = params["volume_aggregation_factor"].cast<double>();
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to set processing parameters: " + std::string(e.what()));
    }
}

pybind11::dict OrderBookProcessor::get_processing_params() const {
    pybind11::dict params;
    params["default_height"] = params_.default_height;
    params["min_price_delta"] = params_.min_price_delta;
    params["min_trade_id_delta"] = params_.min_trade_id_delta;
    params["enable_asks_processing"] = params_.enable_asks_processing;
    params["enable_bids_processing"] = params_.enable_bids_processing;
    params["volume_aggregation_factor"] = params_.volume_aggregation_factor;
    return params;
}

OrderBookVolumes OrderBookProcessor::process_volumes_from_trades(
    const std::vector<TradeData>& trades,
    int32_t width, int32_t height, double scale,
    int64_t min_trade_id, double min_price) const {
    
    OrderBookVolumes volumes(width, height, scale, min_trade_id, min_price);
    
    // Fill volumes from trades
    fill_volumes_from_trades(trades, volumes.asks_array, volumes.bids_array, 
                            width, height, scale, min_trade_id, min_price);
    
    return volumes;
}

OrderBookVolumes OrderBookProcessor::process_volumes_with_snapshots(
    const std::vector<TradeData>& trades,
    const std::vector<OrderBookSnapshot>& order_book_data,
    int32_t width, int32_t height, double scale,
    int64_t min_trade_id, double min_price) const {
    
    OrderBookVolumes volumes(width, height, scale, min_trade_id, min_price);
    
    // Fill volumes from trades
    if (!trades.empty()) {
        fill_volumes_from_trades(trades, volumes.asks_array, volumes.bids_array, 
                                width, height, scale, min_trade_id, min_price);
    }
    
    // Fill volumes from order book snapshots
    if (!order_book_data.empty()) {
        fill_volumes_from_snapshots(order_book_data, volumes.asks_array, volumes.bids_array, 
                                   width, height, scale, min_trade_id, min_price);
    }
    
    return volumes;
}

void OrderBookProcessor::fill_volumes_from_trades(
    const std::vector<TradeData>& trades,
    std::vector<std::vector<double>>& asks_array,
    std::vector<std::vector<double>>& bids_array,
    int32_t width, int32_t height, double scale,
    int64_t min_trade_id, double min_price) const {
    
    for (const auto& trade : trades) {
        // Calculate coordinates
        int32_t x = static_cast<int32_t>((trade.trade_id - min_trade_id) / scale);
        int32_t y = static_cast<int32_t>((trade.price - min_price) / scale);
        
        // Clamp coordinates
        auto [clamped_x, clamped_y] = clamp_coordinates(x, y, width, height);
        
        // Calculate volume contribution
        double volume_contribution = calculate_volume_contribution(trade, scale);
        
        // Update appropriate array based on trade direction
        if (trade.is_buy && params_.enable_bids_processing) {
            update_volume_at_coordinates(bids_array, clamped_x, clamped_y, 
                                       volume_contribution, width, height);
        } else if (!trade.is_buy && params_.enable_asks_processing) {
            update_volume_at_coordinates(asks_array, clamped_x, clamped_y, 
                                       volume_contribution, width, height);
        }
    }
}

void OrderBookProcessor::fill_volumes_from_snapshots(
    const std::vector<OrderBookSnapshot>& order_book_data,
    std::vector<std::vector<double>>& asks_array,
    std::vector<std::vector<double>>& bids_array,
    int32_t width, int32_t height, double scale,
    int64_t min_trade_id, double min_price) const {
    
    for (const auto& snapshot : order_book_data) {
        // Calculate x coordinate from timestamp
        int32_t x = static_cast<int32_t>((snapshot.timestamp_ms - min_trade_id) / scale);
        
        // Process asks
        if (params_.enable_asks_processing) {
            for (const auto& [price, volume] : snapshot.asks) {
                int32_t y = static_cast<int32_t>((price - min_price) / scale);
                auto [clamped_x, clamped_y] = clamp_coordinates(x, y, width, height);
                update_volume_at_coordinates(asks_array, clamped_x, clamped_y, 
                                           volume * params_.volume_aggregation_factor, width, height);
            }
        }
        
        // Process bids
        if (params_.enable_bids_processing) {
            for (const auto& [price, volume] : snapshot.bids) {
                int32_t y = static_cast<int32_t>((price - min_price) / scale);
                auto [clamped_x, clamped_y] = clamp_coordinates(x, y, width, height);
                update_volume_at_coordinates(bids_array, clamped_x, clamped_y, 
                                           volume * params_.volume_aggregation_factor, width, height);
            }
        }
    }
}

std::pair<std::pair<double, double>, std::pair<int64_t, int64_t>> OrderBookProcessor::get_ranges(
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

double OrderBookProcessor::calculate_volume_contribution(const TradeData& trade, double /* scale */) const {
    double volume = trade.price * trade.quantity;
    return volume * params_.volume_aggregation_factor;
}

void OrderBookProcessor::update_volume_at_coordinates(
    std::vector<std::vector<double>>& array,
    int32_t x, int32_t y, double volume,
    int32_t width, int32_t height) const {
    
    if (x >= 0 && x < width && y >= 0 && y < height) {
        array[x][y] += volume;
    }
}

std::pair<int32_t, int32_t> OrderBookProcessor::clamp_coordinates(
    int32_t x, int32_t y, int32_t width, int32_t height) const {
    
    x = std::max(0, std::min(x, width - 1));
    y = std::max(0, std::min(y, height - 1));
    
    return std::make_pair(x, y);
}

} // namespace okx_data_processor
