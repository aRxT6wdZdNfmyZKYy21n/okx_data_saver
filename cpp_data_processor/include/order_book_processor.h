#pragma once

#include "data_structures.h"
#include <vector>
#include <string>
#include <map>
#include <pybind11/pybind11.h>

namespace okx_data_processor {

/**
 * @brief Order book snapshot structure
 */
struct OrderBookSnapshot {
    int64_t timestamp_ms;
    std::vector<std::pair<double, double>> asks;  // price, volume
    std::vector<std::pair<double, double>> bids;  // price, volume
    
    OrderBookSnapshot() = default;
    OrderBookSnapshot(int64_t ts, 
                     const std::vector<std::pair<double, double>>& a,
                     const std::vector<std::pair<double, double>>& b)
        : timestamp_ms(ts), asks(a), bids(b) {}
};

/**
 * @brief Order book volumes processor
 * 
 * Processes trade data to create order book volume arrays
 * for visualization and analysis.
 */
class OrderBookProcessor {
public:
    OrderBookProcessor();
    ~OrderBookProcessor() = default;

    // Disable copy constructor and assignment operator
    OrderBookProcessor(const OrderBookProcessor&) = delete;
    OrderBookProcessor& operator=(const OrderBookProcessor&) = delete;

    /**
     * @brief Process order book volumes from trades data
     * 
     * @param symbol_id Symbol identifier
     * @param trades Vector of trade data
     * @return OrderBookVolumes Processed order book volumes
     */
    OrderBookVolumes process_order_book_volumes(
        SymbolId symbol_id, const std::vector<TradeData>& trades);

    /**
     * @brief Process order book volumes with existing order book data
     * 
     * @param symbol_id Symbol identifier
     * @param trades Vector of trade data
     * @param order_book_data Vector of order book snapshots
     * @return OrderBookVolumes Processed order book volumes
     */
    OrderBookVolumes process_order_book_volumes_with_snapshots(
        SymbolId symbol_id, 
        const std::vector<TradeData>& trades,
        const std::vector<OrderBookSnapshot>& order_book_data);

    /**
     * @brief Calculate array dimensions based on trade data
     * 
     * @param trades Vector of trade data
     * @param height Desired height
     * @return std::pair<int32_t, int32_t> Width and height
     */
    std::pair<int32_t, int32_t> calculate_array_dimensions(
        const std::vector<TradeData>& trades, int32_t height = 100) const;

    /**
     * @brief Calculate scale factor for array coordinates
     * 
     * @param trades Vector of trade data
     * @param width Array width
     * @param height Array height
     * @return double Scale factor
     */
    double calculate_scale_factor(
        const std::vector<TradeData>& trades, int32_t width, int32_t height) const;

    /**
     * @brief Set processing parameters
     * 
     * @param params Dictionary of parameters
     */
    void set_processing_params(const pybind11::dict& params);

    /**
     * @brief Get processing parameters
     * 
     * @return pybind11::dict Dictionary of parameters
     */
    pybind11::dict get_processing_params() const;

private:
    // Processing parameters
    struct ProcessingParams {
        int32_t default_height = 100;
        double min_price_delta = 0.001;
        double min_trade_id_delta = 1.0;
        bool enable_asks_processing = true;
        bool enable_bids_processing = true;
        double volume_aggregation_factor = 1.0;
    } params_;

    /**
     * @brief Process order book volumes from trades only
     * 
     * @param trades Vector of trade data
     * @param width Array width
     * @param height Array height
     * @param scale Scale factor
     * @param min_trade_id Minimum trade ID
     * @param min_price Minimum price
     * @return OrderBookVolumes Processed volumes
     */
    OrderBookVolumes process_volumes_from_trades(
        const std::vector<TradeData>& trades,
        int32_t width, int32_t height, double scale,
        int64_t min_trade_id, double min_price) const;

    /**
     * @brief Process order book volumes with snapshots
     * 
     * @param trades Vector of trade data
     * @param order_book_data Vector of order book snapshots
     * @param width Array width
     * @param height Array height
     * @param scale Scale factor
     * @param min_trade_id Minimum trade ID
     * @param min_price Minimum price
     * @return OrderBookVolumes Processed volumes
     */
    OrderBookVolumes process_volumes_with_snapshots(
        const std::vector<TradeData>& trades,
        const std::vector<OrderBookSnapshot>& order_book_data,
        int32_t width, int32_t height, double scale,
        int64_t min_trade_id, double min_price) const;

    /**
     * @brief Fill volume arrays from trades
     * 
     * @param trades Vector of trade data
     * @param asks_array Asks volume array
     * @param bids_array Bids volume array
     * @param width Array width
     * @param height Array height
     * @param scale Scale factor
     * @param min_trade_id Minimum trade ID
     * @param min_price Minimum price
     */
    void fill_volumes_from_trades(
        const std::vector<TradeData>& trades,
        std::vector<std::vector<double>>& asks_array,
        std::vector<std::vector<double>>& bids_array,
        int32_t width, int32_t height, double scale,
        int64_t min_trade_id, double min_price) const;

    /**
     * @brief Fill volume arrays from order book snapshots
     * 
     * @param order_book_data Vector of order book snapshots
     * @param asks_array Asks volume array
     * @param bids_array Bids volume array
     * @param width Array width
     * @param height Array height
     * @param scale Scale factor
     * @param min_trade_id Minimum trade ID
     * @param min_price Minimum price
     */
    void fill_volumes_from_snapshots(
        const std::vector<OrderBookSnapshot>& order_book_data,
        std::vector<std::vector<double>>& asks_array,
        std::vector<std::vector<double>>& bids_array,
        int32_t width, int32_t height, double scale,
        int64_t min_trade_id, double min_price) const;

    /**
     * @brief Get price and trade ID ranges from trades
     * 
     * @param trades Vector of trade data
     * @return std::pair<std::pair<double, double>, std::pair<int64_t, int64_t>> 
     *         Price range (min, max) and trade ID range (min, max)
     */
    std::pair<std::pair<double, double>, std::pair<int64_t, int64_t>> get_ranges(
        const std::vector<TradeData>& trades) const;

    /**
     * @brief Calculate volume contribution for a trade
     * 
     * @param trade Trade data
     * @param scale Scale factor
     * @return double Volume contribution
     */
    double calculate_volume_contribution(const TradeData& trade, double scale) const;

    /**
     * @brief Update volume array at specific coordinates
     * 
     * @param array Volume array to update
     * @param x X coordinate
     * @param y Y coordinate
     * @param volume Volume to add
     * @param width Array width
     * @param height Array height
     */
    void update_volume_at_coordinates(
        std::vector<std::vector<double>>& array,
        int32_t x, int32_t y, double volume,
        int32_t width, int32_t height) const;

    /**
     * @brief Clamp coordinates to array bounds
     * 
     * @param x X coordinate
     * @param y Y coordinate
     * @param width Array width
     * @param height Array height
     * @return std::pair<int32_t, int32_t> Clamped coordinates
     */
    std::pair<int32_t, int32_t> clamp_coordinates(
        int32_t x, int32_t y, int32_t width, int32_t height) const;
};

} // namespace okx_data_processor
