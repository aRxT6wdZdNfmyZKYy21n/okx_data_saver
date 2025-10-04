#pragma once

#include <vector>
#include <string>
#include <string_view>
#include <chrono>
#include <memory>
#include <optional>
#include <variant>
#include <cstdint>
#include <pybind11/pybind11.h>

namespace okx_data_processor {

// Forward declarations
enum class SymbolId : int32_t;

/**
 * @brief Trade data structure
 */
struct TradeData {
    int64_t trade_id;
    double price;
    double quantity;
    bool is_buy;
    std::chrono::system_clock::time_point datetime;
    
    // C++17: Use default member initializers and aggregate initialization
    TradeData() = default;
    
    // C++17: Use auto for parameter deduction
    constexpr TradeData(int64_t id, double p, double q, bool buy, 
                       std::chrono::system_clock::time_point dt) noexcept
        : trade_id(id), price(p), quantity(q), is_buy(buy), datetime(dt) {}
    
    // C++17: Add constexpr methods
    constexpr bool is_valid() const noexcept {
        return trade_id > 0 && price > 0.0 && quantity > 0.0;
    }
    
    // C++17: Add structured binding support
    auto as_tuple() const noexcept {
        return std::make_tuple(trade_id, price, quantity, is_buy, datetime);
    }
};

/**
 * @brief Candle data structure
 */
struct CandleData {
    double open_price;
    double high_price;
    double low_price;
    double close_price;
    double volume;
    int64_t start_trade_id;
    int64_t end_trade_id;
    int64_t start_timestamp_ms;
    int64_t end_timestamp_ms;
    int32_t trades_count;
    
    CandleData() = default;
    
    // C++17: Use constexpr constructor
    constexpr CandleData(double open, double high, double low, double close, double vol,
                        int64_t start_id, int64_t end_id, int64_t start_ts, int64_t end_ts, int32_t count) noexcept
        : open_price(open), high_price(high), low_price(low), close_price(close), volume(vol),
          start_trade_id(start_id), end_trade_id(end_id), start_timestamp_ms(start_ts),
          end_timestamp_ms(end_ts), trades_count(count) {}
    
    // C++17: Add constexpr validation
    constexpr bool is_valid() const noexcept {
        return open_price > 0.0 && high_price > 0.0 && low_price > 0.0 && close_price > 0.0 &&
               volume >= 0.0 && start_trade_id > 0 && end_trade_id >= start_trade_id &&
               start_timestamp_ms > 0 && end_timestamp_ms >= start_timestamp_ms && trades_count >= 0;
    }
    
    // C++17: Add price range validation
    constexpr bool has_valid_price_range() const noexcept {
        return high_price >= low_price && 
               high_price >= open_price && high_price >= close_price &&
               low_price <= open_price && low_price <= close_price;
    }
    
    // C++17: Calculate price change
    constexpr double price_change() const noexcept {
        return close_price - open_price;
    }
    
    // C++17: Calculate price change percentage
    constexpr double price_change_percent() const noexcept {
        return open_price != 0.0 ? (price_change() / open_price) * 100.0 : 0.0;
    }
};

/**
 * @brief Bollinger Bands data structure
 */
struct BollingerBands {
    std::vector<double> upper_band;
    std::vector<double> middle_band;
    std::vector<double> lower_band;
    int32_t timeperiod;
    
    // C++17: Use default member initializers
    BollingerBands() : timeperiod(20) {}
    BollingerBands(int32_t period) noexcept : timeperiod(period) {}
    
    // C++17: Add validation (not constexpr due to vector operations)
    bool is_valid() const noexcept {
        return timeperiod > 0 && 
               upper_band.size() == middle_band.size() && 
               middle_band.size() == lower_band.size();
    }
    
    // C++17: Add size validation
    size_t size() const noexcept {
        return upper_band.size();
    }
    
    // C++17: Add empty check
    bool empty() const noexcept {
        return upper_band.empty();
    }
    
    // C++17: Add structured binding support for bands
    auto as_tuple() const noexcept {
        return std::make_tuple(std::cref(upper_band), std::cref(middle_band), std::cref(lower_band));
    }
};

/**
 * @brief RSI data structure
 */
struct RSIData {
    std::vector<double> rsi_values;
    int32_t timeperiod;
    
    // C++17: Use default member initializers
    RSIData() : timeperiod(14) {}
    RSIData(int32_t period) noexcept : timeperiod(period) {}
    
    // C++17: Add validation (not constexpr due to vector operations)
    bool is_valid() const noexcept {
        return timeperiod > 0 && std::all_of(rsi_values.begin(), rsi_values.end(),
            [](double value) { return value >= 0.0 && value <= 100.0; });
    }
    
    // C++17: Add size validation
    size_t size() const noexcept {
        return rsi_values.size();
    }
    
    // C++17: Add empty check
    bool empty() const noexcept {
        return rsi_values.empty();
    }
    
    // C++17: Add RSI classification
    bool is_overbought(double threshold = 70.0) const noexcept {
        return !rsi_values.empty() && rsi_values.back() > threshold;
    }
    
    bool is_oversold(double threshold = 30.0) const noexcept {
        return !rsi_values.empty() && rsi_values.back() < threshold;
    }
};

/**
 * @brief Smoothed line data structure
 */
struct SmoothedLine {
    bool is_buy;
    double start_price;
    double end_price;
    double quantity;
    double volume;
    int64_t start_trade_id;
    int64_t end_trade_id;
    std::chrono::system_clock::time_point start_datetime;
    std::chrono::system_clock::time_point end_datetime;
    
    SmoothedLine() = default;
    SmoothedLine(bool buy, double start_p, double end_p, double q, double vol,
                 int64_t start_id, int64_t end_id,
                 std::chrono::system_clock::time_point start_dt,
                 std::chrono::system_clock::time_point end_dt)
        : is_buy(buy), start_price(start_p), end_price(end_p), quantity(q), volume(vol),
          start_trade_id(start_id), end_trade_id(end_id), start_datetime(start_dt), end_datetime(end_dt) {}
};

/**
 * @brief Extreme line data structure
 */
struct ExtremeLine {
    double price;
    int64_t start_trade_id;
    int64_t end_trade_id;
    
    ExtremeLine() = default;
    ExtremeLine(double p, int64_t start_id, int64_t end_id)
        : price(p), start_trade_id(start_id), end_trade_id(end_id) {}
};

/**
 * @brief Order book volume data structure
 */
struct OrderBookVolumes {
    std::vector<std::vector<double>> asks_array;
    std::vector<std::vector<double>> bids_array;
    int32_t width;
    int32_t height;
    double scale;
    int64_t min_trade_id;
    double min_price;
    
    OrderBookVolumes() = default;
    OrderBookVolumes(int32_t w, int32_t h, double s, int64_t min_id, double min_p)
        : width(w), height(h), scale(s), min_trade_id(min_id), min_price(min_p) {
        asks_array.resize(width, std::vector<double>(height, 0.0));
        bids_array.resize(width, std::vector<double>(height, 0.0));
    }
};

/**
 * @brief Velocity data structure
 */
struct VelocityData {
    std::vector<double> velocity_values;
    std::string interval;
    
    VelocityData() = default;
    VelocityData(const std::string& intv) : interval(intv) {}
};

/**
 * @brief Processing result structure
 */
struct ProcessingResult {
    bool success;
    std::optional<std::string> error_message;  // C++17: Use optional for error message
    double processing_time_seconds;
    
    // C++17: Use default member initializers
    ProcessingResult() : success(false), processing_time_seconds(0.0) {}
    
    // C++17: Use string_view for better performance
    ProcessingResult(bool s, std::string_view msg, double time) noexcept
        : success(s), error_message(std::string(msg)), processing_time_seconds(time) {}
    
    // C++17: Add methods
    bool has_error() const noexcept {
        return !success && error_message.has_value();
    }
    
    // C++17: Add structured binding support
    auto as_tuple() const noexcept {
        return std::make_tuple(success, error_message, processing_time_seconds);
    }
    
    // C++17: Add factory methods
    static ProcessingResult success_result(double time) noexcept {
        ProcessingResult result;
        result.success = true;
        result.processing_time_seconds = time;
        return result;
    }
    
    static ProcessingResult error_result(std::string_view msg, double time = 0.0) noexcept {
        return ProcessingResult{false, msg, time};
    }
};

/**
 * @brief Symbol ID enumeration
 */
enum class SymbolId : int32_t {
    BTC_USDT = 1,
    ETH_USDT = 2,
    // Add more symbols as needed
};

/**
 * @brief Utility functions for data conversion
 */
class DataConverter {
public:
    // Convert Polars DataFrame to C++ structures
    static std::vector<TradeData> from_polars_trades(const pybind11::object& polars_dataframe);
    
    // Convert C++ structures to Polars DataFrames (like Python version)
    static pybind11::object to_polars_candles(const std::vector<CandleData>& candles);
    static pybind11::object to_polars_bollinger(const BollingerBands& bollinger);
    static pybind11::object to_polars_rsi(const RSIData& rsi);
    static pybind11::object to_polars_smoothed_lines(const std::vector<SmoothedLine>& lines);
    static pybind11::object to_polars_extreme_lines(const std::vector<ExtremeLine>& lines);
    static pybind11::object to_numpy_extreme_lines(const std::vector<ExtremeLine>& lines);
    static pybind11::object to_polars_order_book_volumes(const OrderBookVolumes& volumes);
    static pybind11::object to_polars_velocity(const VelocityData& velocity);
    
    // Convert C++ structures to Python objects
    static std::vector<CandleData> from_python_candles(const pybind11::object& candles_df);
    static BollingerBands from_python_bollinger(const pybind11::object& bollinger_df);
    static RSIData from_python_rsi(const pybind11::object& rsi_df);
    static std::vector<SmoothedLine> from_python_smoothed(const pybind11::object& lines_df);
    static std::vector<ExtremeLine> from_python_extreme_lines(const pybind11::object& lines_df);
    static OrderBookVolumes from_python_order_book_volumes(const pybind11::object& volumes_df);
    static VelocityData from_python_velocity(const pybind11::object& velocity_df);
};

} // namespace okx_data_processor
