#pragma once

#include <vector>
#include <string>
#include <chrono>
#include <memory>
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
    
    TradeData() = default;
    TradeData(int64_t id, double p, double q, bool buy, std::chrono::system_clock::time_point dt)
        : trade_id(id), price(p), quantity(q), is_buy(buy), datetime(dt) {}
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
    CandleData(double open, double high, double low, double close, double vol,
               int64_t start_id, int64_t end_id, int64_t start_ts, int64_t end_ts, int32_t count)
        : open_price(open), high_price(high), low_price(low), close_price(close), volume(vol),
          start_trade_id(start_id), end_trade_id(end_id), start_timestamp_ms(start_ts),
          end_timestamp_ms(end_ts), trades_count(count) {}
};

/**
 * @brief Bollinger Bands data structure
 */
struct BollingerBands {
    std::vector<double> upper_band;
    std::vector<double> middle_band;
    std::vector<double> lower_band;
    int32_t timeperiod;
    
    BollingerBands() : timeperiod(20) {}
    BollingerBands(int32_t period) : timeperiod(period) {}
};

/**
 * @brief RSI data structure
 */
struct RSIData {
    std::vector<double> rsi_values;
    int32_t timeperiod;
    
    RSIData() : timeperiod(14) {}
    RSIData(int32_t period) : timeperiod(period) {}
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
    std::string error_message;
    double processing_time_seconds;
    
    ProcessingResult() : success(false), processing_time_seconds(0.0) {}
    ProcessingResult(bool s, const std::string& msg, double time)
        : success(s), error_message(msg), processing_time_seconds(time) {}
};

/**
 * @brief Symbol ID enumeration
 */
enum class SymbolId : int32_t {
    BTC_USDT = 0,
    ETH_USDT = 1,
    // Add more symbols as needed
};

/**
 * @brief Utility functions for data conversion
 */
class DataConverter {
public:
    // Convert Python objects to C++ structures
    static std::vector<TradeData> from_python_trades(const pybind11::object& trades_df);
    static pybind11::object to_python_candles(const std::vector<CandleData>& candles);
    static pybind11::object to_python_bollinger(const BollingerBands& bollinger);
    static pybind11::object to_python_rsi(const RSIData& rsi);
    static pybind11::object to_python_smoothed(const std::vector<SmoothedLine>& lines);
    static pybind11::object to_python_extreme_lines(const std::vector<ExtremeLine>& lines);
    static pybind11::object to_python_order_book_volumes(const OrderBookVolumes& volumes);
    static pybind11::object to_python_velocity(const VelocityData& velocity);
    
    // Convert C++ structures to Python objects
    static pybind11::object to_python_trades(const std::vector<TradeData>& trades);
    static std::vector<CandleData> from_python_candles(const pybind11::object& candles_df);
    static BollingerBands from_python_bollinger(const pybind11::object& bollinger_df);
    static RSIData from_python_rsi(const pybind11::object& rsi_df);
    static std::vector<SmoothedLine> from_python_smoothed(const pybind11::object& lines_df);
    static std::vector<ExtremeLine> from_python_extreme_lines(const pybind11::object& lines_df);
    static OrderBookVolumes from_python_order_book_volumes(const pybind11::object& volumes_df);
    static VelocityData from_python_velocity(const pybind11::object& velocity_df);
};

} // namespace okx_data_processor
