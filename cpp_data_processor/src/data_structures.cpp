#include "data_structures.h"
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <algorithm>
#include <numeric>

namespace okx_data_processor {

// DataConverter implementation
std::vector<TradeData> DataConverter::from_python_trades(const pybind11::object& trades_df) {
    std::vector<TradeData> trades;
    
    try {
        // Extract data from pandas/polars DataFrame
        auto trade_ids = trades_df.attr("trade_id").cast<pybind11::array_t<int64_t>>();
        auto prices = trades_df.attr("price").cast<pybind11::array_t<double>>();
        auto quantities = trades_df.attr("quantity").cast<pybind11::array_t<double>>();
        auto is_buys = trades_df.attr("is_buy").cast<pybind11::array_t<bool>>();
        auto datetimes = trades_df.attr("datetime").cast<pybind11::array_t<int64_t>>();
        
        size_t size = trade_ids.size();
        trades.reserve(size);
        
        for (size_t i = 0; i < size; ++i) {
            auto datetime_point = std::chrono::system_clock::from_time_t(
                datetimes.at(i) / 1000) + 
                std::chrono::milliseconds(datetimes.at(i) % 1000);
            
            trades.emplace_back(
                trade_ids.at(i),
                prices.at(i),
                quantities.at(i),
                is_buys.at(i),
                datetime_point
            );
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert Python trades data: " + std::string(e.what()));
    }
    
    return trades;
}

pybind11::object DataConverter::to_python_trades(const std::vector<TradeData>& trades) {
    pybind11::list trade_ids, prices, quantities, is_buys, datetimes;
    
    for (const auto& trade : trades) {
        trade_ids.append(trade.trade_id);
        prices.append(trade.price);
        quantities.append(trade.quantity);
        is_buys.append(trade.is_buy);
        
        auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            trade.datetime.time_since_epoch()).count();
        datetimes.append(timestamp_ms);
    }
    
    pybind11::dict result;
    result["trade_id"] = trade_ids;
    result["price"] = prices;
    result["quantity"] = quantities;
    result["is_buy"] = is_buys;
    result["datetime"] = datetimes;
    
    return result;
}

pybind11::object DataConverter::to_python_candles(const std::vector<CandleData>& candles) {
    pybind11::list open_prices, high_prices, low_prices, close_prices, volumes;
    pybind11::list start_trade_ids, end_trade_ids, start_timestamps, end_timestamps, trades_counts;
    
    for (const auto& candle : candles) {
        open_prices.append(candle.open_price);
        high_prices.append(candle.high_price);
        low_prices.append(candle.low_price);
        close_prices.append(candle.close_price);
        volumes.append(candle.volume);
        start_trade_ids.append(candle.start_trade_id);
        end_trade_ids.append(candle.end_trade_id);
        start_timestamps.append(candle.start_timestamp_ms);
        end_timestamps.append(candle.end_timestamp_ms);
        trades_counts.append(candle.trades_count);
    }
    
    pybind11::dict result;
    result["open_price"] = open_prices;
    result["high_price"] = high_prices;
    result["low_price"] = low_prices;
    result["close_price"] = close_prices;
    result["volume"] = volumes;
    result["start_trade_id"] = start_trade_ids;
    result["end_trade_id"] = end_trade_ids;
    result["start_timestamp_ms"] = start_timestamps;
    result["end_timestamp_ms"] = end_timestamps;
    result["trades_count"] = trades_counts;
    
    return result;
}

pybind11::object DataConverter::to_python_bollinger(const BollingerBands& bollinger) {
    pybind11::dict result;
    result["upper_band"] = bollinger.upper_band;
    result["middle_band"] = bollinger.middle_band;
    result["lower_band"] = bollinger.lower_band;
    result["timeperiod"] = bollinger.timeperiod;
    return result;
}

pybind11::object DataConverter::to_python_rsi(const RSIData& rsi) {
    pybind11::dict result;
    result["rsi_values"] = rsi.rsi_values;
    result["timeperiod"] = rsi.timeperiod;
    return result;
}

pybind11::object DataConverter::to_python_smoothed(const std::vector<SmoothedLine>& lines) {
    pybind11::list is_buys, start_prices, end_prices, quantities, volumes;
    pybind11::list start_trade_ids, end_trade_ids, start_datetimes, end_datetimes;
    
    for (const auto& line : lines) {
        is_buys.append(line.is_buy);
        start_prices.append(line.start_price);
        end_prices.append(line.end_price);
        quantities.append(line.quantity);
        volumes.append(line.volume);
        start_trade_ids.append(line.start_trade_id);
        end_trade_ids.append(line.end_trade_id);
        
        auto start_timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            line.start_datetime.time_since_epoch()).count();
        auto end_timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            line.end_datetime.time_since_epoch()).count();
        
        start_datetimes.append(start_timestamp_ms);
        end_datetimes.append(end_timestamp_ms);
    }
    
    pybind11::dict result;
    result["is_buy"] = is_buys;
    result["start_price"] = start_prices;
    result["end_price"] = end_prices;
    result["quantity"] = quantities;
    result["volume"] = volumes;
    result["start_trade_id"] = start_trade_ids;
    result["end_trade_id"] = end_trade_ids;
    result["start_datetime"] = start_datetimes;
    result["end_datetime"] = end_datetimes;
    
    return result;
}

pybind11::object DataConverter::to_python_extreme_lines(const std::vector<ExtremeLine>& lines) {
    pybind11::list prices, start_trade_ids, end_trade_ids;
    
    for (const auto& line : lines) {
        prices.append(line.price);
        start_trade_ids.append(line.start_trade_id);
        end_trade_ids.append(line.end_trade_id);
    }
    
    pybind11::dict result;
    result["price"] = prices;
    result["start_trade_id"] = start_trade_ids;
    result["end_trade_id"] = end_trade_ids;
    
    return result;
}

pybind11::object DataConverter::to_python_order_book_volumes(const OrderBookVolumes& volumes) {
    pybind11::dict result;
    result["asks_array"] = volumes.asks_array;
    result["bids_array"] = volumes.bids_array;
    result["width"] = volumes.width;
    result["height"] = volumes.height;
    result["scale"] = volumes.scale;
    result["min_trade_id"] = volumes.min_trade_id;
    result["min_price"] = volumes.min_price;
    return result;
}

pybind11::object DataConverter::to_python_velocity(const VelocityData& velocity) {
    pybind11::dict result;
    result["velocity_values"] = velocity.velocity_values;
    result["interval"] = velocity.interval;
    return result;
}

// Conversion from Python to C++ structures
std::vector<CandleData> DataConverter::from_python_candles(const pybind11::object& candles_df) {
    std::vector<CandleData> candles;
    
    try {
        auto open_prices = candles_df.attr("open_price").cast<pybind11::array_t<double>>();
        auto high_prices = candles_df.attr("high_price").cast<pybind11::array_t<double>>();
        auto low_prices = candles_df.attr("low_price").cast<pybind11::array_t<double>>();
        auto close_prices = candles_df.attr("close_price").cast<pybind11::array_t<double>>();
        auto volumes = candles_df.attr("volume").cast<pybind11::array_t<double>>();
        auto start_trade_ids = candles_df.attr("start_trade_id").cast<pybind11::array_t<int64_t>>();
        auto end_trade_ids = candles_df.attr("end_trade_id").cast<pybind11::array_t<int64_t>>();
        auto start_timestamps = candles_df.attr("start_timestamp_ms").cast<pybind11::array_t<int64_t>>();
        auto end_timestamps = candles_df.attr("end_timestamp_ms").cast<pybind11::array_t<int64_t>>();
        auto trades_counts = candles_df.attr("trades_count").cast<pybind11::array_t<int32_t>>();
        
        size_t size = open_prices.size();
        candles.reserve(size);
        
        for (size_t i = 0; i < size; ++i) {
            candles.emplace_back(
                open_prices.at(i),
                high_prices.at(i),
                low_prices.at(i),
                close_prices.at(i),
                volumes.at(i),
                start_trade_ids.at(i),
                end_trade_ids.at(i),
                start_timestamps.at(i),
                end_timestamps.at(i),
                trades_counts.at(i)
            );
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert Python candles data: " + std::string(e.what()));
    }
    
    return candles;
}

BollingerBands DataConverter::from_python_bollinger(const pybind11::object& bollinger_df) {
    BollingerBands bollinger;
    
    try {
        bollinger.upper_band = bollinger_df.attr("upper_band").cast<std::vector<double>>();
        bollinger.middle_band = bollinger_df.attr("middle_band").cast<std::vector<double>>();
        bollinger.lower_band = bollinger_df.attr("lower_band").cast<std::vector<double>>();
        bollinger.timeperiod = bollinger_df.attr("timeperiod").cast<int32_t>();
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert Python Bollinger Bands data: " + std::string(e.what()));
    }
    
    return bollinger;
}

RSIData DataConverter::from_python_rsi(const pybind11::object& rsi_df) {
    RSIData rsi;
    
    try {
        rsi.rsi_values = rsi_df.attr("rsi_values").cast<std::vector<double>>();
        rsi.timeperiod = rsi_df.attr("timeperiod").cast<int32_t>();
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert Python RSI data: " + std::string(e.what()));
    }
    
    return rsi;
}

std::vector<SmoothedLine> DataConverter::from_python_smoothed(const pybind11::object& lines_df) {
    std::vector<SmoothedLine> lines;
    
    try {
        auto is_buys = lines_df.attr("is_buy").cast<pybind11::array_t<bool>>();
        auto start_prices = lines_df.attr("start_price").cast<pybind11::array_t<double>>();
        auto end_prices = lines_df.attr("end_price").cast<pybind11::array_t<double>>();
        auto quantities = lines_df.attr("quantity").cast<pybind11::array_t<double>>();
        auto volumes = lines_df.attr("volume").cast<pybind11::array_t<double>>();
        auto start_trade_ids = lines_df.attr("start_trade_id").cast<pybind11::array_t<int64_t>>();
        auto end_trade_ids = lines_df.attr("end_trade_id").cast<pybind11::array_t<int64_t>>();
        auto start_datetimes = lines_df.attr("start_datetime").cast<pybind11::array_t<int64_t>>();
        auto end_datetimes = lines_df.attr("end_datetime").cast<pybind11::array_t<int64_t>>();
        
        size_t size = is_buys.size();
        lines.reserve(size);
        
        for (size_t i = 0; i < size; ++i) {
            auto start_datetime_point = std::chrono::system_clock::from_time_t(
                start_datetimes.at(i) / 1000) + 
                std::chrono::milliseconds(start_datetimes.at(i) % 1000);
            auto end_datetime_point = std::chrono::system_clock::from_time_t(
                end_datetimes.at(i) / 1000) + 
                std::chrono::milliseconds(end_datetimes.at(i) % 1000);
            
            lines.emplace_back(
                is_buys.at(i),
                start_prices.at(i),
                end_prices.at(i),
                quantities.at(i),
                volumes.at(i),
                start_trade_ids.at(i),
                end_trade_ids.at(i),
                start_datetime_point,
                end_datetime_point
            );
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert Python smoothed lines data: " + std::string(e.what()));
    }
    
    return lines;
}

std::vector<ExtremeLine> DataConverter::from_python_extreme_lines(const pybind11::object& lines_df) {
    std::vector<ExtremeLine> lines;
    
    try {
        auto prices = lines_df.attr("price").cast<pybind11::array_t<double>>();
        auto start_trade_ids = lines_df.attr("start_trade_id").cast<pybind11::array_t<int64_t>>();
        auto end_trade_ids = lines_df.attr("end_trade_id").cast<pybind11::array_t<int64_t>>();
        
        size_t size = prices.size();
        lines.reserve(size);
        
        for (size_t i = 0; i < size; ++i) {
            lines.emplace_back(
                prices.at(i),
                start_trade_ids.at(i),
                end_trade_ids.at(i)
            );
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert Python extreme lines data: " + std::string(e.what()));
    }
    
    return lines;
}

OrderBookVolumes DataConverter::from_python_order_book_volumes(const pybind11::object& volumes_df) {
    OrderBookVolumes volumes;
    
    try {
        volumes.asks_array = volumes_df.attr("asks_array").cast<std::vector<std::vector<double>>>();
        volumes.bids_array = volumes_df.attr("bids_array").cast<std::vector<std::vector<double>>>();
        volumes.width = volumes_df.attr("width").cast<int32_t>();
        volumes.height = volumes_df.attr("height").cast<int32_t>();
        volumes.scale = volumes_df.attr("scale").cast<double>();
        volumes.min_trade_id = volumes_df.attr("min_trade_id").cast<int64_t>();
        volumes.min_price = volumes_df.attr("min_price").cast<double>();
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert Python order book volumes data: " + std::string(e.what()));
    }
    
    return volumes;
}

VelocityData DataConverter::from_python_velocity(const pybind11::object& velocity_df) {
    VelocityData velocity;
    
    try {
        velocity.velocity_values = velocity_df.attr("velocity_values").cast<std::vector<double>>();
        velocity.interval = velocity_df.attr("interval").cast<std::string>();
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert Python velocity data: " + std::string(e.what()));
    }
    
    return velocity;
}

} // namespace okx_data_processor
