#include "data_structures.h"
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <algorithm>
#include <numeric>

namespace okx_data_processor {

// DataConverter implementation
std::vector<TradeData> DataConverter::from_polars_trades(const pybind11::object& polars_dataframe) {
    std::vector<TradeData> trades;
    
    try {
        // Handle Polars DataFrame
        auto df = polars_dataframe.cast<pybind11::object>();
        
        // Get columns from DataFrame
        auto trade_ids_series = df.attr("__getitem__")("trade_id");
        auto prices_series = df.attr("__getitem__")("price");
        auto quantities_series = df.attr("__getitem__")("quantity");
        auto is_buys_series = df.attr("__getitem__")("is_buy");
        auto datetimes_series = df.attr("__getitem__")("datetime");
        
        // Get length from one of the series
        size_t size = trade_ids_series.attr("len")().cast<size_t>();
        trades.reserve(size);
        
        // Convert polars.Series to numpy arrays for efficient access
        auto trade_ids_np = trade_ids_series.attr("to_numpy")().cast<pybind11::array_t<int64_t>>();
        auto prices_np = prices_series.attr("to_numpy")().cast<pybind11::array_t<double>>();
        auto quantities_np = quantities_series.attr("to_numpy")().cast<pybind11::array_t<double>>();
        auto is_buys_np = is_buys_series.attr("to_numpy")().cast<pybind11::array_t<bool>>();
        auto datetimes_np = datetimes_series.attr("to_numpy")().cast<pybind11::array_t<int64_t>>();
        
        // Get raw pointers for fast access
        auto trade_ids_ptr = trade_ids_np.data();
        auto prices_ptr = prices_np.data();
        auto quantities_ptr = quantities_np.data();
        auto is_buys_ptr = is_buys_np.data();
        auto datetimes_ptr = datetimes_np.data();
        
        for (size_t i = 0; i < size; ++i) {
            auto datetime_point = std::chrono::system_clock::from_time_t(
                datetimes_ptr[i] / 1000) + 
                std::chrono::milliseconds(datetimes_ptr[i] % 1000);
            
            trades.emplace_back(
                trade_ids_ptr[i],
                prices_ptr[i],
                quantities_ptr[i],
                is_buys_ptr[i],
                datetime_point
            );
        }
        
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert Polars DataFrame trades data to C++ trades data: " + std::string(e.what()));
    }
    
    return trades;
}


pybind11::object DataConverter::to_polars_candles(const std::vector<CandleData>& candles) {
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
    
    // Create Polars DataFrame
    pybind11::dict data;
    data["open_price"] = open_prices;
    data["high_price"] = high_prices;
    data["low_price"] = low_prices;
    data["close_price"] = close_prices;
    data["volume"] = volumes;
    data["start_trade_id"] = start_trade_ids;
    data["end_trade_id"] = end_trade_ids;
    data["start_timestamp_ms"] = start_timestamps;
    data["end_timestamp_ms"] = end_timestamps;
    data["trades_count"] = trades_counts;
    
    pybind11::module polars = pybind11::module::import("polars");
    return polars.attr("DataFrame")(data);
}


pybind11::object DataConverter::to_polars_bollinger(const BollingerBands& bollinger) {
    pybind11::dict data;
    data["upper_band"] = bollinger.upper_band;
    data["middle_band"] = bollinger.middle_band;
    data["lower_band"] = bollinger.lower_band;
    data["timeperiod"] = bollinger.timeperiod;
    
    pybind11::module polars = pybind11::module::import("polars");
    return polars.attr("DataFrame")(data);
}


pybind11::object DataConverter::to_polars_rsi(const RSIData& rsi) {
    pybind11::dict data;
    data["rsi_values"] = rsi.rsi_values;
    data["timeperiod"] = rsi.timeperiod;
    
    pybind11::module polars = pybind11::module::import("polars");
    return polars.attr("DataFrame")(data);
}


pybind11::object DataConverter::to_polars_smoothed_lines(const std::vector<SmoothedLine>& lines) {
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
    
    pybind11::dict data;
    data["is_buy"] = is_buys;
    data["start_price"] = start_prices;
    data["end_price"] = end_prices;
    data["quantity"] = quantities;
    data["volume"] = volumes;
    data["start_trade_id"] = start_trade_ids;
    data["end_trade_id"] = end_trade_ids;
    data["start_datetime"] = start_datetimes;
    data["end_datetime"] = end_datetimes;
    
    pybind11::module polars = pybind11::module::import("polars");
    return polars.attr("DataFrame")(data);
}

pybind11::object DataConverter::to_polars_smoothed_data(const std::vector<SmoothedDataPoint>& data_points) {
    pybind11::list trade_ids, prices, datetimes;
    
    for (const auto& point : data_points) {
        trade_ids.append(point.trade_id);
        prices.append(point.price);
        
        auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            point.datetime.time_since_epoch()).count();
        datetimes.append(timestamp_ms);
    }
    
    pybind11::dict data;
    data["trade_id"] = trade_ids;
    data["price"] = prices;
    data["datetime"] = datetimes;
    
    pybind11::module polars = pybind11::module::import("polars");
    return polars.attr("DataFrame")(data);
}


pybind11::object DataConverter::to_polars_extreme_lines(const std::vector<ExtremeLine>& lines) {
    pybind11::list prices, start_trade_ids, end_trade_ids;
    
    for (const auto& line : lines) {
        prices.append(line.price);
        start_trade_ids.append(line.start_trade_id);
        end_trade_ids.append(line.end_trade_id);
    }
    
    pybind11::dict data;
    data["price"] = prices;
    data["start_trade_id"] = start_trade_ids;
    data["end_trade_id"] = end_trade_ids;
    
    pybind11::module polars = pybind11::module::import("polars");
    return polars.attr("DataFrame")(data);
}

pybind11::object DataConverter::to_numpy_extreme_lines(const std::vector<ExtremeLine>& lines) {
    if (lines.empty()) {
        // Return empty numpy array with shape (0, 3)
        pybind11::module numpy = pybind11::module::import("numpy");
        return numpy.attr("array")(pybind11::list(), pybind11::str("float64")).attr("reshape")(0, 3);
    }
    
    // Create 2D array: each row is [price, start_trade_id, end_trade_id]
    pybind11::list rows;
    for (const auto& line : lines) {
        pybind11::list row;
        row.append(line.price);
        row.append(static_cast<double>(line.start_trade_id));
        row.append(static_cast<double>(line.end_trade_id));
        rows.append(row);
    }
    
    pybind11::module numpy = pybind11::module::import("numpy");
    return numpy.attr("array")(rows, pybind11::str("float64"));
}


pybind11::object DataConverter::to_polars_order_book_volumes(const OrderBookVolumes& volumes) {
    pybind11::dict data;
    data["asks_array"] = volumes.asks_array;
    data["bids_array"] = volumes.bids_array;
    data["width"] = volumes.width;
    data["height"] = volumes.height;
    data["scale"] = volumes.scale;
    data["min_trade_id"] = volumes.min_trade_id;
    data["min_price"] = volumes.min_price;
    
    pybind11::module polars = pybind11::module::import("polars");
    return polars.attr("DataFrame")(data);
}


pybind11::object DataConverter::to_polars_velocity(const VelocityData& velocity) {
    pybind11::dict data;
    data["velocity_values"] = velocity.velocity_values;
    data["interval"] = velocity.interval;
    
    pybind11::module polars = pybind11::module::import("polars");
    return polars.attr("DataFrame")(data);
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
