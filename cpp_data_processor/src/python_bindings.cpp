#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include <pybind11/numpy.h>
#include <pybind11/functional.h>

#include "main_processor.h"
#include "data_structures.h"
#include "bollinger_bands.h"
#include "candles_processor.h"
#include "rsi_calculator.h"
#include "smoothing_processor.h"
#include "extreme_lines_processor.h"
#include "order_book_processor.h"
#include "velocity_calculator.h"

namespace py = pybind11;

PYBIND11_MODULE(cpp_data_processor, m) {
    m.doc() = "C++ Data Processor for OKX Data Saver with Python bindings";

    // Bind SymbolId enum
    py::enum_<okx_data_processor::SymbolId>(m, "SymbolId")
        .value("BTC_USDT", okx_data_processor::SymbolId::BTC_USDT)
        .value("ETH_USDT", okx_data_processor::SymbolId::ETH_USDT);

    // Bind TradeData struct
    py::class_<okx_data_processor::TradeData>(m, "TradeData")
        .def(py::init<>())
        .def(py::init<int64_t, double, double, bool, std::chrono::system_clock::time_point>(),
             py::arg("trade_id"), py::arg("price"), py::arg("quantity"), 
             py::arg("is_buy"), py::arg("datetime"))
        .def_readwrite("trade_id", &okx_data_processor::TradeData::trade_id)
        .def_readwrite("price", &okx_data_processor::TradeData::price)
        .def_readwrite("quantity", &okx_data_processor::TradeData::quantity)
        .def_readwrite("is_buy", &okx_data_processor::TradeData::is_buy)
        .def_readwrite("datetime", &okx_data_processor::TradeData::datetime);

    // Bind CandleData struct
    py::class_<okx_data_processor::CandleData>(m, "CandleData")
        .def(py::init<>())
        .def(py::init<double, double, double, double, double, int64_t, int64_t, 
                      int64_t, int64_t, int32_t>(),
             py::arg("open_price"), py::arg("high_price"), py::arg("low_price"), 
             py::arg("close_price"), py::arg("volume"), py::arg("start_trade_id"),
             py::arg("end_trade_id"), py::arg("start_timestamp_ms"), 
             py::arg("end_timestamp_ms"), py::arg("trades_count"))
        .def_readwrite("open_price", &okx_data_processor::CandleData::open_price)
        .def_readwrite("high_price", &okx_data_processor::CandleData::high_price)
        .def_readwrite("low_price", &okx_data_processor::CandleData::low_price)
        .def_readwrite("close_price", &okx_data_processor::CandleData::close_price)
        .def_readwrite("volume", &okx_data_processor::CandleData::volume)
        .def_readwrite("start_trade_id", &okx_data_processor::CandleData::start_trade_id)
        .def_readwrite("end_trade_id", &okx_data_processor::CandleData::end_trade_id)
        .def_readwrite("start_timestamp_ms", &okx_data_processor::CandleData::start_timestamp_ms)
        .def_readwrite("end_timestamp_ms", &okx_data_processor::CandleData::end_timestamp_ms)
        .def_readwrite("trades_count", &okx_data_processor::CandleData::trades_count);

    // Bind BollingerBands struct
    py::class_<okx_data_processor::BollingerBands>(m, "BollingerBands")
        .def(py::init<>())
        .def(py::init<int32_t>(), py::arg("timeperiod"))
        .def_readwrite("upper_band", &okx_data_processor::BollingerBands::upper_band)
        .def_readwrite("middle_band", &okx_data_processor::BollingerBands::middle_band)
        .def_readwrite("lower_band", &okx_data_processor::BollingerBands::lower_band)
        .def_readwrite("timeperiod", &okx_data_processor::BollingerBands::timeperiod);

    // Bind RSIData struct
    py::class_<okx_data_processor::RSIData>(m, "RSIData")
        .def(py::init<>())
        .def(py::init<int32_t>(), py::arg("timeperiod"))
        .def_readwrite("rsi_values", &okx_data_processor::RSIData::rsi_values)
        .def_readwrite("timeperiod", &okx_data_processor::RSIData::timeperiod);

    // Bind SmoothedLine struct
    py::class_<okx_data_processor::SmoothedLine>(m, "SmoothedLine")
        .def(py::init<>())
        .def(py::init<bool, double, double, double, double, int64_t, int64_t,
                      std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>(),
             py::arg("is_buy"), py::arg("start_price"), py::arg("end_price"),
             py::arg("quantity"), py::arg("volume"), py::arg("start_trade_id"),
             py::arg("end_trade_id"), py::arg("start_datetime"), py::arg("end_datetime"))
        .def_readwrite("is_buy", &okx_data_processor::SmoothedLine::is_buy)
        .def_readwrite("start_price", &okx_data_processor::SmoothedLine::start_price)
        .def_readwrite("end_price", &okx_data_processor::SmoothedLine::end_price)
        .def_readwrite("quantity", &okx_data_processor::SmoothedLine::quantity)
        .def_readwrite("volume", &okx_data_processor::SmoothedLine::volume)
        .def_readwrite("start_trade_id", &okx_data_processor::SmoothedLine::start_trade_id)
        .def_readwrite("end_trade_id", &okx_data_processor::SmoothedLine::end_trade_id)
        .def_readwrite("start_datetime", &okx_data_processor::SmoothedLine::start_datetime)
        .def_readwrite("end_datetime", &okx_data_processor::SmoothedLine::end_datetime);

    // Bind SmoothedDataPoint struct
    py::class_<okx_data_processor::SmoothedDataPoint>(m, "SmoothedDataPoint")
        .def(py::init<>())
        .def(py::init<int64_t, double, std::chrono::system_clock::time_point>(),
             py::arg("trade_id"), py::arg("price"), py::arg("datetime"))
        .def_readwrite("trade_id", &okx_data_processor::SmoothedDataPoint::trade_id)
        .def_readwrite("price", &okx_data_processor::SmoothedDataPoint::price)
        .def_readwrite("datetime", &okx_data_processor::SmoothedDataPoint::datetime);

    // Bind ExtremeLine struct
    py::class_<okx_data_processor::ExtremeLine>(m, "ExtremeLine")
        .def(py::init<>())
        .def(py::init<double, int64_t, int64_t>(),
             py::arg("price"), py::arg("start_trade_id"), py::arg("end_trade_id"))
        .def_readwrite("price", &okx_data_processor::ExtremeLine::price)
        .def_readwrite("start_trade_id", &okx_data_processor::ExtremeLine::start_trade_id)
        .def_readwrite("end_trade_id", &okx_data_processor::ExtremeLine::end_trade_id);

    // Bind OrderBookVolumes struct
    py::class_<okx_data_processor::OrderBookVolumes>(m, "OrderBookVolumes")
        .def(py::init<>())
        .def(py::init<int32_t, int32_t, double, int64_t, double>(),
             py::arg("width"), py::arg("height"), py::arg("scale"), 
             py::arg("min_trade_id"), py::arg("min_price"))
        .def_readwrite("asks_array", &okx_data_processor::OrderBookVolumes::asks_array)
        .def_readwrite("bids_array", &okx_data_processor::OrderBookVolumes::bids_array)
        .def_readwrite("width", &okx_data_processor::OrderBookVolumes::width)
        .def_readwrite("height", &okx_data_processor::OrderBookVolumes::height)
        .def_readwrite("scale", &okx_data_processor::OrderBookVolumes::scale)
        .def_readwrite("min_trade_id", &okx_data_processor::OrderBookVolumes::min_trade_id)
        .def_readwrite("min_price", &okx_data_processor::OrderBookVolumes::min_price);

    // Bind VelocityData struct
    py::class_<okx_data_processor::VelocityData>(m, "VelocityData")
        .def(py::init<>())
        .def(py::init<const std::string&>(), py::arg("interval"))
        .def_readwrite("velocity_values", &okx_data_processor::VelocityData::velocity_values)
        .def_readwrite("interval", &okx_data_processor::VelocityData::interval);

    // Bind ProcessingResult struct
    py::class_<okx_data_processor::ProcessingResult>(m, "ProcessingResult")
        .def(py::init<>())
        .def(py::init<bool, const std::string&, double>(),
             py::arg("success"), py::arg("error_message"), py::arg("processing_time_seconds"))
        .def_readwrite("success", &okx_data_processor::ProcessingResult::success)
        .def_readwrite("error_message", &okx_data_processor::ProcessingResult::error_message)
        .def_readwrite("processing_time_seconds", &okx_data_processor::ProcessingResult::processing_time_seconds);

    // Bind BollingerBandsProcessor class
    py::class_<okx_data_processor::BollingerBandsProcessor>(m, "BollingerBandsProcessor")
        .def(py::init<>())
        .def(py::init<int32_t, double>(), py::arg("period"), py::arg("std_dev_multiplier") = 2.0)
        .def("calculate", &okx_data_processor::BollingerBandsProcessor::calculate,
             py::arg("prices"))
        .def("calculate_from_trades", &okx_data_processor::BollingerBandsProcessor::calculate_from_trades,
             py::arg("trades"))
        .def("set_parameters", &okx_data_processor::BollingerBandsProcessor::set_parameters,
             py::arg("period"), py::arg("std_dev_multiplier") = 2.0)
        .def("get_parameters", &okx_data_processor::BollingerBandsProcessor::get_parameters)
        .def("has_enough_data", &okx_data_processor::BollingerBandsProcessor::has_enough_data,
             py::arg("data_size"));

    // Bind CandlesProcessor class
    py::class_<okx_data_processor::CandlesProcessor>(m, "CandlesProcessor")
        .def(py::init<>())
        .def("process_trades", &okx_data_processor::CandlesProcessor::process_trades,
             py::arg("symbol_id"), py::arg("trades"))
        .def("process_trades_for_interval", &okx_data_processor::CandlesProcessor::process_trades_for_interval,
             py::arg("symbol_id"), py::arg("trades"), py::arg("interval_name"))
        .def("add_interval", &okx_data_processor::CandlesProcessor::add_interval,
             py::arg("interval_name"), py::arg("duration_ms"))
        .def("get_configured_intervals", &okx_data_processor::CandlesProcessor::get_configured_intervals)
        .def("set_min_trade_id", &okx_data_processor::CandlesProcessor::set_min_trade_id,
             py::arg("symbol_id"), py::arg("interval_name"), py::arg("min_trade_id"))
        .def("get_min_trade_id", &okx_data_processor::CandlesProcessor::get_min_trade_id,
             py::arg("symbol_id"), py::arg("interval_name"));

    // Bind RSICalculator class
    py::class_<okx_data_processor::RSICalculator>(m, "RSICalculator")
        .def(py::init<>())
        .def(py::init<int32_t>(), py::arg("period"))
        .def("calculate", &okx_data_processor::RSICalculator::calculate,
             py::arg("prices"))
        .def("calculate_from_trades", &okx_data_processor::RSICalculator::calculate_from_trades,
             py::arg("trades"))
        .def("calculate_from_candles", &okx_data_processor::RSICalculator::calculate_from_candles,
             py::arg("candles"))
        .def("set_period", &okx_data_processor::RSICalculator::set_period,
             py::arg("period"))
        .def("get_period", &okx_data_processor::RSICalculator::get_period)
        .def("has_enough_data", &okx_data_processor::RSICalculator::has_enough_data,
             py::arg("data_size"))
        .def_static("calculate_smoothed_ma", &okx_data_processor::RSICalculator::calculate_smoothed_ma,
                    py::arg("values"), py::arg("period"));

    // Bind SmoothingProcessor class
    py::class_<okx_data_processor::SmoothingProcessor>(m, "SmoothingProcessor")
        .def(py::init<>())
        .def("process_smoothed_data", &okx_data_processor::SmoothingProcessor::process_smoothed_data,
             py::arg("symbol_id"), py::arg("trades"))
        .def("process_level_data", &okx_data_processor::SmoothingProcessor::process_level_data,
             py::arg("symbol_id"), py::arg("trades"), py::arg("level"))
        .def("process_smoothed_data_points", &okx_data_processor::SmoothingProcessor::process_smoothed_data_points,
             py::arg("symbol_id"), py::arg("trades"))
        .def("process_level_data_points", &okx_data_processor::SmoothingProcessor::process_level_data_points,
             py::arg("symbol_id"), py::arg("trades"), py::arg("level"))
        .def("add_smoothing_level", &okx_data_processor::SmoothingProcessor::add_smoothing_level,
             py::arg("level_name"), py::arg("level_number"))
        .def("get_configured_levels", &okx_data_processor::SmoothingProcessor::get_configured_levels)
        .def("set_min_trade_id", &okx_data_processor::SmoothingProcessor::set_min_trade_id,
             py::arg("symbol_id"), py::arg("level_name"), py::arg("min_trade_id"))
        .def("get_min_trade_id", &okx_data_processor::SmoothingProcessor::get_min_trade_id,
             py::arg("symbol_id"), py::arg("level_name"));

    // Bind ExtremeLinesProcessor class
    py::class_<okx_data_processor::ExtremeLinesProcessor>(m, "ExtremeLinesProcessor")
        .def(py::init<>())
        .def("process_extreme_lines", &okx_data_processor::ExtremeLinesProcessor::process_extreme_lines,
             py::arg("symbol_id"), py::arg("smoothed_lines"), py::arg("trades"))
        .def("create_extreme_lines_array", &okx_data_processor::ExtremeLinesProcessor::create_extreme_lines_array,
             py::arg("extreme_lines"), py::arg("trades"), py::arg("width"), py::arg("height"))
        .def("calculate_array_dimensions", &okx_data_processor::ExtremeLinesProcessor::calculate_array_dimensions,
             py::arg("trades"), py::arg("height") = 100)
        .def("calculate_scale_factor", &okx_data_processor::ExtremeLinesProcessor::calculate_scale_factor,
             py::arg("trades"), py::arg("width"), py::arg("height"))
        .def("set_processing_params", &okx_data_processor::ExtremeLinesProcessor::set_processing_params,
             py::arg("params"))
        .def("get_ranges", &okx_data_processor::ExtremeLinesProcessor::get_ranges,
             py::arg("trades"));

    // Bind OrderBookProcessor class
    py::class_<okx_data_processor::OrderBookProcessor>(m, "OrderBookProcessor")
        .def(py::init<>())
        .def("process_order_book_volumes", &okx_data_processor::OrderBookProcessor::process_order_book_volumes,
             py::arg("symbol_id"), py::arg("trades"))
        .def("process_order_book_volumes_with_snapshots", &okx_data_processor::OrderBookProcessor::process_order_book_volumes_with_snapshots,
             py::arg("symbol_id"), py::arg("trades"), py::arg("order_book_data"))
        .def("calculate_array_dimensions", &okx_data_processor::OrderBookProcessor::calculate_array_dimensions,
             py::arg("trades"), py::arg("height") = 100)
        .def("calculate_scale_factor", &okx_data_processor::OrderBookProcessor::calculate_scale_factor,
             py::arg("trades"), py::arg("width"), py::arg("height"))
        .def("set_processing_params", &okx_data_processor::OrderBookProcessor::set_processing_params,
             py::arg("params"))
        .def("get_processing_params", &okx_data_processor::OrderBookProcessor::get_processing_params);

    // Bind OrderBookSnapshot struct
    py::class_<okx_data_processor::OrderBookSnapshot>(m, "OrderBookSnapshot")
        .def(py::init<>())
        .def(py::init<int64_t, const std::vector<std::pair<double, double>>&, const std::vector<std::pair<double, double>>&>(),
             py::arg("timestamp_ms"), py::arg("asks"), py::arg("bids"))
        .def_readwrite("timestamp_ms", &okx_data_processor::OrderBookSnapshot::timestamp_ms)
        .def_readwrite("asks", &okx_data_processor::OrderBookSnapshot::asks)
        .def_readwrite("bids", &okx_data_processor::OrderBookSnapshot::bids);

    // Bind VelocityCalculator class
    py::class_<okx_data_processor::VelocityCalculator>(m, "VelocityCalculator")
        .def(py::init<>())
        .def("calculate_velocity_from_candles", &okx_data_processor::VelocityCalculator::calculate_velocity_from_candles,
             py::arg("symbol_id"), py::arg("candles"), py::arg("interval"))
        .def("calculate_velocity_from_trades", &okx_data_processor::VelocityCalculator::calculate_velocity_from_trades,
             py::arg("symbol_id"), py::arg("trades"), py::arg("interval"))
        .def("calculate_velocity_for_intervals", &okx_data_processor::VelocityCalculator::calculate_velocity_for_intervals,
             py::arg("symbol_id"), py::arg("candles_map"))
        .def("add_interval", &okx_data_processor::VelocityCalculator::add_interval,
             py::arg("interval_name"), py::arg("duration_ms"))
        .def("get_configured_intervals", &okx_data_processor::VelocityCalculator::get_configured_intervals)
        .def("set_calculation_params", &okx_data_processor::VelocityCalculator::set_calculation_params,
             py::arg("params"))
        .def("get_calculation_params", &okx_data_processor::VelocityCalculator::get_calculation_params);

    // Bind DataProcessor class
    py::class_<okx_data_processor::DataProcessor>(m, "DataProcessor")
        .def(py::init<>())
        .def("process_trades_data", &okx_data_processor::DataProcessor::process_trades_data,
             py::arg("symbol_id"), py::arg("polars_dataframe"))
        .def("process_trades_data_async", &okx_data_processor::DataProcessor::process_trades_data_async,
             py::arg("symbol_id"), py::arg("polars_dataframe"), py::arg("callback"))
        .def("get_processing_stats", &okx_data_processor::DataProcessor::get_processing_stats)
        .def("reset_stats", &okx_data_processor::DataProcessor::reset_stats)
        .def("set_processing_params", &okx_data_processor::DataProcessor::set_processing_params,
             py::arg("params"))
        .def("save_results_to_redis", &okx_data_processor::DataProcessor::save_results_to_redis,
             py::arg("symbol_id"), py::arg("data_type"), py::arg("data"), py::arg("additional_params") = py::dict())
        .def("is_redis_connected", &okx_data_processor::DataProcessor::is_redis_connected);

    // Bind DataConverter class
    py::class_<okx_data_processor::DataConverter>(m, "DataConverter")
        .def_static("from_polars_trades", &okx_data_processor::DataConverter::from_polars_trades,
                    py::arg("polars_dataframe"))
        .def_static("to_polars_candles", &okx_data_processor::DataConverter::to_polars_candles,
                    py::arg("candles"))
        .def_static("to_polars_bollinger", &okx_data_processor::DataConverter::to_polars_bollinger,
                    py::arg("bollinger"))
        .def_static("to_polars_rsi", &okx_data_processor::DataConverter::to_polars_rsi,
                    py::arg("rsi"))
        .def_static("to_polars_smoothed_lines", &okx_data_processor::DataConverter::to_polars_smoothed_lines,
                    py::arg("lines"))
        .def_static("to_polars_smoothed_data", &okx_data_processor::DataConverter::to_polars_smoothed_data,
                    py::arg("data_points"))
        .def_static("to_polars_extreme_lines", &okx_data_processor::DataConverter::to_polars_extreme_lines,
                    py::arg("lines"))
        .def_static("to_numpy_extreme_lines_array", &okx_data_processor::DataConverter::to_numpy_extreme_lines_array,
                    py::arg("array"))
        .def_static("to_polars_order_book_volumes", &okx_data_processor::DataConverter::to_polars_order_book_volumes,
                    py::arg("volumes"))
        .def_static("to_polars_velocity", &okx_data_processor::DataConverter::to_polars_velocity,
                    py::arg("velocity"))
        .def_static("from_python_candles", &okx_data_processor::DataConverter::from_python_candles,
                    py::arg("candles_df"))
        .def_static("from_python_bollinger", &okx_data_processor::DataConverter::from_python_bollinger,
                    py::arg("bollinger_df"))
        .def_static("from_python_rsi", &okx_data_processor::DataConverter::from_python_rsi,
                    py::arg("rsi_df"))
        .def_static("from_python_smoothed", &okx_data_processor::DataConverter::from_python_smoothed,
                    py::arg("lines_df"))
        .def_static("from_python_extreme_lines", &okx_data_processor::DataConverter::from_python_extreme_lines,
                    py::arg("lines_df"))
        .def_static("from_python_order_book_volumes", &okx_data_processor::DataConverter::from_python_order_book_volumes,
                    py::arg("volumes_df"))
        .def_static("from_python_velocity", &okx_data_processor::DataConverter::from_python_velocity,
                    py::arg("velocity_df"));

    // Add module-level functions
    m.def("create_data_processor", []() {
        return std::make_unique<okx_data_processor::DataProcessor>();
    });

    m.def("create_bollinger_processor", [](int32_t period, double std_dev_multiplier) {
        return std::make_unique<okx_data_processor::BollingerBandsProcessor>(period, std_dev_multiplier);
    });

    m.def("create_candles_processor", []() {
        return std::make_unique<okx_data_processor::CandlesProcessor>();
    });
}
