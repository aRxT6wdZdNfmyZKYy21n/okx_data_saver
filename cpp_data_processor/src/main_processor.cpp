#include "main_processor.h"
#include "bollinger_bands.h"
#include "candles_processor.h"
#include "rsi_calculator.h"
#include "smoothing_processor.h"
#include "extreme_lines_processor.h"
#include "order_book_processor.h"
#include "velocity_calculator.h"
#include "redis_client.h"
#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>
#include <chrono>
#include <thread>
#include <future>
#include <sstream>
#include <algorithm>  // C++17: For std::all_of, std::any_of, etc.
#include <numeric>    // C++17: For std::reduce, std::transform_reduce
#include <execution>  // C++17: For parallel algorithms

namespace okx_data_processor {

DataProcessor::DataProcessor() {
    initialize_components();
}

void DataProcessor::initialize_components() {
    bollinger_processor_ = std::make_unique<BollingerBandsProcessor>();
    candles_processor_ = std::make_unique<CandlesProcessor>();
    rsi_calculator_ = std::make_unique<RSICalculator>();
    smoothing_processor_ = std::make_unique<SmoothingProcessor>();
    extreme_lines_processor_ = std::make_unique<ExtremeLinesProcessor>();
    // order_book_processor_ = std::make_unique<OrderBookProcessor>();
    velocity_calculator_ = std::make_unique<VelocityCalculator>();
    
    // Initialize Redis client
    redis_client_ = std::make_unique<RedisClient>();
    redis_client_->initialize();
}

ProcessingResult DataProcessor::process_trades_data(SymbolId symbol_id, const pybind11::object& polars_dataframe) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    try {
        // C++17: Use auto for type deduction
        auto trades = DataConverter::from_polars_trades(polars_dataframe);
        
        // C++17: Use constexpr validation
        if (trades.empty()) {
            return ProcessingResult::success_result(0.0);
        }
        
        // C++17: Validate trades data using constexpr method
        if (!std::all_of(trades.begin(), trades.end(), [](const auto& trade) {
            return trade.is_valid();
        })) {
            return ProcessingResult::error_result("Invalid trade data detected");
        }
        
        // Process each component
        ProcessingResult bollinger_result = process_bollinger_bands(symbol_id, trades);
        if (!bollinger_result.success) {
            return bollinger_result;
        }
        
        ProcessingResult candles_result = process_candles_data(symbol_id, trades);
        if (!candles_result.success) {
            return candles_result;
        }
        
        // Process other components
        ProcessingResult rsi_result = process_rsi_data(symbol_id, trades);
        if (!rsi_result.success) {
            return rsi_result;
        }
        
        ProcessingResult smoothing_result = process_smoothed_data(symbol_id, trades);
        if (!smoothing_result.success) {
            return smoothing_result;
        }
        
        ProcessingResult extreme_lines_result = process_extreme_lines(symbol_id, trades);
        if (!extreme_lines_result.success) {
            return extreme_lines_result;
        }
        
        // Process remaining components
        /*
        ProcessingResult order_book_result = process_order_book_volumes(symbol_id, trades);
        if (!order_book_result.success) {
            return order_book_result;
        }
        */
        
        ProcessingResult velocity_result = process_velocity_data(symbol_id, trades);
        if (!velocity_result.success) {
            return velocity_result;
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        // Update statistics
        total_trades_processed_ += trades.size();
        total_processing_time_ms_ += duration.count();
        successful_operations_++;
        
        // C++17: Use constexpr calculation
        return ProcessingResult::success_result(duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        const auto end_time = std::chrono::high_resolution_clock::now();
        const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        const auto processing_time = duration.count() / 1000.0;
        
        failed_operations_++;
        
        // C++17: Use string_view for better performance
        return ProcessingResult::error_result(
            std::string("Processing failed: ") + e.what(), 
            processing_time
        );
    }
}

void DataProcessor::process_trades_data_async(SymbolId symbol_id, const pybind11::object& polars_dataframe, 
                                            const pybind11::function& callback) {
    // Run processing in a separate thread
    std::thread([this, symbol_id, polars_dataframe, callback]() {
        try {
            ProcessingResult result = process_trades_data(symbol_id, polars_dataframe);
            callback(result);
        } catch (const std::exception& e) {
            ProcessingResult error_result(false, std::string("Async processing failed: ") + e.what(), 0.0);
            callback(error_result);
        }
    }).detach();
}

pybind11::dict DataProcessor::get_processing_stats() const {
    pybind11::dict stats;
    stats["total_trades_processed"] = total_trades_processed_.load();
    stats["total_processing_time_ms"] = total_processing_time_ms_.load();
    stats["successful_operations"] = successful_operations_.load();
    stats["failed_operations"] = failed_operations_.load();
    
    if (successful_operations_.load() > 0) {
        stats["average_processing_time_ms"] = total_processing_time_ms_.load() / successful_operations_.load();
    } else {
        stats["average_processing_time_ms"] = 0.0;
    }
    
    return stats;
}

void DataProcessor::reset_stats() {
    total_trades_processed_ = 0;
    total_processing_time_ms_ = 0;
    successful_operations_ = 0;
    failed_operations_ = 0;
}

void DataProcessor::set_processing_params(const pybind11::dict& params) {
    try {
        if (params.contains("enable_bollinger_bands")) {
            processing_params_.enable_bollinger_bands = params["enable_bollinger_bands"].cast<bool>();
        }
        if (params.contains("enable_candles")) {
            processing_params_.enable_candles = params["enable_candles"].cast<bool>();
        }
        if (params.contains("enable_rsi")) {
            processing_params_.enable_rsi = params["enable_rsi"].cast<bool>();
        }
        if (params.contains("enable_smoothing")) {
            processing_params_.enable_smoothing = params["enable_smoothing"].cast<bool>();
        }
        if (params.contains("enable_extreme_lines")) {
            processing_params_.enable_extreme_lines = params["enable_extreme_lines"].cast<bool>();
        }
        if (params.contains("enable_order_book_volumes")) {
            processing_params_.enable_order_book_volumes = params["enable_order_book_volumes"].cast<bool>();
        }
        if (params.contains("enable_velocity")) {
            processing_params_.enable_velocity = params["enable_velocity"].cast<bool>();
        }
        if (params.contains("bollinger_period")) {
            processing_params_.bollinger_period = params["bollinger_period"].cast<int32_t>();
        }
        if (params.contains("rsi_period")) {
            processing_params_.rsi_period = params["rsi_period"].cast<int32_t>();
        }
        if (params.contains("candle_intervals")) {
            processing_params_.candle_intervals = params["candle_intervals"].cast<std::vector<std::string>>();
        }
        if (params.contains("smoothing_levels")) {
            processing_params_.smoothing_levels = params["smoothing_levels"].cast<std::vector<std::string>>();
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to set processing parameters: " + std::string(e.what()));
    }
}


ProcessingResult DataProcessor::process_bollinger_bands(SymbolId symbol_id, const std::vector<TradeData>& trades) {
    if (!processing_params_.enable_bollinger_bands) {
        return ProcessingResult::success_result(0.0);
    }
    
    try {
        const auto start_time = std::chrono::high_resolution_clock::now();
        
        // Set parameters
        bollinger_processor_->set_parameters(processing_params_.bollinger_period);
        
        // C++17: Use auto for type deduction
        auto bollinger = bollinger_processor_->calculate_from_trades(trades);
        
        // C++17: Validate bollinger bands data
        if (!bollinger.is_valid()) {
            return ProcessingResult::error_result("Invalid Bollinger Bands data generated");
        }
        
        // Convert to Python object and save to Redis
        auto bollinger_py = DataConverter::to_polars_bollinger(bollinger);
        save_results_to_redis(symbol_id, "bollinger", bollinger_py, pybind11::dict());
        
        const auto end_time = std::chrono::high_resolution_clock::now();
        const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        const auto processing_time = duration.count() / 1000.0;
        
        return ProcessingResult::success_result(processing_time);
        
    } catch (const std::exception& e) {
        return ProcessingResult::error_result(
            std::string("Bollinger Bands processing failed: ") + e.what(), 
            0.0
        );
    }
}

ProcessingResult DataProcessor::process_candles_data(SymbolId symbol_id, const std::vector<TradeData>& trades) {
    if (!processing_params_.enable_candles) {
        return ProcessingResult(true, "Candles processing disabled", 0.0);
    }
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Process candles for all configured intervals
        auto candles_map = candles_processor_->process_trades(symbol_id, trades);
        
        // Convert symbol_id to string for cache key
        std::string symbol_key = std::to_string(static_cast<int>(symbol_id));
        
        // Save each interval's candles to cache and Redis
        for (const auto& pair : candles_map) {
            const std::string& interval_name = pair.first;
            const std::vector<CandleData>& candles = pair.second;
            
            pybind11::object candles_py = DataConverter::to_polars_candles(candles);
            
            // Save to in-memory cache
            processed_data_cache_[symbol_key]["candles_" + interval_name] = candles_py;
            
            // Save to Redis
            pybind11::dict params;
            params["interval"] = interval_name;
            save_results_to_redis(symbol_id, "candles", candles_py, params);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "Candles data processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("Candles processing failed: ") + e.what(), 0.0);
    }
}

ProcessingResult DataProcessor::process_rsi_data(SymbolId symbol_id, const std::vector<TradeData>& trades) {
    if (!processing_params_.enable_rsi) {
        return ProcessingResult(true, "RSI processing disabled", 0.0);
    }
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Set parameters
        rsi_calculator_->set_period(processing_params_.rsi_period);
        
        // Calculate RSI
        RSIData rsi = rsi_calculator_->calculate_from_trades(trades);
        
        // Convert to Python object and save to Redis
        pybind11::object rsi_py = DataConverter::to_polars_rsi(rsi);
        save_results_to_redis(symbol_id, "rsi", rsi_py, pybind11::dict());
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "RSI data processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("RSI processing failed: ") + e.what(), 0.0);
    }
}

ProcessingResult DataProcessor::process_smoothed_data(SymbolId symbol_id, const std::vector<TradeData>& trades) {
    if (!processing_params_.enable_smoothing) {
        return ProcessingResult(true, "Smoothed data processing disabled", 0.0);
    }
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Process smoothed data for all levels
        auto smoothed_map = smoothing_processor_->process_smoothed_data(symbol_id, trades);
        
        // Process smoothed data points for all levels
        auto smoothed_data_points_map = smoothing_processor_->process_smoothed_data_points(symbol_id, trades);
        
        // Save each level's lines data to Redis
        for (const auto& pair : smoothed_map) {
            const std::string& level_name = pair.first;
            const std::vector<SmoothedLine>& lines = pair.second;
            
            pybind11::object lines_py = DataConverter::to_polars_smoothed_lines(lines);
            
            // Save lines data with proper parameters
            pybind11::dict lines_params;
            lines_params["level"] = level_name;
            lines_params["min_trade_id"] = 0; // Default value
            lines_params["max_trade_id"] = 0; // Default value
            save_results_to_redis(symbol_id, "lines", lines_py, lines_params);
        }
        
        // Save each level's smoothed data points to Redis
        for (const auto& pair : smoothed_data_points_map) {
            const std::string& level_name = pair.first;
            const std::vector<SmoothedDataPoint>& data_points = pair.second;
            
            pybind11::object smoothed_py = DataConverter::to_polars_smoothed_data(data_points);
            
            // Save smoothed data with proper parameters
            pybind11::dict smoothed_params;
            smoothed_params["level"] = level_name;
            smoothed_params["min_trade_id"] = 0; // Default value
            smoothed_params["max_trade_id"] = 0; // Default value
            save_results_to_redis(symbol_id, "smoothed", smoothed_py, smoothed_params);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "Smoothed data processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("Smoothed data processing failed: ") + e.what(), 0.0);
    }
}

ProcessingResult DataProcessor::process_extreme_lines(SymbolId symbol_id, const std::vector<TradeData>& trades) {
    if (!processing_params_.enable_extreme_lines) {
        return ProcessingResult(true, "Extreme lines processing disabled", 0.0);
    }
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Get smoothed lines for level 1
        auto smoothed_lines = smoothing_processor_->process_level_data(symbol_id, trades, "Smoothed (1)");
        
        if (smoothed_lines.empty()) {
            return ProcessingResult(true, "No smoothed lines available for extreme lines processing", 0.0);
        }
        
        // Process extreme lines
        auto extreme_lines = extreme_lines_processor_->process_extreme_lines(symbol_id, smoothed_lines, trades);
        
        if (extreme_lines.empty()) {
            return ProcessingResult(true, "No extreme lines generated", 0.0);
        }
        
        // Calculate array dimensions and scale (Python logic)
        auto [price_range, trade_id_range] = extreme_lines_processor_->get_ranges(trades);
        double min_price = price_range.first;
        double max_price = price_range.second;
        int64_t min_trade_id = trade_id_range.first;
        int64_t max_trade_id = trade_id_range.second;
        
        double delta_price = max_price - min_price;
        int64_t delta_trade_id = max_trade_id - min_trade_id;
        
        if (delta_price <= 0 || delta_trade_id <= 0) {
            return ProcessingResult(true, "Invalid price or trade ID range", 0.0);
        }
        
        // Python logic: aspect_ratio = delta_trade_id / delta_price
        double aspect_ratio = static_cast<double>(delta_trade_id) / delta_price;
        int32_t height = 100;
        double extreme_lines_scale = delta_price / height;  // Python logic
        int32_t width = static_cast<int32_t>(height * aspect_ratio);
        
        // Create 2D array (Python logic: numpy.zeros((width, height)))
        std::vector<std::vector<double>> extreme_lines_array(width, std::vector<double>(height, 0.0));
        
        // Fill array with extreme lines (Python logic)
        for (const auto& extreme_line : extreme_lines) {
            int64_t end_trade_id = extreme_line.end_trade_id;
            int64_t start_trade_id = extreme_line.start_trade_id;
            double price = extreme_line.price;
            
            // Python logic: end_x = int((end_trade_id - min_trade_id) / extreme_lines_scale)
            int32_t end_x = static_cast<int32_t>((end_trade_id - min_trade_id) / extreme_lines_scale);
            int32_t start_x = static_cast<int32_t>((start_trade_id - min_trade_id) / extreme_lines_scale);
            int32_t y = std::min(static_cast<int32_t>((price - min_price) / extreme_lines_scale), height - 1);
            
            // Clamp coordinates to array bounds
            start_x = std::max(0, std::min(start_x, width - 1));
            end_x = std::max(0, std::min(end_x, width - 1));
            y = std::max(0, std::min(y, height - 1));
            
            // Python logic: extreme_lines_array[start_x:end_x, y] = numpy.arange(end_x - start_x)
            for (int32_t x = start_x; x < end_x; ++x) {
                extreme_lines_array[x][y] = static_cast<double>(x - start_x);
            }
        }
        
        // Convert to numpy array and save to Redis
        pybind11::object extreme_lines_py = DataConverter::to_numpy_extreme_lines_array(extreme_lines_array);
        
        // Create metadata dict (Python style)
        pybind11::dict metadata;
        metadata["width"] = width;
        metadata["height"] = height;
        metadata["scale"] = extreme_lines_scale;
        metadata["min_trade_id"] = min_trade_id;
        metadata["min_price"] = min_price;
        
        save_results_to_redis(symbol_id, "extreme_lines", extreme_lines_py, metadata);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "Extreme lines processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("Extreme lines processing failed: ") + e.what(), 0.0);
    }
}

/*
ProcessingResult DataProcessor::process_order_book_volumes(SymbolId symbol_id, const std::vector<TradeData>& trades) {
    if (!processing_params_.enable_order_book_volumes) {
        return ProcessingResult(true, "Order book volumes processing disabled", 0.0);
    }
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Process order book volumes
        OrderBookVolumes volumes = order_book_processor_->process_order_book_volumes(symbol_id, trades);
        
        // Convert to Python object and save to Redis
        pybind11::object volumes_py = DataConverter::to_polars_order_book_volumes(volumes);
        save_results_to_redis(symbol_id, "order_book_volumes", volumes_py, pybind11::dict());
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "Order book volumes processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("Order book volumes processing failed: ") + e.what(), 0.0);
    }
}
*/

ProcessingResult DataProcessor::process_velocity_data(SymbolId symbol_id, const std::vector<TradeData>& /* trades */) {
    if (!processing_params_.enable_velocity) {
        return ProcessingResult(true, "Velocity data processing disabled", 0.0);
    }
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Convert symbol_id to string for cache key
        std::string symbol_key = std::to_string(static_cast<int>(symbol_id));
        
        // Match Python implementation: get candles data and use trades_count as velocity
        for (const auto& interval : processing_params_.candle_intervals) {
            // Get candles data from in-memory cache
            std::string cache_key = "candles_" + interval;
            
            if (processed_data_cache_.find(symbol_key) != processed_data_cache_.end() &&
                processed_data_cache_[symbol_key].find(cache_key) != processed_data_cache_[symbol_key].end()) {
                
                pybind11::object candles_data = processed_data_cache_[symbol_key][cache_key];
                
                // Extract trades_count column as velocity (matching Python implementation)
                pybind11::module polars = pybind11::module::import("polars");
                pybind11::object velocity_series = candles_data.attr("get_column")("trades_count");
                
                // Save velocity data to Redis (Redis service expects Series, not DataFrame)
                pybind11::dict velocity_params;
                velocity_params["interval"] = interval;
                save_results_to_redis(symbol_id, "velocity", velocity_series, velocity_params);
            } else {
                pybind11::print("⚠️  No candles data found in cache for interval:", interval);
            }
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "Velocity data processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("Velocity data processing failed: ") + e.what(), 0.0);
    }
}

void DataProcessor::save_results_to_redis(SymbolId symbol_id, const std::string& data_type, const pybind11::object& dataframe, const pybind11::dict& additional_params) {
    try {
        if (!redis_client_ || !redis_client_->is_connected()) {
            pybind11::print("Redis client not connected, skipping save operation");
            return;
        }

        // Use provided additional parameters or create defaults
        pybind11::dict params = additional_params;
        
        // Add default parameters if not provided
        if (data_type == "bollinger" && !params.contains("timeperiod")) {
            params["timeperiod"] = 20; // Default timeperiod
        } else if (data_type == "rsi") {
            if (!params.contains("interval")) {
                params["interval"] = "1m";  // Default interval
            }
            if (!params.contains("timeperiod")) {
                params["timeperiod"] = 14;  // Default timeperiod
            }
        } else if (data_type == "velocity" && !params.contains("interval")) {
            params["interval"] = "1m"; // Default interval
        }
        
        // Save DataFrame to Redis using Python service
        bool success = redis_client_->save_dataframe(symbol_id, data_type, dataframe, params);
        if (success) {
            pybind11::print("✅ Successfully saved", data_type, "DataFrame for symbol", static_cast<int>(symbol_id), "to Redis");
        } else {
            pybind11::print("❌ Failed to save", data_type, "DataFrame for symbol", static_cast<int>(symbol_id), "to Redis");
        }
        
    } catch (const std::exception& e) {
        pybind11::print("❌ Error saving DataFrame to Redis:", e.what());
    }
}

bool DataProcessor::is_redis_connected() const {
    return redis_client_ && redis_client_->is_connected();
}

} // namespace okx_data_processor
