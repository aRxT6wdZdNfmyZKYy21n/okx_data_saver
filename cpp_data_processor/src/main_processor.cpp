#include "main_processor.h"
#include "bollinger_bands.h"
#include "candles_processor.h"
#include "rsi_calculator.h"
#include "smoothing_processor.h"
#include "extreme_lines_processor.h"
#include "order_book_processor.h"
#include "velocity_calculator.h"
#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <chrono>
#include <thread>
#include <future>

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
    order_book_processor_ = std::make_unique<OrderBookProcessor>();
    velocity_calculator_ = std::make_unique<VelocityCalculator>();
}

ProcessingResult DataProcessor::process_trades_data(SymbolId symbol_id, const pybind11::object& trades_df) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    try {
        // Convert Python DataFrame to C++ vector
        std::vector<TradeData> trades = DataConverter::from_python_trades(trades_df);
        
        if (trades.empty()) {
            return ProcessingResult(true, "No trades to process", 0.0);
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
        ProcessingResult order_book_result = process_order_book_volumes(symbol_id, trades);
        if (!order_book_result.success) {
            return order_book_result;
        }
        
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
        
        return ProcessingResult(true, "Processing completed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        failed_operations_++;
        
        return ProcessingResult(false, std::string("Processing failed: ") + e.what(), 
                              duration.count() / 1000.0);
    }
}

void DataProcessor::process_trades_data_async(SymbolId symbol_id, const pybind11::object& trades_df, 
                                            const pybind11::function& callback) {
    // Run processing in a separate thread
    std::thread([this, symbol_id, trades_df, callback]() {
        try {
            ProcessingResult result = process_trades_data(symbol_id, trades_df);
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
        return ProcessingResult(true, "Bollinger Bands processing disabled", 0.0);
    }
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Set parameters
        bollinger_processor_->set_parameters(processing_params_.bollinger_period);
        
        // Calculate Bollinger Bands
        BollingerBands bollinger = bollinger_processor_->calculate_from_trades(trades);
        
        // Convert to Python object and save to Redis
        pybind11::object bollinger_py = DataConverter::to_python_bollinger(bollinger);
        save_results_to_redis(symbol_id, "bollinger_bands", bollinger_py);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "Bollinger Bands processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("Bollinger Bands processing failed: ") + e.what(), 0.0);
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
        
        // Save each interval's candles to Redis
        for (const auto& pair : candles_map) {
            const std::string& interval_name = pair.first;
            const std::vector<CandleData>& candles = pair.second;
            
            pybind11::object candles_py = DataConverter::to_python_candles(candles);
            save_results_to_redis(symbol_id, "candles_" + interval_name, candles_py);
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
        pybind11::object rsi_py = DataConverter::to_python_rsi(rsi);
        save_results_to_redis(symbol_id, "rsi", rsi_py);
        
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
        
        // Save each level's data to Redis
        for (const auto& pair : smoothed_map) {
            const std::string& level_name = pair.first;
            const std::vector<SmoothedLine>& lines = pair.second;
            
            pybind11::object lines_py = DataConverter::to_python_smoothed(lines);
            save_results_to_redis(symbol_id, "smoothed_" + level_name, lines_py);
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
        
        // Convert to Python object and save to Redis
        pybind11::object extreme_lines_py = DataConverter::to_python_extreme_lines(extreme_lines);
        save_results_to_redis(symbol_id, "extreme_lines", extreme_lines_py);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "Extreme lines processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("Extreme lines processing failed: ") + e.what(), 0.0);
    }
}

ProcessingResult DataProcessor::process_order_book_volumes(SymbolId symbol_id, const std::vector<TradeData>& trades) {
    if (!processing_params_.enable_order_book_volumes) {
        return ProcessingResult(true, "Order book volumes processing disabled", 0.0);
    }
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Process order book volumes
        OrderBookVolumes volumes = order_book_processor_->process_order_book_volumes(symbol_id, trades);
        
        // Convert to Python object and save to Redis
        pybind11::object volumes_py = DataConverter::to_python_order_book_volumes(volumes);
        save_results_to_redis(symbol_id, "order_book_volumes", volumes_py);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "Order book volumes processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("Order book volumes processing failed: ") + e.what(), 0.0);
    }
}

ProcessingResult DataProcessor::process_velocity_data(SymbolId symbol_id, const std::vector<TradeData>& trades) {
    if (!processing_params_.enable_velocity) {
        return ProcessingResult(true, "Velocity data processing disabled", 0.0);
    }
    
    try {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // Process velocity for all configured intervals
        auto velocity_map = velocity_calculator_->calculate_velocity_for_intervals(symbol_id, {});
        
        // Also calculate from trades directly
        for (const auto& interval : processing_params_.candle_intervals) {
            VelocityData velocity = velocity_calculator_->calculate_velocity_from_trades(symbol_id, trades, interval);
            
            // Convert to Python object and save to Redis
            pybind11::object velocity_py = DataConverter::to_python_velocity(velocity);
            save_results_to_redis(symbol_id, "velocity_" + interval, velocity_py);
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        return ProcessingResult(true, "Velocity data processed successfully", 
                              duration.count() / 1000.0);
        
    } catch (const std::exception& e) {
        return ProcessingResult(false, std::string("Velocity data processing failed: ") + e.what(), 0.0);
    }
}

void DataProcessor::save_results_to_redis(SymbolId symbol_id, const std::string& data_type, const pybind11::object& data) {
    // TODO: Implement Redis saving
    // This is a placeholder - in the real implementation, this would save to Redis
    // For now, we'll just log that we would save the data
    pybind11::print("Would save", data_type, "for symbol", static_cast<int>(symbol_id), "to Redis");
}

pybind11::object DataProcessor::load_data_from_redis(SymbolId symbol_id, const std::string& data_type) {
    // TODO: Implement Redis loading
    // This is a placeholder - in the real implementation, this would load from Redis
    pybind11::print("Would load", data_type, "for symbol", static_cast<int>(symbol_id), "from Redis");
    return pybind11::none();
}

} // namespace okx_data_processor
