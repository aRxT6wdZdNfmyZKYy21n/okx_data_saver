#include "redis_client.h"
#include <stdexcept>
#include <iostream>

namespace okx_data_processor {

// Helper function to convert C++ SymbolId to Python SymbolId
pybind11::object convert_symbol_id_to_python(SymbolId symbol_id) {
    try {
        // Import the Python SymbolId enum
        pybind11::module enumerations = pybind11::module::import("enumerations");
        pybind11::object python_symbol_id = enumerations.attr("SymbolId");
        
        // Convert based on the C++ enum value
        switch (symbol_id) {
            case SymbolId::BTC_USDT:
                return python_symbol_id.attr("BTC_USDT");
            case SymbolId::ETH_USDT:
                return python_symbol_id.attr("ETH_USDT");
            default:
                throw std::runtime_error("Unknown SymbolId: " + std::to_string(static_cast<int>(symbol_id)));
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to convert SymbolId to Python: " + std::string(e.what()));
    }
}

RedisClient::RedisClient() : connected_(false) {
}

bool RedisClient::initialize() {
    try {
        // Try to import the Redis service, but don't fail if it's not available
        try {
            pybind11::module redis_service_module = pybind11::module::import("main.process_data.redis_service");
            redis_service_ = redis_service_module.attr("g_redis_data_service");
            
            connected_ = true;
            std::cout << "✅ Redis client initialized successfully using Python RedisDataService" << std::endl;
            return true;
        } catch (const std::exception& import_error) {
            std::cout << "⚠️ Redis service not available (missing settings): " << import_error.what() << std::endl;
            std::cout << "ℹ️ Redis functionality will be disabled" << std::endl;
            connected_ = false;
            return false;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Failed to initialize Redis client: " << e.what() << std::endl;
        connected_ = false;
        return false;
    }
}

bool RedisClient::save_dataframe(SymbolId symbol_id, 
                               const std::string& data_type, 
                               const pybind11::object& dataframe,
                               const pybind11::dict& additional_params) {
    try {
        if (!connected_) {
            std::cerr << "Redis client not connected" << std::endl;
            return false;
        }

        // Convert C++ SymbolId to Python SymbolId
        pybind11::object python_symbol_id = convert_symbol_id_to_python(symbol_id);
        
        // Call appropriate Python method based on data type
        if (data_type == "trades") {
            // Extract parameters for trades data
            int min_trade_id = additional_params.contains("min_trade_id") ? 
                              additional_params["min_trade_id"].cast<int>() : 0;
            int max_trade_id = additional_params.contains("max_trade_id") ? 
                              additional_params["max_trade_id"].cast<int>() : 0;
            double min_price = additional_params.contains("min_price") ? 
                              additional_params["min_price"].cast<double>() : 0.0;
            double max_price = additional_params.contains("max_price") ? 
                              additional_params["max_price"].cast<double>() : 0.0;
            
            // Call Python async method synchronously
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_trades_data")(
                    python_symbol_id, dataframe, min_trade_id, max_trade_id, min_price, max_price));
            
        } else if (data_type == "bollinger") {
            // Extract parameters for bollinger bands
            int timeperiod = additional_params.contains("timeperiod") ? 
                            additional_params["timeperiod"].cast<int>() : 20;
            
            // Extract series from dataframe
            pybind11::object upper_band = dataframe.attr("get_column")("upper_band");
            pybind11::object middle_band = dataframe.attr("get_column")("middle_band");
            pybind11::object lower_band = dataframe.attr("get_column")("lower_band");
            
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_bollinger_data")(
                    python_symbol_id, upper_band, middle_band, lower_band, timeperiod));
            
        } else if (data_type == "rsi") {
            // Extract parameters for RSI
            std::string interval = additional_params.contains("interval") ? 
                                  additional_params["interval"].cast<std::string>() : "1m";
            int timeperiod = additional_params.contains("timeperiod") ? 
                            additional_params["timeperiod"].cast<int>() : 14;
            
            pybind11::object rsi_series = dataframe.attr("get_column")("rsi_values");
            
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_rsi_data")(
                    python_symbol_id, interval, rsi_series, timeperiod));
            
        } else if (data_type == "velocity") {
            // Extract parameters for velocity
            std::string interval = additional_params["interval"].cast<std::string>();
            
            // dataframe is already a Series for velocity, pass it directly
            pybind11::object velocity_series = dataframe;
            
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_velocity_series")(
                    python_symbol_id, interval, velocity_series));
            
        } else if (data_type == "candles") {
            // Extract parameters for candles
            std::string interval = additional_params.contains("interval") ? 
                                  additional_params["interval"].cast<std::string>() : "1m";
            int min_trade_id = additional_params.contains("min_trade_id") ? 
                              additional_params["min_trade_id"].cast<int>() : 0;
            int max_trade_id = additional_params.contains("max_trade_id") ? 
                              additional_params["max_trade_id"].cast<int>() : 0;
            
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_candles_data")(
                    python_symbol_id, interval, dataframe, min_trade_id, max_trade_id));
            
        } else if (data_type == "lines") {
            // Extract level from additional_params
            std::string level = additional_params.contains("level") ? 
                               additional_params["level"].cast<std::string>() : "Raw (0)";
            int min_trade_id = additional_params.contains("min_trade_id") ? 
                              additional_params["min_trade_id"].cast<int>() : 0;
            int max_trade_id = additional_params.contains("max_trade_id") ? 
                              additional_params["max_trade_id"].cast<int>() : 0;
            
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_lines_data")(
                    python_symbol_id, level, dataframe, min_trade_id, max_trade_id));
            
        } else if (data_type == "smoothed") {
            // Extract level from additional_params
            std::string level = additional_params.contains("level") ? 
                               additional_params["level"].cast<std::string>() : "Raw (0)";
            int min_trade_id = additional_params.contains("min_trade_id") ? 
                              additional_params["min_trade_id"].cast<int>() : 0;
            int max_trade_id = additional_params.contains("max_trade_id") ? 
                              additional_params["max_trade_id"].cast<int>() : 0;
            
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_smoothed_data")(
                    python_symbol_id, level, dataframe, min_trade_id, max_trade_id));
            
        } else if (data_type.substr(0, 9) == "smoothed_") {
            // Extract level from data_type (e.g., "smoothed_Smoothed (1)" -> "Smoothed (1)")
            std::string level = data_type.substr(9); // Remove "smoothed_" prefix
            
            // Extract parameters for smoothed data
            int min_trade_id = additional_params.contains("min_trade_id") ? 
                              additional_params["min_trade_id"].cast<int>() : 0;
            int max_trade_id = additional_params.contains("max_trade_id") ? 
                              additional_params["max_trade_id"].cast<int>() : 0;
            
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_smoothed_data")(
                    python_symbol_id, level, dataframe, min_trade_id, max_trade_id));
            
        } else if (data_type == "extreme_lines") {
            // Extract parameters for extreme lines
            int width = additional_params.contains("width") ? 
                       additional_params["width"].cast<int>() : 1000;
            int height = additional_params.contains("height") ? 
                        additional_params["height"].cast<int>() : 1000;
            double scale = additional_params.contains("scale") ? 
                          additional_params["scale"].cast<double>() : 1.0;
            int min_trade_id = additional_params.contains("min_trade_id") ? 
                              additional_params["min_trade_id"].cast<int>() : 0;
            double min_price = additional_params.contains("min_price") ? 
                              additional_params["min_price"].cast<double>() : 0.0;
            
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_extreme_lines_data")(
                    python_symbol_id, dataframe, width, height, scale, min_trade_id, min_price));
            
        } else if (data_type == "order_book_volumes") {
            pybind11::object task = pybind11::module::import("asyncio")
                .attr("create_task")(redis_service_.attr("save_order_book_volumes_data")(
                    python_symbol_id, dataframe));
            
        } else {
            std::cerr << "Unsupported data type: " << data_type << std::endl;
            return false;
        }
        
        std::cout << "✅ Successfully saved " << data_type << " data for symbol " 
                  << static_cast<int>(symbol_id) << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Failed to save " << data_type << " data: " << e.what() << std::endl;
        return false;
    }
}

bool RedisClient::delete_data([[maybe_unused]] SymbolId symbol_id, 
                            const std::string& data_type,
                            [[maybe_unused]] const pybind11::dict& additional_params) {
    try {
        if (!connected_) {
            std::cerr << "Redis client not connected" << std::endl;
            return false;
        }

        // For now, we'll use a simple approach - try to set the data to None
        // In a more sophisticated implementation, we could add delete methods to RedisDataService
        std::cout << "⚠️ Delete operation not implemented yet for " << data_type << std::endl;
        return false;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Failed to delete " << data_type << " data: " << e.what() << std::endl;
        return false;
    }
}

} // namespace okx_data_processor