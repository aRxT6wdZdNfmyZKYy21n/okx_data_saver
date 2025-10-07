#include "utils/logger.h"
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/async.h>
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <iostream>
#include <map>

namespace okx {
namespace utils {

Logger& Logger::getInstance() {
    static Logger instance;
    return instance;
}

void Logger::initialize(const std::string& log_level, 
                       const std::string& log_file,
                       bool enable_console,
                       bool enable_file) {
    if (initialized_) {
        return;
    }
    
    try {
        // Создаем директорию для логов если не существует и файл не пустой
        if (!log_file.empty()) {
            std::filesystem::path log_path(log_file);
            std::filesystem::create_directories(log_path.parent_path());
        }
        
        // Настраиваем асинхронное логирование
        spdlog::init_thread_pool(8192, 1);
        
        std::vector<spdlog::sink_ptr> sinks;
        
        // Консольный вывод
        if (enable_console) {
            auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            console_sink->set_level(spdlog::level::trace);
            console_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
            sinks.push_back(console_sink);
        }
        
        // Файловый вывод с ротацией
        if (enable_file) {
            auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                log_file, 1024 * 1024 * 10, 5); // 10MB, 5 файлов
            file_sink->set_level(spdlog::level::trace);
            file_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%t] %v");
            sinks.push_back(file_sink);
        }
        
        // Создаем логгер
        logger_ = std::make_shared<spdlog::async_logger>(
            "okx_final_data_set_saver", 
            sinks.begin(), 
            sinks.end(), 
            spdlog::thread_pool(), 
            spdlog::async_overflow_policy::block);
        
        // Устанавливаем уровень логирования
        setLevel(log_level);
        
        // Регистрируем логгер
        spdlog::register_logger(logger_);
        
        initialized_ = true;
        
        LOG_INFO("Logger initialized successfully");
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize logger: " << e.what() << std::endl;
        throw;
    }
}

void Logger::setLevel(const std::string& level) {
    if (!logger_) {
        return;
    }
    
    auto spdlog_level = stringToLevel(level);
    logger_->set_level(spdlog_level);
}

void Logger::logStructured(const std::string& level, 
                          const std::string& message,
                          const std::map<std::string, std::string>& fields) {
    if (!logger_) {
        return;
    }
    
    std::string json_log = createJsonLog(level, message, fields);
    
    auto spdlog_level = stringToLevel(level);
    logger_->log(spdlog_level, json_log);
}

void Logger::logPerformance(const std::string& operation, 
                           double duration_ms,
                           const std::map<std::string, std::string>& metadata) {
    if (!logger_) {
        return;
    }
    
    std::map<std::string, std::string> perf_fields = {
        {"operation", operation},
        {"duration_ms", std::to_string(duration_ms)},
        {"type", "performance"}
    };
    
    // Добавляем метаданные
    for (const auto& [key, value] : metadata) {
        perf_fields[key] = value;
    }
    
    logStructured("INFO", "Performance metric", perf_fields);
}

void Logger::shutdown() {
    if (logger_) {
        logger_->flush();
        spdlog::shutdown();
        logger_.reset();
        initialized_ = false;
    }
}

spdlog::level::level_enum Logger::stringToLevel(const std::string& level) {
    if (level == "TRACE") return spdlog::level::trace;
    if (level == "DEBUG") return spdlog::level::debug;
    if (level == "INFO") return spdlog::level::info;
    if (level == "WARN") return spdlog::level::warn;
    if (level == "ERROR") return spdlog::level::err;
    if (level == "CRITICAL") return spdlog::level::critical;
    
    return spdlog::level::info; // По умолчанию
}

std::string Logger::createJsonLog(const std::string& level,
                                 const std::string& message,
                                 const std::map<std::string, std::string>& fields) {
    std::ostringstream json;
    json << "{";
    
    // Временная метка
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    json << "\"timestamp\":\"" << std::put_time(std::gmtime(&time_t), "%Y-%m-%dT%H:%M:%S");
    json << "." << std::setfill('0') << std::setw(3) << ms.count() << "Z\"";
    
    // Уровень логирования
    json << ",\"level\":\"" << level << "\"";
    
    // Сообщение
    json << ",\"message\":\"" << message << "\"";
    
    // Дополнительные поля
    if (!fields.empty()) {
        json << ",\"fields\":{";
        bool first = true;
        for (const auto& [key, value] : fields) {
            if (!first) json << ",";
            json << "\"" << key << "\":\"" << value << "\"";
            first = false;
        }
        json << "}";
    }
    
    json << "}";
    return json.str();
}

} // namespace utils
} // namespace okx
