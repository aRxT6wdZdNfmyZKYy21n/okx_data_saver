#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/async.h>
#include <memory>
#include <string>
#include <map>

namespace okx {
namespace utils {

class Logger {
public:
    static Logger& getInstance();
    
    // Инициализация логгера
    void initialize(const std::string& log_level = "INFO", 
                   const std::string& log_file = "logs/okx_final_data_set_saver.log",
                   bool enable_console = true,
                   bool enable_file = true);
    
    // Получение логгера
    std::shared_ptr<spdlog::logger> getLogger() const { return logger_; }
    
    // Установка уровня логирования
    void setLevel(const std::string& level);
    
    // Создание структурированного лога
    void logStructured(const std::string& level, 
                      const std::string& message,
                      const std::map<std::string, std::string>& fields = {});
    
    // Метрики производительности
    void logPerformance(const std::string& operation, 
                       double duration_ms,
                       const std::map<std::string, std::string>& metadata = {});
    
    // Очистка ресурсов
    void shutdown();

private:
    Logger() = default;
    ~Logger() = default;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    
    std::shared_ptr<spdlog::logger> logger_;
    bool initialized_ = false;
    
    // Преобразование строкового уровня в spdlog уровень
    spdlog::level::level_enum stringToLevel(const std::string& level);
    
    // Создание JSON строки для структурированного логирования
    std::string createJsonLog(const std::string& level,
                             const std::string& message,
                             const std::map<std::string, std::string>& fields);
};

// Макросы для удобного использования
#define LOG_TRACE(...) ::okx::utils::Logger::getInstance().getLogger()->trace(__VA_ARGS__)
#define LOG_DEBUG(...) ::okx::utils::Logger::getInstance().getLogger()->debug(__VA_ARGS__)
#define LOG_INFO(...) ::okx::utils::Logger::getInstance().getLogger()->info(__VA_ARGS__)
#define LOG_WARN(...) ::okx::utils::Logger::getInstance().getLogger()->warn(__VA_ARGS__)
#define LOG_ERROR(...) ::okx::utils::Logger::getInstance().getLogger()->error(__VA_ARGS__)
#define LOG_CRITICAL(...) ::okx::utils::Logger::getInstance().getLogger()->critical(__VA_ARGS__)

// Макросы для структурированного логирования
#define LOG_STRUCTURED(level, message, fields) \
    ::okx::utils::Logger::getInstance().logStructured(level, message, fields)

// Макросы для метрик производительности
#define LOG_PERFORMANCE(operation, duration_ms, metadata) \
    ::okx::utils::Logger::getInstance().logPerformance(operation, duration_ms, metadata)

} // namespace utils
} // namespace okx
