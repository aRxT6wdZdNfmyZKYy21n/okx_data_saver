#pragma once

#include "data_structures.h"
#include <string>
#include <stdexcept>

namespace okx {
namespace utils {

class Config {
public:
    Config();
    ~Config() = default;
    
    // Получение конфигурации базы данных
    const DatabaseConfig& getDatabaseConfig() const { return db_config_; }
    
    // Получение настроек приложения
    int getProcessingInterval() const { return processing_interval_; }
    int getMaxRetries() const { return max_retries_; }
    std::string getLogLevel() const { return log_level_; }
    
    // Валидация конфигурации
    void validate() const;

private:
    DatabaseConfig db_config_;
    int processing_interval_;
    int max_retries_;
    std::string log_level_;
    
    // Загрузка .env файла
    void loadEnvFile();
    
    // Чтение переменной окружения
    std::string getEnvVar(const std::string& key);
    
    // Парсинг .env файла
    void parseEnvFile(const std::string& filepath);
};

} // namespace utils
} // namespace okx
