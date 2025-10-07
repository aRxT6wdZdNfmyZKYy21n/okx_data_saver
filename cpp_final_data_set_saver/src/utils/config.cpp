#include "utils/config.h"
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <algorithm>
#include <iostream>

namespace okx {
namespace utils {

Config::Config() 
    : processing_interval_(1000)  // 1 секунда по умолчанию
    , max_retries_(3)
    , log_level_("INFO") {
    
    // Загружаем .env файл
    loadEnvFile();
    
    // Читаем конфигурацию базы данных
    db_config_.host = getEnvVar("POSTGRES_DB_HOST_NAME");
    db_config_.port = std::stoi(getEnvVar("POSTGRES_DB_PORT"));
    db_config_.database_name = getEnvVar("POSTGRES_DB_NAME");
    db_config_.username = getEnvVar("POSTGRES_DB_USER_NAME");
    db_config_.password = getEnvVar("POSTGRES_DB_PASSWORD");
    
    // Читаем настройки приложения
    try {
        processing_interval_ = std::stoi(getEnvVar("PROCESSING_INTERVAL_MS"));
    } catch (const std::exception&) {
        // Используем значение по умолчанию
    }
    
    try {
        max_retries_ = std::stoi(getEnvVar("MAX_RETRIES"));
    } catch (const std::exception&) {
        // Используем значение по умолчанию
    }
    
    try {
        log_level_ = getEnvVar("LOG_LEVEL");
    } catch (const std::exception&) {
        // Используем значение по умолчанию
    }
    
    // Валидируем конфигурацию
    validate();
}

void Config::loadEnvFile() {
    // Пробуем загрузить .env файл из текущей директории
    std::string env_file = ".env";
    std::ifstream file(env_file);
    
    if (!file.is_open()) {
        // Пробуем загрузить из config/.env
        env_file = "config/.env";
        file.open(env_file);
    }
    
    if (file.is_open()) {
        parseEnvFile(env_file);
        file.close();
    }
    // Если файл не найден, используем только переменные окружения
}

void Config::parseEnvFile(const std::string& filepath) {
    std::ifstream file(filepath);
    std::string line;
    
    while (std::getline(file, line)) {
        // Пропускаем пустые строки и комментарии
        if (line.empty() || line[0] == '#') {
            continue;
        }
        
        // Ищем знак равенства
        size_t pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        
        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);
        
        // Убираем пробелы
        key.erase(std::remove_if(key.begin(), key.end(), ::isspace), key.end());
        value.erase(std::remove_if(value.begin(), value.end(), ::isspace), value.end());
        
        // Убираем кавычки если есть
        if (value.length() >= 2 && value[0] == '"' && value[value.length()-1] == '"') {
            value = value.substr(1, value.length() - 2);
        }
        
        // Устанавливаем переменную окружения
        setenv(key.c_str(), value.c_str(), 1);
    }
}

std::string Config::getEnvVar(const std::string& key) {
    const char* value = std::getenv(key.c_str());
    if (!value) {
        throw std::runtime_error("Required environment variable not found: " + key);
    }
    return value;
}

void Config::validate() const {
    if (db_config_.host.empty()) {
        throw std::runtime_error("Database host is required");
    }
    
    if (db_config_.port <= 0 || db_config_.port > 65535) {
        throw std::runtime_error("Invalid database port: " + std::to_string(db_config_.port));
    }
    
    if (db_config_.database_name.empty()) {
        throw std::runtime_error("Database name is required");
    }
    
    if (db_config_.username.empty()) {
        throw std::runtime_error("Database username is required");
    }
    
    if (processing_interval_ <= 0) {
        throw std::runtime_error("Processing interval must be positive");
    }
    
    if (max_retries_ < 0) {
        throw std::runtime_error("Max retries must be non-negative");
    }
    
    if (log_level_ != "DEBUG" && log_level_ != "INFO" && 
        log_level_ != "WARN" && log_level_ != "ERROR") {
        throw std::runtime_error("Invalid log level: " + log_level_);
    }
}

} // namespace utils
} // namespace okx
