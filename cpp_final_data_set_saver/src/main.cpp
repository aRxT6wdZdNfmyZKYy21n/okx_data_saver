#include "utils/config.h"
#include "utils/logger.h"
#include "processors/main_loop.h"
#include <iostream>
#include <exception>
#include <stdexcept>
#include <thread>
#include <chrono>

int main(int /*argc*/, char* /*argv*/[]) {
    try {
        // Инициализируем логгер
        ::okx::utils::Logger::getInstance().initialize("INFO", "logs/okx_final_data_set_saver.log");
        
        LOG_INFO("OKX Final Data Set Saver v1.0.0 starting...");
        
        // Загружаем конфигурацию
        ::okx::utils::Config config;
        LOG_INFO("Configuration loaded successfully");
        
        // Создаем и запускаем основной цикл
        ::okx::processors::MainLoop main_loop(config);
        
        LOG_INFO("Press Ctrl+C to stop the application");
        
        // Запускаем основной цикл
        main_loop.start();
        
        // Ждем завершения
        while (main_loop.isRunning()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        LOG_INFO("Application stopped successfully");
        
        // Очищаем ресурсы логгера
        ::okx::utils::Logger::getInstance().shutdown();
        
        return 0;
        
    } catch (const std::exception& e) {
        LOG_ERROR("Fatal error: {}", e.what());
        ::okx::utils::Logger::getInstance().shutdown();
        return 1;
    } catch (...) {
        LOG_ERROR("Unknown fatal error occurred");
        ::okx::utils::Logger::getInstance().shutdown();
        return 1;
    }
}
