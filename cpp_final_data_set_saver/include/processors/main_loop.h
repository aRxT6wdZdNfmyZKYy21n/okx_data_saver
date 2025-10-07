#pragma once

#include "data_structures.h"
#include "database/database_connection.h"
#include "processors/data_set_calculator.h"
#include "utils/config.h"
#include <atomic>
#include <thread>
#include <memory>
#include <vector>
#include <string>

namespace okx {
namespace processors {

class MainLoop {
public:
    explicit MainLoop(const utils::Config& config);
    ~MainLoop();
    
    // Запуск основного цикла
    void start();
    void stop();
    
    // Проверка состояния
    bool isRunning() const { return running_; }
    
    // Обработка сигналов
    void setupSignalHandlers();

private:
    const utils::Config& config_;
    std::unique_ptr<database::DatabaseConnection> db_connection_;
    std::unique_ptr<DataSetCalculator> calculator_;
    
    std::atomic<bool> running_;
    std::atomic<bool> should_stop_;
    std::thread main_thread_;
    
    // Основной цикл обработки
    void mainLoop();
    
    // Обработка одного цикла
    void processCycle();
    
    // Обработка символа
    void processSymbol(const std::string& symbol_id);
    
    // Получение списка символов для обработки
    std::vector<std::string> getSymbolsToProcess();
    
    // Graceful shutdown
    void shutdown();
    
    // Обработка ошибок
    void handleError(const std::string& error_message);
};

} // namespace processors
} // namespace okx
