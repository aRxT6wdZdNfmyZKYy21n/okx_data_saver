#include "processors/main_loop.h"
#include "utils/logger.h"
#include <iostream>
#include <csignal>
#include <chrono>
#include <thread>
#include <stdexcept>
#include <iomanip>
#include <ctime>

namespace okx {
namespace processors {

// Глобальная переменная для обработки сигналов
static MainLoop* g_main_loop = nullptr;

// Обработчик сигналов
void signalHandler(int signal) {
    if (g_main_loop) {
        LOG_INFO("Received signal {}, shutting down gracefully...", signal);
        g_main_loop->stop();
    }
}

MainLoop::MainLoop(const utils::Config& config)
    : config_(config)
    , running_(false)
    , should_stop_(false) {
    
    // Инициализируем компоненты
    db_connection_ = std::make_unique<database::DatabaseConnection>(config.getDatabaseConfig());
    calculator_ = std::make_unique<DataSetCalculator>();
    
    // Настраиваем обработчики сигналов
    setupSignalHandlers();
}

MainLoop::~MainLoop() {
    stop();
}

void MainLoop::start() {
    if (running_) {
        LOG_ERROR("Main loop is already running");
        return;
    }
    
    LOG_INFO("Starting main loop...");
    
    // Подключаемся к базе данных
    try {
        db_connection_->connect();
        LOG_INFO("Connected to database successfully");
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to connect to database: {}", e.what());
        throw;
    }
    
    // Запускаем основной поток
    running_ = true;
    should_stop_ = false;
    main_thread_ = std::thread(&MainLoop::mainLoop, this);
    
    LOG_INFO("Main loop started successfully");
}

void MainLoop::stop() {
    if (!running_) {
        return;
    }
    
    LOG_INFO("Stopping main loop...");
    
    should_stop_ = true;
    
    if (main_thread_.joinable()) {
        main_thread_.join();
    }
    
    running_ = false;
    
    // Отключаемся от базы данных
    try {
        db_connection_->disconnect();
        LOG_INFO("Disconnected from database");
    } catch (const std::exception& e) {
        LOG_ERROR("Error disconnecting from database: {}", e.what());
    }
    
    LOG_INFO("Main loop stopped");
}

void MainLoop::setupSignalHandlers() {
    g_main_loop = this;
    
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
}

void MainLoop::mainLoop() {
    LOG_INFO("Main loop thread started");
    
    while (!should_stop_) {
        try {
            processCycle();
        } catch (const std::exception& e) {
            LOG_ERROR("Error in main loop cycle: {}", e.what());
            
            // Небольшая пауза перед повторной попыткой
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        
        // Пауза между циклами
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.getProcessingInterval()));
    }
    
    LOG_INFO("Main loop thread finished");
}

void MainLoop::processCycle() {
    LOG_DEBUG("Processing cycle started");
    
    // Получаем список символов для обработки
    auto symbols = getSymbolsToProcess();
    
    if (symbols.empty()) {
        LOG_DEBUG("No symbols to process");
        return;
    }
    
    // Обрабатываем каждый символ
    for (const auto& symbol_id : symbols) {
        if (should_stop_) {
            break;
        }
        
        try {
            processSymbol(symbol_id);
        } catch (const std::exception& e) {
            LOG_ERROR("Error processing symbol {}: {}", symbol_id, e.what());
        }
    }
    
    LOG_DEBUG("Processing cycle completed");
}

void MainLoop::processSymbol(const std::string& symbol_id) {
    LOG_DEBUG("Processing symbol: {}", symbol_id);
    
    // Конвертируем строку в SymbolId
    SymbolId symbol_id_enum;
    if (symbol_id == "BTC_USDT") {
        symbol_id_enum = SymbolId::BTC_USDT;
    } else if (symbol_id == "ETH_USDT") {
        symbol_id_enum = SymbolId::ETH_USDT;
    } else if (symbol_id == "SOL_USDT") {
        symbol_id_enum = SymbolId::SOL_USDT;
    } else {
        LOG_ERROR("Unknown symbol: {}", symbol_id);
        return;
    }
    
    // Получаем последнюю запись финального датасета
    auto last_record = db_connection_->getLastFinalDataSetRecord(symbol_id_enum);
    
    // Определяем временной диапазон для поиска данных
    auto end_time = std::chrono::system_clock::now();
    auto start_time = last_record ? 
        std::chrono::system_clock::from_time_t(last_record->end_timestamp_ms / 1000) : 
        end_time - std::chrono::hours(24); // Если нет записей, берем последние 24 часа
    
    // Получаем снимки order book
    auto order_book_snapshots = db_connection_->getOrderBookSnapshots(
        symbol_id, start_time, end_time, 2);
    
    if (order_book_snapshots.empty()) {
        LOG_DEBUG("No order book snapshots found for symbol: {}", symbol_id);
        return;
    }
    
    // Получаем сделки
    auto trades = db_connection_->getTrades(symbol_id, start_time, end_time);
    
    // Рассчитываем финальный датасет
    int32_t data_set_idx = last_record ? last_record->data_set_idx + 1 : 1;
    auto final_records = calculator_->calculateFinalDataSet(
        symbol_id_enum,
        order_book_snapshots,
        trades,
        data_set_idx
    );
    
    // Сохраняем результаты
    for (const auto& record : final_records) {
        db_connection_->saveFinalDataSetRecord(record);
    }
    
    LOG_INFO("Processed symbol {} - trades: {}, snapshots: {}", 
             symbol_id, trades.size(), order_book_snapshots.size());
}

std::vector<std::string> MainLoop::getSymbolsToProcess() {
    // Для простоты возвращаем фиксированный список символов
    // В реальном приложении это может быть запрос к базе данных
    return {"BTC_USDT", "ETH_USDT"};
}

void MainLoop::shutdown() {
    LOG_INFO("Shutting down...");
    stop();
}

void MainLoop::handleError(const std::string& error_message) {
    LOG_ERROR("{}", error_message);
    
    // В реальном приложении здесь может быть логика для:
    // - Отправки уведомлений
    // - Записи в лог-файл
    // - Перезапуска компонентов
}

} // namespace processors
} // namespace okx
