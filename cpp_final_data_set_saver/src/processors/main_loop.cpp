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
    LOG_INFO("Received signal {}, terminating immediately...", signal);
    std::exit(0);
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
    // Агрессивное завершение - не ждем graceful shutdown
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
    running_ = false;
    
    LOG_INFO("Main loop stop requested");
}

void MainLoop::setupSignalHandlers() {
    g_main_loop = this;
    
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    // SIGKILL нельзя перехватить, но добавим для полноты
    std::signal(SIGQUIT, signalHandler);
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
    int64_t min_timestamp_ms = last_record ? last_record->end_timestamp_ms : 0;
    int32_t new_data_set_idx = last_record ? last_record->data_set_idx + 1 : 0;
    
    // Получаем 2 снапшота order book (как в Python коде)
    auto order_book_snapshots = db_connection_->getOrderBookSnapshots(
        symbol_id, 
        std::chrono::system_clock::from_time_t(min_timestamp_ms / 1000),
        std::chrono::system_clock::now(),
        2);
    
    if (order_book_snapshots.size() < 2) {
        LOG_INFO("There are only {} order book snapshots; skipping final data set saving.", 
                 order_book_snapshots.size());
        return;
    }
    
    // Берем первый и второй снапшоты
    const auto& start_snapshot = order_book_snapshots[0];
    const auto& end_snapshot = order_book_snapshots[1];
    
    int64_t start_timestamp_ms = start_snapshot.timestamp_ms;
    int64_t end_timestamp_ms = end_snapshot.timestamp_ms;
    
    LOG_INFO("Start order book snapshot timestamp (ms): {}; end order book snapshot timestamp (ms): {}", 
             start_timestamp_ms, end_timestamp_ms);
    
    // Получаем обновления order book между снапшотами
    auto order_book_updates = db_connection_->getOrderBookUpdates(
        symbol_id, start_timestamp_ms, end_timestamp_ms);
    
    LOG_INFO("Fetched {} order book updates", order_book_updates.size());
    
    // Получаем сделки между снапшотами
    auto trades = db_connection_->getTrades(
        symbol_id, 
        std::chrono::system_clock::from_time_t(start_timestamp_ms / 1000),
        std::chrono::system_clock::from_time_t(end_timestamp_ms / 1000));
    
    LOG_INFO("Fetched {} trades", trades.size());
    
    // Объединяем снапшоты и обновления (как в Python коде)
    std::vector<OrderBookSnapshot> all_order_books;
    all_order_books.push_back(start_snapshot);
    all_order_books.insert(all_order_books.end(), order_book_updates.begin(), order_book_updates.end());
    
    // Рассчитываем финальный датасет
    auto final_records = calculator_->calculateFinalDataSet(
        symbol_id_enum,
        all_order_books,
        trades,
        new_data_set_idx
    );
    
    // Сохраняем результаты
    for (const auto& record : final_records) {
        db_connection_->saveFinalDataSetRecord(record);
    }
    
    LOG_INFO("Processed symbol {} - trades: {}, snapshots: {}, updates: {}", 
             symbol_id, trades.size(), order_book_snapshots.size(), order_book_updates.size());
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
