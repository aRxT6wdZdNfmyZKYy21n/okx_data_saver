#pragma once

#include "data_structures.h"
#include <pqxx/pqxx>
#include <memory>
#include <string>
#include <vector>
#include <chrono>

namespace okx {
namespace database {

class DatabaseConnection {
public:
    explicit DatabaseConnection(const DatabaseConfig& config);
    ~DatabaseConnection();
    
    // Подключение к базе данных
    void connect();
    void disconnect();
    bool isConnected() const;
    
    // Получение подключения для использования в других классах
    pqxx::connection& getConnection();
    
    // Получение последней записи финального датасета
    std::optional<OKXDataSetRecordData> getLastFinalDataSetRecord(SymbolId symbol_id);
    
    // Получение снимков order book
    std::vector<OrderBookSnapshot> getOrderBookSnapshots(
        const std::string& symbol_id,
        const std::chrono::system_clock::time_point& start_time,
        const std::chrono::system_clock::time_point& end_time,
        int limit = 2
    );
    
    // Получение сделок
    std::vector<TradeData> getTrades(
        const std::string& symbol_id,
        const std::chrono::system_clock::time_point& start_time,
        const std::chrono::system_clock::time_point& end_time
    );
    
    // Сохранение финального датасета
    void saveFinalDataSetRecord(const OKXDataSetRecordData& record);
    
    // Получение статистики
    struct TradeStats {
        double min_price;
        double max_price;
        double total_volume;
        int count;
    };
    
    TradeStats calculateTradeStats(
        const std::string& symbol_id,
        const std::chrono::system_clock::time_point& start_time,
        const std::chrono::system_clock::time_point& end_time
    );

private:
    DatabaseConfig config_;
    std::unique_ptr<pqxx::connection> connection_;
    
    // Создание подключения
    void createConnection();
    
    // Выполнение запроса
    pqxx::result executeQuery(const std::string& query);
    
    // Преобразование timestamp
    int64_t timestampToMs(const std::chrono::system_clock::time_point& timestamp);
    std::chrono::system_clock::time_point stringToTimestamp(const std::string& timestamp_str);
    int64_t stringToTimestampMs(const std::string& timestamp_str);
    
    // Парсинг JSON
    std::vector<std::vector<std::string>> parseJsonArray(const std::string& json_str);
    
    // Валидация подключения
    void validateConnection();
};

} // namespace database
} // namespace okx
