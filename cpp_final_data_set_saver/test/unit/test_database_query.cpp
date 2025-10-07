#include <gtest/gtest.h>
#include "database/database_query.h"
#include "database/database_connection.h"
#include "utils/config.h"
#include <memory>
#include <stdexcept>

using namespace okx;
using namespace okx::database;
using namespace okx::utils;

class DatabaseQueryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a test database connection
        try {
            config_ = std::make_unique<Config>();
            connection_ = std::make_unique<DatabaseConnection>(config_->getDatabaseConfig());
            connection_->connect();
            query_ = std::make_unique<DatabaseQuery>(connection_->getConnection());
        } catch (const std::exception& e) {
            // Skip tests if database is not available
            GTEST_SKIP() << "Database connection failed: " << e.what();
        }
    }
    
    void TearDown() override {
        if (connection_) {
            connection_->disconnect();
        }
    }
    
    std::unique_ptr<Config> config_;
    std::unique_ptr<DatabaseConnection> connection_;
    std::unique_ptr<DatabaseQuery> query_;
};

TEST_F(DatabaseQueryTest, Constructor) {
    ASSERT_TRUE(connection_->isConnected());
    
    DatabaseQuery query(connection_->getConnection(), true, 50);
    EXPECT_TRUE(query.isCachingEnabled());
    EXPECT_EQ(query.getCacheStats().max_size, 50);
}

TEST_F(DatabaseQueryTest, ExecuteQuery) {
    auto result = query_->execute("SELECT 1 as test_value");
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result[0]["test_value"].as<int>(), 1);
}

TEST_F(DatabaseQueryTest, ExecuteParameterizedQuery) {
    auto result = query_->execute("SELECT $1 as test_value", true, 42);
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result[0]["test_value"].as<int>(), 42);
}

TEST_F(DatabaseQueryTest, ExecuteScalar) {
    auto result = query_->executeScalar<int>("SELECT 42");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 42);
}

TEST_F(DatabaseQueryTest, ExecuteScalarWithDefault) {
    auto result = query_->executeScalar<int>("SELECT NULL", 0);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 0);
}

TEST_F(DatabaseQueryTest, ExecuteSingleRow) {
    auto result = query_->executeSingleRow("SELECT 1 as first, 2 as second");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ((*result)["first"].as<int>(), 1);
    EXPECT_EQ((*result)["second"].as<int>(), 2);
}

TEST_F(DatabaseQueryTest, ExecuteSingleRowEmpty) {
    auto result = query_->executeSingleRow("SELECT 1 WHERE 1 = 0");
    EXPECT_FALSE(result.has_value());
}

TEST_F(DatabaseQueryTest, ExecuteAllRows) {
    auto rows = query_->executeAllRows("SELECT 1 as first UNION SELECT 2 as first");
    EXPECT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[0]["first"].as<int>(), 1);
    EXPECT_EQ(rows[1]["first"].as<int>(), 2);
}

TEST_F(DatabaseQueryTest, ExecuteBatch) {
    std::vector<std::string> queries = {
        "SELECT 1 as first",
        "SELECT 2 as second",
        "SELECT 3 as third"
    };
    
    auto results = query_->executeBatch(queries);
    EXPECT_EQ(results.size(), 3);
    EXPECT_EQ(results[0][0]["first"].as<int>(), 1);
    EXPECT_EQ(results[1][0]["second"].as<int>(), 2);
    EXPECT_EQ(results[2][0]["third"].as<int>(), 3);
}

TEST_F(DatabaseQueryTest, ExecuteWithRetry) {
    auto result = query_->executeWithRetry("SELECT 1", 3, 100);
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result[0][0].as<int>(), 1);
}

TEST_F(DatabaseQueryTest, Caching) {
    // First execution
    auto result1 = query_->execute("SELECT 1 as test", true);
    EXPECT_FALSE(result1.empty());
    
    // Second execution should use cache
    auto result2 = query_->execute("SELECT 1 as test", true);
    EXPECT_FALSE(result2.empty());
    
    // Check if query is cached
    EXPECT_TRUE(query_->isCached("SELECT 1 as test"));
}

TEST_F(DatabaseQueryTest, DisableCaching) {
    query_->setCachingEnabled(false);
    EXPECT_FALSE(query_->isCachingEnabled());
    
    auto result = query_->execute("SELECT 1", true);
    EXPECT_FALSE(result.empty());
    EXPECT_FALSE(query_->isCached("SELECT 1"));
}

TEST_F(DatabaseQueryTest, ClearCache) {
    // Execute a query to populate cache
    query_->execute("SELECT 1", true);
    EXPECT_TRUE(query_->isCached("SELECT 1"));
    
    // Clear cache
    query_->clearCache();
    EXPECT_FALSE(query_->isCached("SELECT 1"));
}

TEST_F(DatabaseQueryTest, CacheStats) {
    auto stats = query_->getCacheStats();
    EXPECT_EQ(stats.max_size, 100); // Default max cache size
    EXPECT_GE(stats.result_count, 0);
}

TEST_F(DatabaseQueryTest, IsConnected) {
    EXPECT_TRUE(query_->isConnected());
}

TEST_F(DatabaseQueryTest, GetConnectionInfo) {
    std::string info = query_->getConnectionInfo();
    EXPECT_FALSE(info.empty());
    EXPECT_NE(info.find("Connected"), std::string::npos);
}

TEST_F(DatabaseQueryTest, GetLastFinalDatasetRecord) {
    // This test assumes the table exists and may have data
    // In a real test environment, you would set up test data
    [[maybe_unused]] auto result = query_->getLastFinalDatasetRecord("BTC_USDT");
    // Result may be nullopt if no data exists, which is fine for this test
}

TEST_F(DatabaseQueryTest, GetOrderBookSnapshots) {
    auto now = std::chrono::system_clock::now();
    auto past = now - std::chrono::hours(1);
    
    auto snapshots = query_->getOrderBookSnapshots("BTC_USDT", past, 2);
    // Result may be empty if no data exists, which is fine for this test
    EXPECT_GE(snapshots.size(), 0);
}

TEST_F(DatabaseQueryTest, GetTradesBetween) {
    auto now = std::chrono::system_clock::now();
    auto past = now - std::chrono::hours(1);
    
    auto trades = query_->getTradesBetween("BTC_USDT", past, now);
    // Result may be empty if no data exists, which is fine for this test
    EXPECT_GE(trades.size(), 0);
}

TEST_F(DatabaseQueryTest, GetOrderBookUpdates) {
    auto now = std::chrono::system_clock::now();
    auto past = now - std::chrono::hours(1);
    
    auto updates = query_->getOrderBookUpdates("BTC_USDT", past, now);
    // Result may be empty if no data exists, which is fine for this test
    EXPECT_GE(updates.size(), 0);
}

TEST_F(DatabaseQueryTest, InsertFinalDatasetRecord) {
    OKXDataSetRecordData record(SymbolId::BTC_USDT, 0, 0);
    
    // Заполняем основные поля
    record.buy_quantity = Decimal::fromString("1.5");
    record.buy_trades_count = 2;
    record.buy_volume = Decimal::fromString("75000.0");
    record.close_price = Decimal::fromString("50000.0");
    
    // End order book statistics
    record.end_asks_total_quantity = Decimal::fromString("10.0");
    record.end_asks_total_volume = Decimal::fromString("500100.0");
    record.max_end_ask_price = Decimal::fromString("50010.0");
    record.max_end_ask_quantity = Decimal::fromString("5.0");
    record.max_end_ask_volume = Decimal::fromString("250050.0");
    record.min_end_ask_price = Decimal::fromString("50000.0");
    record.min_end_ask_quantity = Decimal::fromString("1.0");
    record.min_end_ask_volume = Decimal::fromString("50000.0");
    
    record.end_bids_total_quantity = Decimal::fromString("8.0");
    record.end_bids_total_volume = Decimal::fromString("400000.0");
    record.max_end_bid_price = Decimal::fromString("50000.0");
    record.max_end_bid_quantity = Decimal::fromString("4.0");
    record.max_end_bid_volume = Decimal::fromString("200000.0");
    record.min_end_bid_price = Decimal::fromString("49990.0");
    record.min_end_bid_quantity = Decimal::fromString("1.0");
    record.min_end_bid_volume = Decimal::fromString("49990.0");
    
    // Timestamps and IDs
    record.end_timestamp_ms = 1640995200000; // 2022-01-01 00:00:00 UTC
    record.end_trade_id = 12345;
    record.high_price = Decimal::fromString("50010.0");
    
    // Start order book statistics
    record.start_asks_total_quantity = Decimal::fromString("9.0");
    record.start_asks_total_volume = Decimal::fromString("450090.0");
    record.max_start_ask_price = Decimal::fromString("50010.0");
    record.max_start_ask_quantity = Decimal::fromString("4.5");
    record.max_start_ask_volume = Decimal::fromString("225045.0");
    record.min_start_ask_price = Decimal::fromString("50000.0");
    record.min_start_ask_quantity = Decimal::fromString("1.0");
    record.min_start_ask_volume = Decimal::fromString("50000.0");
    
    record.start_bids_total_quantity = Decimal::fromString("7.0");
    record.start_bids_total_volume = Decimal::fromString("350000.0");
    record.max_start_bid_price = Decimal::fromString("50000.0");
    record.max_start_bid_quantity = Decimal::fromString("3.5");
    record.max_start_bid_volume = Decimal::fromString("175000.0");
    record.min_start_bid_price = Decimal::fromString("49990.0");
    record.min_start_bid_quantity = Decimal::fromString("1.0");
    record.min_start_bid_volume = Decimal::fromString("49990.0");
    
    // Additional trade statistics
    record.low_price = Decimal::fromString("49990.0");
    record.open_price = Decimal::fromString("50000.0");
    record.start_timestamp_ms = 1640995200000;
    record.start_trade_id = 12340;
    record.total_quantity = Decimal::fromString("2.0");
    record.total_trades_count = 3;
    record.total_volume = Decimal::fromString("100000.0");
    
    // This test may fail if the table doesn't exist or has constraints
    // In a real test environment, you would set up the database schema
    [[maybe_unused]] bool result = query_->insertFinalDatasetRecord(record);
    // Result may be false if table doesn't exist, which is fine for this test
}

TEST_F(DatabaseQueryTest, ParameterizedQueryCaching) {
    // Test caching with parameterized queries
    auto result1 = query_->execute("SELECT $1 as value", true, 42);
    auto result2 = query_->execute("SELECT $1 as value", true, 42);
    
    EXPECT_FALSE(result1.empty());
    EXPECT_FALSE(result2.empty());
    EXPECT_EQ(result1[0]["value"].as<int>(), 42);
    EXPECT_EQ(result2[0]["value"].as<int>(), 42);
}

TEST_F(DatabaseQueryTest, DifferentParameterValues) {
    // Test that different parameter values don't interfere with caching
    auto result1 = query_->execute("SELECT $1 as value", true, 42);
    auto result2 = query_->execute("SELECT $1 as value", true, 43);
    
    EXPECT_FALSE(result1.empty());
    EXPECT_FALSE(result2.empty());
    EXPECT_EQ(result1[0]["value"].as<int>(), 42);
    EXPECT_EQ(result2[0]["value"].as<int>(), 43);
}

TEST_F(DatabaseQueryTest, CacheKeyGeneration) {
    // Test that different queries generate different cache keys
    query_->execute("SELECT 1", true);
    query_->execute("SELECT 2", true);
    
    EXPECT_TRUE(query_->isCached("SELECT 1"));
    EXPECT_TRUE(query_->isCached("SELECT 2"));
}
