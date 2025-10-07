#include <gtest/gtest.h>
#include "database/database_transaction.h"
#include "database/database_connection.h"
#include "utils/config.h"
#include <memory>
#include <stdexcept>

using namespace okx;
using namespace okx::database;
using namespace okx::utils;

class DatabaseTransactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a test database connection
        try {
            config_ = std::make_unique<Config>();
            connection_ = std::make_unique<DatabaseConnection>(config_->getDatabaseConfig());
            connection_->connect();
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
};

TEST_F(DatabaseTransactionTest, Constructor) {
    ASSERT_TRUE(connection_->isConnected());
    
    DatabaseTransaction transaction(connection_->getConnection());
    EXPECT_TRUE(transaction.isActive());
    EXPECT_FALSE(transaction.isCommitted());
    EXPECT_FALSE(transaction.isRolledBack());
}

TEST_F(DatabaseTransactionTest, ExecuteQuery) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    // Execute a simple query
    auto result = transaction.execute("SELECT 1 as test_value");
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result[0]["test_value"].as<int>(), 1);
}

TEST_F(DatabaseTransactionTest, ExecuteParameterizedQuery) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    // Execute a parameterized query
    auto result = transaction.execute("SELECT $1 as test_value", 42);
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result[0]["test_value"].as<int>(), 42);
}

TEST_F(DatabaseTransactionTest, PrepareStatement) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    // Prepare a statement
    transaction.prepare("test_select", "SELECT $1 as value");
    
    // Execute prepared statement
    auto result = transaction.execute_prepared("test_select", 123);
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result[0]["value"].as<int>(), 123);
}

TEST_F(DatabaseTransactionTest, Commit) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    // Execute a query
    transaction.execute("SELECT 1");
    
    // Commit transaction
    transaction.commit();
    
    EXPECT_TRUE(transaction.isCommitted());
    EXPECT_FALSE(transaction.isActive());
}

TEST_F(DatabaseTransactionTest, Rollback) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    // Execute a query
    transaction.execute("SELECT 1");
    
    // Rollback transaction
    transaction.rollback();
    
    EXPECT_TRUE(transaction.isRolledBack());
    EXPECT_FALSE(transaction.isActive());
}

TEST_F(DatabaseTransactionTest, AutomaticRollback) {
    {
        DatabaseTransaction transaction(connection_->getConnection());
        transaction.execute("SELECT 1");
        // Transaction will be automatically rolled back in destructor
    }
    // Transaction should be rolled back
}

TEST_F(DatabaseTransactionTest, ExecuteBatch) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    std::vector<std::string> queries = {
        "SELECT 1 as first",
        "SELECT 2 as second",
        "SELECT 3 as third"
    };
    
    auto results = transaction.executeBatch(queries);
    EXPECT_EQ(results.size(), 3);
    EXPECT_EQ(results[0][0]["first"].as<int>(), 1);
    EXPECT_EQ(results[1][0]["second"].as<int>(), 2);
    EXPECT_EQ(results[2][0]["third"].as<int>(), 3);
}

TEST_F(DatabaseTransactionTest, ExecuteInTransaction) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    auto result = transaction.executeInTransaction([](DatabaseTransaction& txn) {
        txn.execute("SELECT 1 as test");
        return 42;
    });
    
    EXPECT_EQ(result, 42);
    EXPECT_TRUE(transaction.isCommitted());
}

TEST_F(DatabaseTransactionTest, ExecuteInTransactionWithException) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    EXPECT_THROW(
        transaction.executeInTransaction([](DatabaseTransaction& txn) {
            txn.execute("SELECT 1 as test");
            throw std::runtime_error("Test exception");
        }),
        std::runtime_error
    );
    
    EXPECT_TRUE(transaction.isRolledBack());
}

TEST_F(DatabaseTransactionTest, HasPreparedStatement) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    EXPECT_FALSE(transaction.hasPreparedStatement("nonexistent"));
    
    transaction.prepare("test_stmt", "SELECT 1");
    EXPECT_TRUE(transaction.hasPreparedStatement("test_stmt"));
}

TEST_F(DatabaseTransactionTest, GetPreparedStatementNames) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    transaction.prepare("stmt1", "SELECT 1");
    transaction.prepare("stmt2", "SELECT 2");
    
    auto names = transaction.getPreparedStatementNames();
    EXPECT_EQ(names.size(), 2);
    
    // Check if both names are present (order may vary)
    bool has_stmt1 = std::find(names.begin(), names.end(), "stmt1") != names.end();
    bool has_stmt2 = std::find(names.begin(), names.end(), "stmt2") != names.end();
    EXPECT_TRUE(has_stmt1);
    EXPECT_TRUE(has_stmt2);
}

TEST_F(DatabaseTransactionTest, ClearPreparedStatements) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    transaction.prepare("test_stmt", "SELECT 1");
    EXPECT_TRUE(transaction.hasPreparedStatement("test_stmt"));
    
    transaction.clearPreparedStatements();
    EXPECT_FALSE(transaction.hasPreparedStatement("test_stmt"));
}

TEST_F(DatabaseTransactionTest, TransactionGuard) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    {
        TransactionGuard guard(transaction);
        transaction.execute("SELECT 1");
        // Guard will automatically rollback if not committed
    }
    
    EXPECT_TRUE(transaction.isRolledBack());
}

TEST_F(DatabaseTransactionTest, TransactionGuardCommit) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    {
        TransactionGuard guard(transaction);
        transaction.execute("SELECT 1");
        guard.commit();
    }
    
    EXPECT_TRUE(transaction.isCommitted());
}

TEST_F(DatabaseTransactionTest, TransactionGuardRollback) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    {
        TransactionGuard guard(transaction);
        transaction.execute("SELECT 1");
        guard.rollback();
    }
    
    EXPECT_TRUE(transaction.isRolledBack());
}

TEST_F(DatabaseTransactionTest, InvalidStateAfterCommit) {
    DatabaseTransaction transaction(connection_->getConnection());
    transaction.commit();
    
    EXPECT_THROW(transaction.execute("SELECT 1"), std::runtime_error);
    EXPECT_THROW(transaction.commit(), std::runtime_error);
    EXPECT_THROW(transaction.rollback(), std::runtime_error);
}

TEST_F(DatabaseTransactionTest, InvalidStateAfterRollback) {
    DatabaseTransaction transaction(connection_->getConnection());
    transaction.rollback();
    
    EXPECT_THROW(transaction.execute("SELECT 1"), std::runtime_error);
    EXPECT_THROW(transaction.commit(), std::runtime_error);
    EXPECT_THROW(transaction.rollback(), std::runtime_error);
}

TEST_F(DatabaseTransactionTest, CommonPreparedStatements) {
    DatabaseTransaction transaction(connection_->getConnection());
    
    // Check if common prepared statements are initialized
    EXPECT_TRUE(transaction.hasPreparedStatement("get_last_final_dataset"));
    EXPECT_TRUE(transaction.hasPreparedStatement("get_order_book_snapshots"));
    EXPECT_TRUE(transaction.hasPreparedStatement("get_trades_between"));
    EXPECT_TRUE(transaction.hasPreparedStatement("insert_final_dataset"));
    EXPECT_TRUE(transaction.hasPreparedStatement("get_order_book_updates"));
}
