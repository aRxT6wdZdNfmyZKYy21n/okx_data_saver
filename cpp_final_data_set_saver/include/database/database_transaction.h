#pragma once

#include <pqxx/pqxx>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <functional>

namespace okx {
namespace database {

/**
 * @brief Database transaction wrapper for PostgreSQL
 * 
 * This class provides a RAII wrapper around pqxx::transaction
 * with additional functionality for prepared statements and error handling.
 */
class DatabaseTransaction {
private:
    std::unique_ptr<pqxx::work> transaction_;
    std::map<std::string, std::string> prepared_statements_;
    bool committed_;
    bool rolled_back_;
    
public:
    /**
     * @brief Constructor
     * @param connection Database connection
     */
    explicit DatabaseTransaction(pqxx::connection& connection);
    
    /**
     * @brief Destructor - automatically rolls back if not committed
     */
    ~DatabaseTransaction();
    
    // Disable copy constructor and assignment
    DatabaseTransaction(const DatabaseTransaction&) = delete;
    DatabaseTransaction& operator=(const DatabaseTransaction&) = delete;
    
    /**
     * @brief Execute a query
     * @param query SQL query string
     * @return Query result
     */
    pqxx::result execute(const std::string& query);
    
    /**
     * @brief Execute a query with parameters
     * @param query SQL query string with placeholders
     * @param params Query parameters
     * @return Query result
     */
    template<typename... Args>
    pqxx::result execute(const std::string& query, Args&&... args) {
        return transaction_->exec_params(query, std::forward<Args>(args)...);
    }
    
    /**
     * @brief Prepare a statement
     * @param name Statement name
     * @param query SQL query string
     */
    void prepare(const std::string& name, const std::string& query);
    
    /**
     * @brief Execute a prepared statement
     * @param name Statement name
     * @param params Statement parameters
     * @return Query result
     */
    template<typename... Args>
    pqxx::result execute_prepared(const std::string& name, Args&&... args) {
        return transaction_->exec_prepared(name, std::forward<Args>(args)...);
    }
    
    /**
     * @brief Commit the transaction
     */
    void commit();
    
    /**
     * @brief Rollback the transaction
     */
    void rollback();
    
    /**
     * @brief Check if transaction is active
     * @return True if active
     */
    bool isActive() const;
    
    /**
     * @brief Check if transaction is committed
     * @return True if committed
     */
    bool isCommitted() const { return committed_; }
    
    /**
     * @brief Check if transaction is rolled back
     * @return True if rolled back
     */
    bool isRolledBack() const { return rolled_back_; }
    
    /**
     * @brief Get the underlying pqxx::work object
     * @return Reference to pqxx::work
     */
    pqxx::work& getWork() { return *transaction_; }
    
    /**
     * @brief Get the underlying pqxx::work object (const)
     * @return Const reference to pqxx::work
     */
    const pqxx::work& getWork() const { return *transaction_; }
    
    /**
     * @brief Execute multiple queries in a batch
     * @param queries Vector of SQL queries
     * @return Vector of query results
     */
    std::vector<pqxx::result> executeBatch(const std::vector<std::string>& queries);
    
    /**
     * @brief Execute a function within the transaction
     * @param func Function to execute
     * @return Function result
     */
    template<typename Func>
    auto executeInTransaction(Func&& func) -> decltype(func(*this)) {
        try {
            if constexpr (std::is_void_v<decltype(func(*this))>) {
                func(*this);
                commit();
            } else {
                auto result = func(*this);
                commit();
                return result;
            }
        } catch (...) {
            rollback();
            throw;
        }
    }
    
    /**
     * @brief Get last error message
     * @return Error message
     */
    std::string getLastError() const;
    
    /**
     * @brief Check if a prepared statement exists
     * @param name Statement name
     * @return True if exists
     */
    bool hasPreparedStatement(const std::string& name) const;
    
    /**
     * @brief Get all prepared statement names
     * @return Vector of statement names
     */
    std::vector<std::string> getPreparedStatementNames() const;
    
    /**
     * @brief Clear all prepared statements
     */
    void clearPreparedStatements();
    
private:
    /**
     * @brief Initialize common prepared statements
     */
    void initializePreparedStatements();
    
    /**
     * @brief Validate transaction state
     */
    void validateState() const;
};

/**
 * @brief Transaction guard for automatic rollback
 */
class TransactionGuard {
private:
    DatabaseTransaction& transaction_;
    bool committed_;
    
public:
    explicit TransactionGuard(DatabaseTransaction& transaction)
        : transaction_(transaction), committed_(false) {}
    
    ~TransactionGuard() {
        if (!committed_) {
            transaction_.rollback();
        }
    }
    
    void commit() {
        transaction_.commit();
        committed_ = true;
    }
    
    void rollback() {
        transaction_.rollback();
        committed_ = true;
    }
};

} // namespace database
} // namespace okx
