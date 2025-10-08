#pragma once

#include <duckdb.h>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace epoch_frame {

// Wrapper around DuckDB C API for zero-copy Arrow integration
class CAPIConnection {
private:
    duckdb_database db = nullptr;
    duckdb_connection conn = nullptr;

    // Keep Arrow tables alive while they're registered
    struct RegisteredTable {
        std::shared_ptr<arrow::Table> table;
        std::string temp_filename;  // Temporary .arrows file for nanoarrow extension
    };
    std::unordered_map<std::string, RegisteredTable> registered_tables;

public:
    CAPIConnection();
    ~CAPIConnection();

    // Delete copy operations
    CAPIConnection(const CAPIConnection&) = delete;
    CAPIConnection& operator=(const CAPIConnection&) = delete;

    // Register Arrow table for zero-copy access
    void registerArrowTable(const std::string& table_name,
                           std::shared_ptr<arrow::Table> table);

    // Execute SQL and return Arrow table
    std::shared_ptr<arrow::Table> query(const std::string& sql);

    // Execute SQL without results
    void execute(const std::string& sql);

    // Drop a table
    void dropTable(const std::string& table_name);

    // Set max expression depth for DuckDB queries
    void setMaxExpressionDepth(int depth);

    // Get thread-local instance (one connection per thread for zero contention)
    static CAPIConnection& getThreadLocal();

    // GroupBy query helpers
    std::shared_ptr<arrow::Table> groupByQuery(
        const std::string& table_name,
        const std::vector<std::string>& group_columns,
        const std::vector<std::pair<std::string, std::string>>& agg_functions);

    // Helper to map aggregation function names
    static std::string mapAggregateFunction(const std::string& arrow_agg_name);

private:
    // Convert extension types (like arrow.bool8) to regular Arrow types
    std::shared_ptr<arrow::Table> convertExtensionTypes(std::shared_ptr<arrow::Table> table);
};

} // namespace epoch_frame