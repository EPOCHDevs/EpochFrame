#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <cstdint>
#include <vector>
#include <utility>

// Forward declarations
namespace arrow {
    class Table;
}

struct ArrowArrayStream;

namespace duckdb {
    class DuckDB;
    class Connection;
}

namespace epoch_frame {

// Forward declare IPC buffer holder
struct IPCBufferHolder;

// Wrapper around DuckDB C++ API using Arrow IPC format for stability
class CAPIConnection {
private:
    std::unique_ptr<duckdb::DuckDB> db;
    std::unique_ptr<duckdb::Connection> conn;

public:
    CAPIConnection();  // Default constructor (creates own database)
    CAPIConnection(std::shared_ptr<duckdb::DuckDB> shared_db);  // Constructor using shared database
    ~CAPIConnection();  // Defined in .cpp to allow incomplete types in unique_ptr

    // Delete copy operations
    CAPIConnection(const CAPIConnection&) = delete;
    CAPIConnection& operator=(const CAPIConnection&) = delete;

    // Execute SQL query on an Arrow table (table available as "t" in SQL)
    // Example: query(my_table, "SELECT * FROM t WHERE x > 10")
    std::shared_ptr<arrow::Table> query(std::shared_ptr<arrow::Table> table,
                                        const std::string& sql);

    // Set max expression depth for DuckDB queries
    void setMaxExpressionDepth(int depth);

private:
    // Convert extension types (like arrow.bool8) to regular Arrow types
    std::shared_ptr<arrow::Table> convertExtensionTypes(std::shared_ptr<arrow::Table> table);
};

// Get the SQL engine's thread-local connection (used by groupby operations)
CAPIConnection& getSQLEngineConnection();

} // namespace epoch_frame