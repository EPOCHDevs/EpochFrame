#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>

namespace epoch_frame {

// Abstract SQL engine interface - hides implementation details
class SQLEngine {
public:
    virtual ~SQLEngine() = default;

    // Execute SQL query and return Arrow table
    virtual std::shared_ptr<arrow::Table> query(const std::string& sql) = 0;

    // Register Arrow table with a name for SQL queries
    virtual void register_table(const std::string& name,
                               std::shared_ptr<arrow::Table> table) = 0;

    // Drop a registered table
    virtual void drop_table(const std::string& name) = 0;

    // Execute statement without returning results
    virtual void execute(const std::string& sql) = 0;

    // Get the singleton SQL engine instance
    static SQLEngine& get();
};

} // namespace epoch_frame