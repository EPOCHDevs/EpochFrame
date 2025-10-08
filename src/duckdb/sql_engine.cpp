#include "sql_engine.h"
#include "c_api_connection.h"
#include <memory>

namespace epoch_frame {

// Internal DuckDB implementation using C API for zero-copy
class DuckDBEngine : public SQLEngine {
private:
    CAPIConnection& get_connection() {
        return CAPIConnection::getThreadLocal();
    }

public:
    std::shared_ptr<arrow::Table> query(const std::string& sql) override {
        return get_connection().query(sql);
   }

    void register_table(const std::string& name,
                       std::shared_ptr<arrow::Table> table) override {
        get_connection().registerArrowTable(name, table);
    }

    void drop_table(const std::string& name) override {
        get_connection().dropTable(name);
    }

    void execute(const std::string& sql) override {
        get_connection().execute(sql);
    }
};

SQLEngine& SQLEngine::get() {
    static DuckDBEngine instance;
    return instance;
}

} // namespace epoch_frame
