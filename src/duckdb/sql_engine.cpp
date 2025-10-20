#include "sql_engine.h"
#include "c_api_connection.h"
#include <duckdb.hpp>
#include <memory>

namespace epoch_frame {

// Internal DuckDB implementation - SIMPLIFIED to single-table API
class DuckDBEngine : public SQLEngine {
private:
    // Each thread gets its own database AND connection - fully isolated
    static thread_local std::unique_ptr<CAPIConnection> thread_db_connection_;

    // Get or create the thread-local database+connection
    CAPIConnection& getThreadConnection() {
        if (!thread_db_connection_) {
            // Each thread creates its own DuckDB instance (no sharing!)
            thread_db_connection_ = std::make_unique<CAPIConnection>();
        }
        return *thread_db_connection_;
    }

public:
    DuckDBEngine() = default;

    CAPIConnection& getMainConnection() {
        // Return current thread's connection for direct access to new API
        return getThreadConnection();
    }
};

// Define thread_local static member
thread_local std::unique_ptr<CAPIConnection> DuckDBEngine::thread_db_connection_;

SQLEngine& SQLEngine::get() {
    static DuckDBEngine instance;
    return instance;
}

// Friend function to get the underlying connection (used by groupby operations)
CAPIConnection& getSQLEngineConnection() {
    static DuckDBEngine& engine = static_cast<DuckDBEngine&>(SQLEngine::get());
    return engine.getMainConnection();
}

} // namespace epoch_frame
