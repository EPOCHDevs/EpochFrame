#include <iostream>
#include <arrow/api.h>
#include <arrow/extension_type.h>
#include <arrow/compute/api.h>
#include <duckdb.h>
#include <arrow/c/bridge.h>

// Simple test to verify extension type conversion
int main() {
    // Create DuckDB connection
    duckdb_database db = nullptr;
    duckdb_connection conn = nullptr;

    if (duckdb_open(nullptr, &db) != DuckDBSuccess) {
        std::cerr << "Failed to create DuckDB database" << std::endl;
        return 1;
    }

    if (duckdb_connect(db, &conn) != DuckDBSuccess) {
        duckdb_close(&db);
        std::cerr << "Failed to create DuckDB connection" << std::endl;
        return 1;
    }

    // Install and load arrow extension
    duckdb_result config_result;
    duckdb_query(conn, "INSTALL arrow", &config_result);
    duckdb_destroy_result(&config_result);
    duckdb_query(conn, "LOAD arrow", &config_result);
    duckdb_destroy_result(&config_result);

    // Create a table with boolean values
    duckdb_query(conn, "CREATE TABLE test_bool (id INT, flag BOOLEAN)", &config_result);
    duckdb_destroy_result(&config_result);

    duckdb_query(conn, "INSERT INTO test_bool VALUES (1, true), (2, false), (3, true)", &config_result);
    duckdb_destroy_result(&config_result);

    // Query with UNION to potentially trigger extension types
    const char* sql = "SELECT * FROM test_bool UNION ALL SELECT 4, false";

    // Execute query and get Arrow result
    duckdb_arrow arrow_result = nullptr;
    if (duckdb_query_arrow(conn, sql, &arrow_result) != DuckDBSuccess) {
        std::cerr << "Query failed" << std::endl;
        duckdb_disconnect(&conn);
        duckdb_close(&db);
        return 1;
    }

    // Get schema to check types
    ArrowSchema schema = {};
    duckdb_arrow_schema arrow_schema = reinterpret_cast<duckdb_arrow_schema>(&schema);
    if (duckdb_query_arrow_schema(arrow_result, &arrow_schema) != DuckDBSuccess) {
        std::cerr << "Failed to get schema" << std::endl;
        duckdb_destroy_arrow(&arrow_result);
        duckdb_disconnect(&conn);
        duckdb_close(&db);
        return 1;
    }

    // Import the schema
    auto schema_result = arrow::ImportSchema(&schema);
    if (!schema_result.ok()) {
        std::cerr << "Failed to import schema" << std::endl;
        duckdb_destroy_arrow(&arrow_result);
        duckdb_disconnect(&conn);
        duckdb_close(&db);
        return 1;
    }

    auto imported_schema = schema_result.ValueOrDie();

    std::cout << "Schema from DuckDB (before conversion):" << std::endl;
    for (int i = 0; i < imported_schema->num_fields(); i++) {
        auto field = imported_schema->field(i);
        auto type = field->type();
        std::cout << "  Field " << field->name() << ": " << type->ToString();

        if (type->id() == arrow::Type::EXTENSION) {
            auto extType = std::static_pointer_cast<arrow::ExtensionType>(type);
            std::cout << " [Extension: " << extType->extension_name() << "]";
        }
        std::cout << std::endl;
    }

    // Clean up
    duckdb_destroy_arrow(&arrow_result);
    duckdb_disconnect(&conn);
    duckdb_close(&db);

    return 0;
}