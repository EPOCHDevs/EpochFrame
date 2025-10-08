#include "c_api_connection.h"
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/file.h>
#include <stdexcept>
#include <iostream>
#include <filesystem>
#include <fstream>

namespace epoch_frame {

CAPIConnection::CAPIConnection() : db(nullptr), conn(nullptr) {
    // Create in-memory database
    if (duckdb_open(nullptr, &db) != DuckDBSuccess) {
        throw std::runtime_error("Failed to create DuckDB database");
    }

    // Create connection
    if (duckdb_connect(db, &conn) != DuckDBSuccess) {
        duckdb_close(&db);
        db = nullptr;
        throw std::runtime_error("Failed to create DuckDB connection");
    }

    // Install and load the nanoarrow extension for modern Arrow integration
    duckdb_result config_result;

    // Install the arrow extension
    duckdb_query(conn, "INSTALL arrow", &config_result);
    duckdb_destroy_result(&config_result);

    // Load the arrow extension
    duckdb_query(conn, "LOAD arrow", &config_result);
    duckdb_destroy_result(&config_result);

    // Apply Python-like settings for better Arrow compatibility
    // Set object cache to false like Python default
    duckdb_query(conn, "SET enable_object_cache = false", &config_result);
    duckdb_destroy_result(&config_result);

    // Ensure arrow settings match Python defaults
    duckdb_query(conn, "SET arrow_large_buffer_size = false", &config_result);
    duckdb_destroy_result(&config_result);

    duckdb_query(conn, "SET arrow_lossless_conversion = true", &config_result);
    duckdb_destroy_result(&config_result);
}

CAPIConnection::~CAPIConnection() {
    // Clean up registered tables - no streams to manage with nanoarrow extension
    registered_tables.clear();

    // Disconnect and close
    if (conn) {
        duckdb_disconnect(&conn);
    }
    if (db) {
        duckdb_close(&db);
    }
}

void CAPIConnection::registerArrowTable(const std::string& table_name,
                                       std::shared_ptr<arrow::Table> table) {
    if (!table) {
        throw std::invalid_argument("Arrow table is null");
    }

    // Drop existing table/view if any
    dropTable(table_name);

    // Write table to temporary Arrow IPC file for nanoarrow extension
    std::string temp_filename = "/tmp/" + table_name + "_" + std::to_string(reinterpret_cast<uintptr_t>(this)) + ".arrows";

    // Create file output stream
    auto file_result = arrow::io::FileOutputStream::Open(temp_filename);
    if (!file_result.ok()) {
        throw std::runtime_error("Failed to create temporary file: " + file_result.status().ToString());
    }
    auto file_stream = file_result.ValueOrDie();

    // Create IPC writer
    auto writer_result = arrow::ipc::MakeStreamWriter(file_stream, table->schema());
    if (!writer_result.ok()) {
        throw std::runtime_error("Failed to create IPC writer: " + writer_result.status().ToString());
    }
    auto writer = writer_result.ValueOrDie();

    // Write table to file
    auto status = writer->WriteTable(*table);
    if (!status.ok()) {
        throw std::runtime_error("Failed to write table: " + status.ToString());
    }

    status = writer->Close();
    if (!status.ok()) {
        throw std::runtime_error("Failed to close writer: " + status.ToString());
    }

    status = file_stream->Close();
    if (!status.ok()) {
        throw std::runtime_error("Failed to close file: " + status.ToString());
    }

    // Create view using nanoarrow extension's read_arrow function
    std::string create_view_sql = "CREATE OR REPLACE VIEW " + table_name + " AS SELECT * FROM read_arrow('" + temp_filename + "')";

    duckdb_result result;
    if (duckdb_query(conn, create_view_sql.c_str(), &result) != DuckDBSuccess) {
        // Clean up temp file on error
        std::filesystem::remove(temp_filename);
        auto err = duckdb_result_error(&result);
        std::string error_msg = err ? std::string(err) : "Unknown error";
        duckdb_destroy_result(&result);
        throw std::runtime_error("Failed to create view with nanoarrow: " + error_msg);
    }
    duckdb_destroy_result(&result);

    // Store table info (no streams needed with nanoarrow extension)
    RegisteredTable reg_table;
    reg_table.table = table;
    reg_table.temp_filename = temp_filename;
    registered_tables[table_name] = std::move(reg_table);
}

std::shared_ptr<arrow::Table> CAPIConnection::query(const std::string& sql) {
    // No need to disable filter pushdown with nanoarrow extension
    // The new arrow extension handles filtering properly

    // Execute query and get Arrow result
    duckdb_arrow arrow_result = nullptr;
    if (duckdb_query_arrow(conn, sql.c_str(), &arrow_result) != DuckDBSuccess) {
        // No filter pushdown cleanup needed

        // Get error message for better debugging
        duckdb_result error_result;
        std::string error_msg = "Query failed";
        if (duckdb_query(conn, sql.c_str(), &error_result) != DuckDBSuccess) {
            auto err = duckdb_result_error(&error_result);
            if (err) {
                error_msg = std::string("Query failed: ") + err;
            }
            duckdb_destroy_result(&error_result);
        }
        throw std::runtime_error(error_msg);
    }

    // Collect all result chunks into record batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    // Get the schema once (it's the same for all chunks)
    std::shared_ptr<arrow::Schema> schema_ptr;

    idx_t chunk_count = 0;
    while (true) {
        ArrowArray array = {};
        ArrowSchema schema = {};

        // Get the array for this chunk
        duckdb_arrow_array arrow_array = reinterpret_cast<duckdb_arrow_array>(&array);
        if (duckdb_query_arrow_array(arrow_result, &arrow_array) != DuckDBSuccess) {
            duckdb_destroy_arrow(&arrow_result);
            throw std::runtime_error("Failed to get Arrow array");
        }

        // Check if we have data
        if (!array.release) {
            // No more chunks
            break;
        }

        // For the first chunk, also get the schema
        if (chunk_count == 0) {
            duckdb_arrow_schema arrow_schema = reinterpret_cast<duckdb_arrow_schema>(&schema);
            if (duckdb_query_arrow_schema(arrow_result, &arrow_schema) != DuckDBSuccess) {
                if (array.release) array.release(&array);
                duckdb_destroy_arrow(&arrow_result);
                throw std::runtime_error("Failed to get Arrow schema");
            }

            // Import the schema
            auto schema_result = arrow::ImportSchema(&schema);
            if (!schema_result.ok()) {
                if (array.release) array.release(&array);
                if (schema.release) schema.release(&schema);
                duckdb_destroy_arrow(&arrow_result);
                throw std::runtime_error("Failed to import schema: " + schema_result.status().ToString());
            }
            schema_ptr = schema_result.ValueOrDie();
        }

        // Import the record batch using the imported schema
        auto import_result = arrow::ImportRecordBatch(&array, schema_ptr);
        if (!import_result.ok()) {
            if (array.release) array.release(&array);
            duckdb_destroy_arrow(&arrow_result);
            throw std::runtime_error("Failed to import batch: " + import_result.status().ToString());
        }

        batches.push_back(import_result.ValueOrDie());
        chunk_count++;
    }

    // Clean up
    duckdb_destroy_arrow(&arrow_result);

    // Convert to table
    if (batches.empty()) {
        // No filter pushdown cleanup needed

        // Return empty table with proper schema if we have it
        if (schema_ptr) {
            auto empty_result = arrow::Table::MakeEmpty(schema_ptr);
            if (!empty_result.ok()) {
                throw std::runtime_error("Failed to create empty table");
            }
            return empty_result.ValueOrDie();
        }
        // Create minimal empty table
        auto empty_schema = arrow::schema({});
        auto empty_result = arrow::Table::MakeEmpty(empty_schema);
        if (!empty_result.ok()) {
            throw std::runtime_error("Failed to create empty table");
        }
        return empty_result.ValueOrDie();
    }

    auto table_result = arrow::Table::FromRecordBatches(batches);
    if (!table_result.ok()) {
        throw std::runtime_error("Failed to create table: " + table_result.status().ToString());
    }

    // No filter pushdown cleanup needed

    return table_result.ValueOrDie();
}

void CAPIConnection::execute(const std::string& sql) {
    duckdb_result result;
    if (duckdb_query(conn, sql.c_str(), &result) != DuckDBSuccess) {
        std::string error_msg = duckdb_result_error(&result);
        duckdb_destroy_result(&result);
        throw std::runtime_error("Execution failed: " + error_msg);
    }
    duckdb_destroy_result(&result);
}

void CAPIConnection::dropTable(const std::string& table_name) {
    // Try to drop both view and table (ignore errors)
    try {
        execute("DROP VIEW IF EXISTS " + table_name);
    } catch (...) {}

    try {
        execute("DROP TABLE IF EXISTS " + table_name);
    } catch (...) {}

    // Clean up registered table and temporary file
    auto it = registered_tables.find(table_name);
    if (it != registered_tables.end()) {
        // Remove temporary file if it exists
        if (!it->second.temp_filename.empty()) {
            try {
                std::filesystem::remove(it->second.temp_filename);
            } catch (...) {
                // Ignore file cleanup errors
            }
        }
        registered_tables.erase(it);
    }
}

CAPIConnection& CAPIConnection::getInstance() {
    static CAPIConnection instance;
    return instance;
}

} // namespace epoch_frame