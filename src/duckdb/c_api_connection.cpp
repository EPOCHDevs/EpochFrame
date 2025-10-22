#include "c_api_connection.h"
#include <duckdb.hpp>
#include <duckdb/common/arrow/result_arrow_wrapper.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/config.hpp>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/extension_type.h>
#include <arrow/compute/api.h>
#include <arrow/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/memory.h>
#include <arrow/io/file.h>
#include <stdexcept>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <sstream>
#include <mutex>
#include <cctype>
#include <atomic>
#include <thread>

// Include nanoarrow extension headers for read_arrow registration
#include "table_function/read_arrow.hpp"

namespace epoch_frame {

// Thread-safe counter for generating unique temporary file names
static std::atomic<uint64_t> temp_file_counter{0};

CAPIConnection::CAPIConnection() {
    using namespace duckdb;

    // Create in-memory database
    db = std::make_unique<DuckDB>(nullptr);
    conn = std::make_unique<Connection>(*db);

    // Register read_arrow function from nanoarrow extension
    auto& db_instance = db->instance->GetDatabase(*conn->context);

    // Create a minimal ExtensionLoader wrapper to register the function
    struct MinimalLoader : public ExtensionLoader {
        MinimalLoader(DatabaseInstance& db_inst) : ExtensionLoader(db_inst, "nanoarrow") {}
    };

    MinimalLoader loader(db_instance);
    ext_nanoarrow::RegisterReadArrowStream(loader);

    // Configure DuckDB settings
    conn->Query("PRAGMA disable_profiling");
    conn->Query("SET max_expression_depth TO 10000");
}

CAPIConnection::CAPIConnection(std::shared_ptr<duckdb::DuckDB> shared_db) {
    using namespace duckdb;

    // Use shared database (thread-local connection to shared DB)
    // Don't store in unique_ptr since we don't own it - just create connection
    conn = std::make_unique<Connection>(*shared_db);

    // Register read_arrow function from nanoarrow extension (per-connection)
    auto& db_instance = shared_db->instance->GetDatabase(*conn->context);

    // Create a minimal ExtensionLoader wrapper to register the function
    struct MinimalLoader : public ExtensionLoader {
        MinimalLoader(DatabaseInstance& db_inst) : ExtensionLoader(db_inst, "nanoarrow") {}
    };

    MinimalLoader loader(db_instance);
    ext_nanoarrow::RegisterReadArrowStream(loader);

    // Configure DuckDB settings
    conn->Query("SET enable_object_cache = false");
    conn->Query("SET max_expression_depth TO 10000");
    conn->Query("PRAGMA disable_profiling");
}

CAPIConnection::~CAPIConnection() {
    // IPC buffers are managed by shared_ptr, will be cleaned up automatically
    // C++ API connections cleaned up automatically by unique_ptr
}

std::shared_ptr<arrow::Table> CAPIConnection::query(std::shared_ptr<arrow::Table> table,
                                                    const std::string& sql) {
    if (!table) {
        throw std::invalid_argument("Arrow table is null");
    }

    // Generate unique temporary file name using thread ID and atomic counter
    auto thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
    uint64_t file_id = temp_file_counter.fetch_add(1);
    std::ostringstream temp_file_stream;
    temp_file_stream << "/tmp/duckdb_arrow_" << thread_id << "_" << file_id << ".arrows";
    std::string temp_file = temp_file_stream.str();

    // Ensure cleanup happens even if exceptions are thrown
    struct FileCleanup {
        std::string filename;
        ~FileCleanup() { std::remove(filename.c_str()); }
    };
    FileCleanup cleanup{temp_file};

    // Write Arrow table to temporary file
    auto output_stream = arrow::io::FileOutputStream::Open(temp_file);
    if (!output_stream.ok()) {
        throw std::runtime_error("Failed to create temp file: " + output_stream.status().ToString());
    }

    auto writer = arrow::ipc::MakeFileWriter(output_stream.ValueOrDie(), table->schema());
    if (!writer.ok()) {
        throw std::runtime_error("Failed to create Arrow writer: " + writer.status().ToString());
    }

    auto write_status = writer.ValueOrDie()->WriteTable(*table);
    if (!write_status.ok()) {
        throw std::runtime_error("Failed to write table: " + write_status.ToString());
    }

    auto close_status = writer.ValueOrDie()->Close();
    if (!close_status.ok()) {
        throw std::runtime_error("Failed to close writer: " + close_status.ToString());
    }

    // Create temp view using read_arrow (built-in function, no extension needed)
    std::string create_view_sql = "CREATE OR REPLACE TEMP VIEW table AS SELECT * FROM read_arrow('" + temp_file + "')";
    auto view_result = conn->Query(create_view_sql);
    if (view_result->HasError()) {
        throw std::runtime_error("Failed to create temp view: " + view_result->GetError());
    }

    // Execute user's SQL query
    auto result = conn->Query(sql);
    if (result->HasError()) {
        throw std::runtime_error("Query failed: " + result->GetError());
    }

    // File will be automatically cleaned up by FileCleanup destructor

    // Use ResultArrowArrayStreamWrapper to convert to Arrow
    // IMPORTANT: wrapper must stay alive until stream is fully consumed
    auto wrapper = duckdb::make_uniq<duckdb::ResultArrowArrayStreamWrapper>(
        std::move(result), 1024);  // batch_size = 1024 rows

    // Get the ArrowArrayStream from the wrapper
    ArrowArrayStream* c_stream = &wrapper->stream;

    // Get schema first
    ArrowSchema arrow_schema;
    if (c_stream->get_schema(c_stream, &arrow_schema) != 0) {
        const char* error = c_stream->get_last_error(c_stream);
        throw std::runtime_error(std::string("Failed to get schema: ") + (error ? error : "unknown error"));
    }

    auto schema_result = arrow::ImportSchema(&arrow_schema);
    if (!schema_result.ok()) {
        throw std::runtime_error("Failed to import schema: " + schema_result.status().ToString());
    }
    auto schema = schema_result.ValueOrDie();

    // Collect all record batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    while (true) {
        ArrowArray arrow_array;
        int ret = c_stream->get_next(c_stream, &arrow_array);
        if (ret != 0) {
            const char* error = c_stream->get_last_error(c_stream);
            throw std::runtime_error(std::string("Failed to get next array: ") + (error ? error : "unknown error"));
        }

        // Check if we reached the end (null release function)
        if (arrow_array.release == nullptr) {
            break;
        }

        // Import the array as a RecordBatch
        auto batch_result = arrow::ImportRecordBatch(&arrow_array, schema);
        if (!batch_result.ok()) {
            throw std::runtime_error("Failed to import record batch: " + batch_result.status().ToString());
        }

        batches.push_back(batch_result.ValueOrDie());
    }

    // Don't manually release the stream - the wrapper's destructor will handle it
    // Now it's safe to let wrapper be destroyed

    // Create table from batches
    std::shared_ptr<arrow::Table> result_table;
    if (batches.empty()) {
        auto empty_result = arrow::Table::MakeEmpty(schema);
        if (!empty_result.ok()) {
            throw std::runtime_error("Failed to create empty table: " + empty_result.status().ToString());
        }
        result_table = empty_result.ValueOrDie();
    } else {
        auto table_result = arrow::Table::FromRecordBatches(schema, batches);
        if (!table_result.ok()) {
            throw std::runtime_error("Failed to create table: " + table_result.status().ToString());
        }
        result_table = table_result.ValueOrDie();
    }

    // Convert extension types to regular Arrow types
    result_table = convertExtensionTypes(result_table);

    return result_table;
}

void CAPIConnection::setMaxExpressionDepth(int depth) {
    std::string sql = "SET max_expression_depth TO " + std::to_string(depth);
    auto result = conn->Query(sql);
    if (result->HasError()) {
        throw std::runtime_error("Failed to set max expression depth: " + result->GetError());
    }
}

std::shared_ptr<arrow::Table> CAPIConnection::convertExtensionTypes(std::shared_ptr<arrow::Table> table) {
    if (!table) {
        return table;
    }

    // Check if any columns need conversion (extension types or decimal128 with scale 0)
    bool needsConversion = false;
    auto schema = table->schema();

    for (int i = 0; i < schema->num_fields(); i++) {
        auto field = schema->field(i);
        if (field->type()->id() == arrow::Type::EXTENSION) {
            needsConversion = true;
            break;
        }
        if (field->type()->id() == arrow::Type::DECIMAL128) {
            auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(field->type());
            if (decimal_type->scale() == 0) {
                needsConversion = true;
                break;
            }
        }
    }

    // If no conversion needed, return table as-is
    if (!needsConversion) {
        return table;
    }

    // Convert columns with extension types
    std::vector<std::shared_ptr<arrow::ChunkedArray>> newColumns;
    std::vector<std::shared_ptr<arrow::Field>> newFields;

    for (int i = 0; i < table->num_columns(); i++) {
        auto column = table->column(i);
        auto field = schema->field(i);

        if (field->type()->id() == arrow::Type::EXTENSION) {
            auto extType = std::static_pointer_cast<arrow::ExtensionType>(field->type());
            auto extName = extType->extension_name();

            // Handle arrow.bool8 extension type
            if (extName == "arrow.bool8") {
                // The storage type for bool8 is int8
                // Convert int8 to boolean: 0 = false, non-zero = true

                // Get the storage array (int8)
                auto extArray = std::static_pointer_cast<arrow::ExtensionArray>(column->chunk(0));
                auto storageArray = extArray->storage();

                // Convert int8 to boolean using compute function
                arrow::compute::ExecContext ctx;
                arrow::Datum input(storageArray);

                // Cast int8 to boolean (0 = false, else = true)
                auto castResult = arrow::compute::Cast(input, arrow::boolean(), arrow::compute::CastOptions::Safe(), &ctx);

                if (!castResult.ok()) {
                    // If cast fails, try manual conversion
                    std::vector<std::shared_ptr<arrow::Array>> boolChunks;

                    for (int j = 0; j < column->num_chunks(); j++) {
                        auto chunk = column->chunk(j);
                        auto extChunk = std::static_pointer_cast<arrow::ExtensionArray>(chunk);
                        auto int8Storage = std::static_pointer_cast<arrow::Int8Array>(extChunk->storage());

                        arrow::BooleanBuilder builder;
                        auto status = builder.Reserve(int8Storage->length());
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to reserve boolean builder: " + status.ToString());
                        }

                        for (int64_t k = 0; k < int8Storage->length(); k++) {
                            if (int8Storage->IsNull(k)) {
                                status = builder.AppendNull();
                            } else {
                                status = builder.Append(int8Storage->Value(k) != 0);
                            }
                            if (!status.ok()) {
                                throw std::runtime_error("Failed to append to boolean builder: " + status.ToString());
                            }
                        }

                        std::shared_ptr<arrow::Array> boolArray;
                        status = builder.Finish(&boolArray);
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to finish boolean builder: " + status.ToString());
                        }
                        boolChunks.push_back(boolArray);
                    }

                    auto boolColumn = arrow::ChunkedArray::Make(boolChunks);
                    if (!boolColumn.ok()) {
                        throw std::runtime_error("Failed to create boolean column: " + boolColumn.status().ToString());
                        }
                    newColumns.push_back(boolColumn.ValueOrDie());
                } else {
                    // Cast succeeded, use the result
                    auto boolArray = castResult.ValueOrDie().make_array();
                    newColumns.push_back(std::make_shared<arrow::ChunkedArray>(boolArray));
                }

                // Create new field with boolean type
                newFields.push_back(arrow::field(field->name(), arrow::boolean(), field->nullable()));
            } else if (extName == "arrow.opaque" && extType->Serialize().find("hugeint") != std::string::npos) {
                // Handle DuckDB HUGEINT extension type (128-bit integer)
                // HUGEINT is stored as 16-byte fixed-size binary in little-endian format
                // We'll convert it to int64 if it fits, otherwise to double
                std::vector<std::shared_ptr<arrow::Array>> convertedChunks;

                for (int j = 0; j < column->num_chunks(); j++) {
                    auto chunk = column->chunk(j);
                    auto extChunk = std::static_pointer_cast<arrow::ExtensionArray>(chunk);
                    auto storageArray = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(extChunk->storage());

                    // Try to convert to int64 first
                    arrow::Int64Builder int64Builder;
                    auto status = int64Builder.Reserve(chunk->length());
                    if (!status.ok()) {
                        throw std::runtime_error("Failed to reserve int64 builder: " + status.ToString());
                    }

                    bool all_fit_int64 = true;
                    for (int64_t k = 0; k < storageArray->length(); k++) {
                        if (storageArray->IsNull(k)) {
                            status = int64Builder.AppendNull();
                        } else {
                            // Get the 16-byte value
                            auto bytes = storageArray->GetValue(k);
                            // Read as little-endian 128-bit: lower 64 bits, then upper 64 bits
                            int64_t lower = *reinterpret_cast<const int64_t*>(bytes);
                            int64_t upper = *reinterpret_cast<const int64_t*>(bytes + 8);

                            // Check if value fits in int64
                            if (upper == 0 || (upper == -1 && lower < 0)) {
                                status = int64Builder.Append(lower);
                            } else {
                                all_fit_int64 = false;
                                break;
                            }
                        }
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to append to int64 builder: " + status.ToString());
                        }
                    }

                    if (all_fit_int64) {
                        std::shared_ptr<arrow::Array> int64Array;
                        status = int64Builder.Finish(&int64Array);
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to finish int64 builder: " + status.ToString());
                        }
                        convertedChunks.push_back(int64Array);
                    } else {
                        // Convert to double instead
                        arrow::DoubleBuilder doubleBuilder;
                        status = doubleBuilder.Reserve(chunk->length());
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to reserve double builder: " + status.ToString());
                        }

                        for (int64_t k = 0; k < storageArray->length(); k++) {
                            if (storageArray->IsNull(k)) {
                                status = doubleBuilder.AppendNull();
                            } else {
                                auto bytes = storageArray->GetValue(k);
                                int64_t lower = *reinterpret_cast<const int64_t*>(bytes);
                                int64_t upper = *reinterpret_cast<const int64_t*>(bytes + 8);
                                // Approximate conversion to double
                                double value = static_cast<double>(lower) + static_cast<double>(upper) * 18446744073709551616.0;
                                status = doubleBuilder.Append(value);
                            }
                            if (!status.ok()) {
                                throw std::runtime_error("Failed to append to double builder: " + status.ToString());
                            }
                        }

                        std::shared_ptr<arrow::Array> doubleArray;
                        status = doubleBuilder.Finish(&doubleArray);
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to finish double builder: " + status.ToString());
                        }
                        convertedChunks.push_back(doubleArray);
                    }
                }

                // Create chunked array from converted chunks
                auto convertedColumn = arrow::ChunkedArray::Make(convertedChunks);
                if (!convertedColumn.ok()) {
                    throw std::runtime_error("Failed to create converted column: " + convertedColumn.status().ToString());
                }
                newColumns.push_back(convertedColumn.ValueOrDie());

                // Determine the new type based on the first chunk
                auto newType = convertedChunks[0]->type();
                newFields.push_back(arrow::field(field->name(), newType, field->nullable()));
            } else {
                // For other extension types, keep as-is for now
                // You can add more conversion cases here as needed
                newColumns.push_back(column);
                newFields.push_back(field);
            }
        } else if (field->type()->id() == arrow::Type::DECIMAL128) {
            // Handle decimal128 types - DuckDB SUM() returns decimal128(38, 0) for integer sums
            auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(field->type());

            // Only convert if scale is 0 (these are actually integers)
            if (decimal_type->scale() == 0) {
                std::vector<std::shared_ptr<arrow::Array>> convertedChunks;

                for (int j = 0; j < column->num_chunks(); j++) {
                    auto chunk = column->chunk(j);
                    auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(chunk);

                    // Try to convert to int64 first
                    arrow::Int64Builder int64Builder;
                    auto status = int64Builder.Reserve(chunk->length());
                    if (!status.ok()) {
                        throw std::runtime_error("Failed to reserve int64 builder: " + status.ToString());
                    }

                    bool all_fit_int64 = true;
                    for (int64_t k = 0; k < decimal_array->length(); k++) {
                        if (decimal_array->IsNull(k)) {
                            status = int64Builder.AppendNull();
                        } else {
                            // Get the raw decimal128 bytes (16 bytes)
                            auto decimal_bytes = decimal_array->GetValue(k);
                            arrow::Decimal128 decimal_val(decimal_bytes);

                            // Decimal128 can be converted to int64 if it fits
                            // Check if value is within int64 range
                            int64_t int_val = 0;  // Initialize to avoid uninitialized warning
                            auto convert_status = decimal_val.ToInteger(&int_val);

                            if (convert_status.ok()) {
                                status = int64Builder.Append(int_val);
                            } else {
                                all_fit_int64 = false;
                                break;
                            }
                        }
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to append to int64 builder: " + status.ToString());
                        }
                    }

                    if (all_fit_int64) {
                        std::shared_ptr<arrow::Array> int64Array;
                        status = int64Builder.Finish(&int64Array);
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to finish int64 builder: " + status.ToString());
                        }
                        convertedChunks.push_back(int64Array);
                    } else {
                        // Convert to double if doesn't fit in int64
                        arrow::DoubleBuilder doubleBuilder;
                        status = doubleBuilder.Reserve(chunk->length());
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to reserve double builder: " + status.ToString());
                        }

                        for (int64_t k = 0; k < decimal_array->length(); k++) {
                            if (decimal_array->IsNull(k)) {
                                status = doubleBuilder.AppendNull();
                            } else {
                                auto decimal_bytes = decimal_array->GetValue(k);
                                arrow::Decimal128 decimal_val(decimal_bytes);
                                double double_val = decimal_val.ToDouble(decimal_type->scale());
                                status = doubleBuilder.Append(double_val);
                            }
                            if (!status.ok()) {
                                throw std::runtime_error("Failed to append to double builder: " + status.ToString());
                            }
                        }

                        std::shared_ptr<arrow::Array> doubleArray;
                        status = doubleBuilder.Finish(&doubleArray);
                        if (!status.ok()) {
                            throw std::runtime_error("Failed to finish double builder: " + status.ToString());
                        }
                        convertedChunks.push_back(doubleArray);
                    }
                }

                // Create chunked array from converted chunks
                auto convertedColumn = arrow::ChunkedArray::Make(convertedChunks);
                if (!convertedColumn.ok()) {
                    throw std::runtime_error("Failed to create converted column: " + convertedColumn.status().ToString());
                }
                newColumns.push_back(convertedColumn.ValueOrDie());

                // Determine the new type based on the first chunk
                auto newType = convertedChunks[0]->type();
                newFields.push_back(arrow::field(field->name(), newType, field->nullable()));
            } else {
                // Keep decimal with non-zero scale as-is
                newColumns.push_back(column);
                newFields.push_back(field);
            }
        } else {
            // Not an extension type or decimal128, keep as-is
            newColumns.push_back(column);
            newFields.push_back(field);
        }
    }

    // Create new table with converted columns
    auto newSchema = arrow::schema(newFields);
    return arrow::Table::Make(newSchema, newColumns);
}

} // namespace epoch_frame
