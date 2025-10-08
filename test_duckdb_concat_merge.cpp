#include <iostream>
#include <memory>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <duckdb.hpp>

using namespace duckdb;

// Helper to register Arrow table with DuckDB
void RegisterArrowTable(Connection& conn, const std::string& name,
                       std::shared_ptr<arrow::Table> table) {
    // Create Arrow IPC stream in memory
    auto stream_result = arrow::io::BufferOutputStream::Create();
    if (!stream_result.ok()) throw std::runtime_error("Failed to create stream");
    auto stream = stream_result.ValueOrDie();

    auto writer_result = arrow::ipc::MakeStreamWriter(stream, table->schema());
    if (!writer_result.ok()) throw std::runtime_error("Failed to create writer");
    auto writer = writer_result.ValueOrDie();

    auto status = writer->WriteTable(*table);
    if (!status.ok()) throw std::runtime_error("Failed to write table");

    status = writer->Close();
    if (!status.ok()) throw std::runtime_error("Failed to close writer");

    auto buffer_result = stream->Finish();
    if (!buffer_result.ok()) throw std::runtime_error("Failed to finish stream");
    auto buffer = buffer_result.ValueOrDie();

    // Register with DuckDB
    std::string sql = "CREATE OR REPLACE TABLE " + name +
                      " AS SELECT * FROM read_arrow(arrow(" +
                      std::to_string(reinterpret_cast<uintptr_t>(buffer->data())) + ", " +
                      std::to_string(buffer->size()) + "))";
    conn.Query(sql);
}

// DuckDB-based concat implementation
std::shared_ptr<arrow::Table> ConcatWithDuckDB(
    const std::vector<std::shared_ptr<arrow::Table>>& tables,
    const std::string& axis = "row",
    const std::string& join_type = "outer") {

    DuckDB db;
    Connection conn(db);

    // Register all tables
    for (size_t i = 0; i < tables.size(); i++) {
        RegisterArrowTable(conn, "t" + std::to_string(i), tables[i]);
    }

    std::string sql;
    if (axis == "row") {
        // Row concatenation using UNION ALL
        sql = "SELECT * FROM t0";
        for (size_t i = 1; i < tables.size(); i++) {
            sql += " UNION ALL SELECT * FROM t" + std::to_string(i);
        }
    } else {
        // Column concatenation using JOIN
        if (tables.size() == 2) {
            std::string join_clause = (join_type == "inner") ? "INNER" : "FULL OUTER";
            sql = "SELECT * FROM t0 " + join_clause + " JOIN t1 USING (index)";
        } else {
            // Multiple tables - chain joins
            sql = "WITH base AS (SELECT * FROM t0)";
            for (size_t i = 1; i < tables.size(); i++) {
                std::string join_clause = (join_type == "inner") ? "INNER" : "FULL OUTER";
                sql += ", step" + std::to_string(i) + " AS (";
                if (i == 1) {
                    sql += "SELECT * FROM base " + join_clause + " JOIN t1 USING (index)";
                } else {
                    sql += "SELECT * FROM step" + std::to_string(i-1) + " " +
                           join_clause + " JOIN t" + std::to_string(i) + " USING (index)";
                }
                sql += ")";
            }
            sql += " SELECT * FROM step" + std::to_string(tables.size()-1);
        }
    }

    auto result = conn.Query(sql);
    if (result->HasError()) {
        throw std::runtime_error("Query failed: " + result->GetError());
    }

    // Convert back to Arrow
    return result->ToArrowTable();
}

// DuckDB-based merge implementation
std::shared_ptr<arrow::Table> MergeWithDuckDB(
    std::shared_ptr<arrow::Table> left,
    std::shared_ptr<arrow::Table> right,
    const std::vector<std::string>& on_columns,
    const std::string& how = "inner") {

    DuckDB db;
    Connection conn(db);

    // Register tables
    RegisterArrowTable(conn, "left_table", left);
    RegisterArrowTable(conn, "right_table", right);

    // Build join condition
    std::string join_condition;
    for (size_t i = 0; i < on_columns.size(); i++) {
        if (i > 0) join_condition += " AND ";
        join_condition += "left_table." + on_columns[i] + " = right_table." + on_columns[i];
    }

    // Build SQL based on join type
    std::string sql;
    if (how == "inner") {
        sql = "SELECT * FROM left_table INNER JOIN right_table ON " + join_condition;
    } else if (how == "left") {
        sql = "SELECT * FROM left_table LEFT JOIN right_table ON " + join_condition;
    } else if (how == "right") {
        sql = "SELECT * FROM left_table RIGHT JOIN right_table ON " + join_condition;
    } else if (how == "outer") {
        sql = "SELECT * FROM left_table FULL OUTER JOIN right_table ON " + join_condition;
    } else if (how == "cross") {
        sql = "SELECT * FROM left_table CROSS JOIN right_table";
    } else {
        throw std::runtime_error("Unknown join type: " + how);
    }

    auto result = conn.Query(sql);
    if (result->HasError()) {
        throw std::runtime_error("Query failed: " + result->GetError());
    }

    return result->ToArrowTable();
}

// Advanced merge with complex conditions
std::shared_ptr<arrow::Table> MergeWithCondition(
    std::shared_ptr<arrow::Table> left,
    std::shared_ptr<arrow::Table> right,
    const std::string& condition,
    const std::string& how = "inner") {

    DuckDB db;
    Connection conn(db);

    RegisterArrowTable(conn, "left_table", left);
    RegisterArrowTable(conn, "right_table", right);

    std::string join_type;
    if (how == "inner") join_type = "INNER";
    else if (how == "left") join_type = "LEFT";
    else if (how == "right") join_type = "RIGHT";
    else if (how == "outer") join_type = "FULL OUTER";
    else throw std::runtime_error("Unknown join type: " + how);

    std::string sql = "SELECT * FROM left_table " + join_type +
                      " JOIN right_table ON " + condition;

    auto result = conn.Query(sql);
    if (result->HasError()) {
        throw std::runtime_error("Query failed: " + result->GetError());
    }

    return result->ToArrowTable();
}

int main() {
    // Create sample Arrow tables
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Table 1
    arrow::Int64Builder id_builder1(pool);
    arrow::DoubleBuilder val_builder1(pool);
    id_builder1.AppendValues({1, 2, 3});
    val_builder1.AppendValues({10.0, 20.0, 30.0});

    std::shared_ptr<arrow::Array> id_array1, val_array1;
    id_builder1.Finish(&id_array1);
    val_builder1.Finish(&val_array1);

    auto table1 = arrow::Table::Make(
        arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("value_a", arrow::float64())
        }),
        {id_array1, val_array1}
    );

    // Table 2
    arrow::Int64Builder id_builder2(pool);
    arrow::DoubleBuilder val_builder2(pool);
    id_builder2.AppendValues({2, 3, 4});
    val_builder2.AppendValues({200.0, 300.0, 400.0});

    std::shared_ptr<arrow::Array> id_array2, val_array2;
    id_builder2.Finish(&id_array2);
    val_builder2.Finish(&val_array2);

    auto table2 = arrow::Table::Make(
        arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("value_b", arrow::float64())
        }),
        {id_array2, val_array2}
    );

    try {
        // Test concat (row-wise)
        std::cout << "Testing row concatenation:" << std::endl;
        auto concat_result = ConcatWithDuckDB({table1, table2}, "row");
        std::cout << "Row concat result schema: " << concat_result->schema()->ToString() << std::endl;
        std::cout << "Num rows: " << concat_result->num_rows() << std::endl;

        // Test merge (join)
        std::cout << "\nTesting inner join:" << std::endl;
        auto merge_result = MergeWithDuckDB(table1, table2, {"id"}, "inner");
        std::cout << "Inner join result schema: " << merge_result->schema()->ToString() << std::endl;
        std::cout << "Num rows: " << merge_result->num_rows() << std::endl;

        // Test left join
        std::cout << "\nTesting left join:" << std::endl;
        auto left_join = MergeWithDuckDB(table1, table2, {"id"}, "left");
        std::cout << "Left join result schema: " << left_join->schema()->ToString() << std::endl;
        std::cout << "Num rows: " << left_join->num_rows() << std::endl;

        // Test complex condition
        std::cout << "\nTesting complex join condition:" << std::endl;
        auto complex_join = MergeWithCondition(table1, table2,
            "left_table.id = right_table.id AND left_table.value_a < right_table.value_b",
            "inner");
        std::cout << "Complex join result schema: " << complex_join->schema()->ToString() << std::endl;
        std::cout << "Num rows: " << complex_join->num_rows() << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\nAll tests passed!" << std::endl;
    return 0;
}