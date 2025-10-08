#include <iostream>
#include <arrow/api.h>
#include <arrow/extension_type.h>
#include "src/duckdb/c_api_connection.h"

int main() {
    using namespace epoch_frame;

    try {
        auto conn = std::make_unique<CAPIConnection>();

        // Create a test table with various types
        std::string sql = R"(
            SELECT
                1 as int_col,
                1.5 as float_col,
                'test' as string_col,
                true as bool_col,
                CAST('2023-01-01' AS DATE) as date_col,
                CAST('2023-01-01 12:00:00' AS TIMESTAMP) as timestamp_col
        )";

        auto result = conn->query(sql);

        std::cout << "Result table schema:\n";
        auto schema = result->schema();
        for (int i = 0; i < schema->num_fields(); i++) {
            auto field = schema->field(i);
            auto type = field->type();

            std::cout << "  Field " << i << ": " << field->name() << " -> " << type->ToString();
            if (type->id() == arrow::Type::EXTENSION) {
                auto extType = std::static_pointer_cast<arrow::ExtensionType>(type);
                std::cout << " (extension: " << extType->extension_name() << ")";
            }
            std::cout << "\n";
        }

        // Test a union query which might produce more extension types
        conn->execute("CREATE TABLE test1 (a INT, b BOOLEAN, c DOUBLE)");
        conn->execute("INSERT INTO test1 VALUES (1, true, 1.5), (2, false, 2.5)");
        conn->execute("CREATE TABLE test2 (a INT, b BOOLEAN, c DOUBLE)");
        conn->execute("INSERT INTO test2 VALUES (3, true, 3.5), (4, false, 4.5)");

        sql = "SELECT * FROM test1 UNION ALL SELECT * FROM test2";
        result = conn->query(sql);

        std::cout << "\nUnion query result schema:\n";
        schema = result->schema();
        for (int i = 0; i < schema->num_fields(); i++) {
            auto field = schema->field(i);
            auto type = field->type();

            std::cout << "  Field " << i << ": " << field->name() << " -> " << type->ToString();
            if (type->id() == arrow::Type::EXTENSION) {
                auto extType = std::static_pointer_cast<arrow::ExtensionType>(type);
                std::cout << " (extension: " << extType->extension_name() << ")";
            }
            std::cout << "\n";
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}