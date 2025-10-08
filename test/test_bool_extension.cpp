#include <iostream>
#include "epoch_frame/dataframe.h"
#include "epoch_frame/common.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"

using namespace epoch_frame;
using namespace epoch_frame::factory::index;

int main() {
    try {
        // Create test DataFrames with boolean columns
        auto idx1 = from_range(3);
        auto idx2 = from_range(3, 6);

        // Create DataFrames that will trigger DuckDB usage through concat
        DataFrame df1 = make_dataframe(idx1, {{1, 2, 3}, {true, false, true}}, {"int_col", "bool_col"});
        DataFrame df2 = make_dataframe(idx2, {{4, 5, 6}, {false, true, false}}, {"int_col", "bool_col"});

        std::cout << "DataFrame 1 schema:" << std::endl;
        auto schema1 = df1.table()->schema();
        for (int i = 0; i < schema1->num_fields(); i++) {
            auto field = schema1->field(i);
            std::cout << "  " << field->name() << ": " << field->type()->ToString() << std::endl;
        }

        std::cout << "\nDataFrame 2 schema:" << std::endl;
        auto schema2 = df2.table()->schema();
        for (int i = 0; i < schema2->num_fields(); i++) {
            auto field = schema2->field(i);
            std::cout << "  " << field->name() << ": " << field->type()->ToString() << std::endl;
        }

        // Concat which will use DuckDB
        ConcatOptions options{{df1, df2}, JoinType::Outer, AxisType::Row, false, false};
        auto result = concat(options);

        std::cout << "\nResult schema after concat (should have bool, not extension<arrow.bool8>):" << std::endl;
        auto result_schema = result.table()->schema();
        for (int i = 0; i < result_schema->num_fields(); i++) {
            auto field = result_schema->field(i);
            auto type = field->type();
            std::cout << "  " << field->name() << ": " << type->ToString();

            // Check if it's an extension type
            if (type->id() == arrow::Type::EXTENSION) {
                auto extType = std::static_pointer_cast<arrow::ExtensionType>(type);
                std::cout << " [ERROR: Still extension type: " << extType->extension_name() << "]";
            } else if (type->id() == arrow::Type::BOOL) {
                std::cout << " [OK: Regular boolean type]";
            }
            std::cout << std::endl;
        }

        // Also test column-wise concat
        ConcatOptions col_options{{df1, df2}, JoinType::Outer, AxisType::Column, false, false};
        auto col_result = concat(col_options);

        std::cout << "\nColumn concat result schema:" << std::endl;
        auto col_schema = col_result.table()->schema();
        for (int i = 0; i < col_schema->num_fields(); i++) {
            auto field = col_schema->field(i);
            auto type = field->type();
            std::cout << "  " << field->name() << ": " << type->ToString();

            if (type->id() == arrow::Type::EXTENSION) {
                auto extType = std::static_pointer_cast<arrow::ExtensionType>(type);
                std::cout << " [ERROR: Still extension type: " << extType->extension_name() << "]";
            } else if (type->id() == arrow::Type::BOOL) {
                std::cout << " [OK: Regular boolean type]";
            }
            std::cout << std::endl;
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}