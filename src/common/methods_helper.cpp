//
// Created by adesola on 2/13/25.
//

#include "methods_helper.h"
#include <fmt/format.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/compute/exec.h>
#include <epoch_lab_shared/macros.h>
#include <tbb/parallel_for.h>
#include <iostream>
#include <arrow/compute/expression.h>
#include "factory/array_factory.h"
#include "index/arrow_index.h"
#include <unordered_set>

namespace ac = arrow::acero;
namespace cp = arrow::compute;

namespace epochframe {
    arrow::TablePtr add_column(const arrow::TablePtr &table, const std::string &name,
                               const arrow::ChunkedArrayPtr &array) {
        auto result = table->AddColumn(table->num_columns(), arrow::field(name, array->type()), array);
        AssertWithTraceFromStream(result.ok(), "Error adding column: " + result.status().ToString());
        return result.MoveValueUnsafe();
    }

    // Consolidated function to compute intersection, union, left_missing, and right_missing
    std::tuple<std::vector<std::string>, std::unordered_set<std::string>>
    compute_sets(const std::vector<std::string>& left_columns, const std::vector<std::string>& right_columns) {
        // Make local copies of the vectors to avoid modifying the originals
        std::vector<std::string> left_sorted = left_columns;
        std::vector<std::string> right_sorted = right_columns;

        // Sort both vectors as set operations require sorted input
        std::sort(left_sorted.begin(), left_sorted.end());
        std::sort(right_sorted.begin(), right_sorted.end());

        // Compute the union
        std::vector<std::string> unions;
        std::set_union(left_sorted.begin(), left_sorted.end(),
                       right_sorted.begin(), right_sorted.end(),
                       std::back_inserter(unions));

        std::vector<std::string> intersections;
        std::set_intersection(left_sorted.begin(), left_sorted.end(),
                              right_sorted.begin(), right_sorted.end(),
                              std::back_inserter(intersections));

        // Return all results as a tuple
        return std::make_tuple(unions,
                               std::unordered_set<std::string>{intersections.begin(), intersections.end()});
    }

    IndexPtr make_index(arrow::TablePtr const& table,
                                      std::string const& index_name,
                                      std::string const& l_suffix,
                                      std::string const& r_suffix) {
        const auto left_index_name = index_name + l_suffix;
        const auto right_index_name = index_name + r_suffix;

        auto l_index = table->GetColumnByName(left_index_name);
        auto r_index = table->GetColumnByName(right_index_name);

        AssertWithTraceFromStream(l_index != nullptr, "left Index column not found: " << left_index_name + l_suffix);
        AssertWithTraceFromStream(r_index != nullptr, "right Index column not found: " << right_index_name + r_suffix);

        auto merged = arrow_utils::call_compute_array({l_index, r_index}, "coalesce");

        return std::make_shared<ArrowIndex>(merged, index_name);
    }

    arrow::TablePtr collect_table(arrow::TablePtr const& table,
                                  arrow::TablePtr const& merged_table,
                            std::vector<std::string> const& union_columns,
                            std::unordered_set<std::string> const& intersection_columns,
                            std::string const& suffix) {
        arrow::ChunkedArrayVector columns;
        arrow::FieldVector fields;
        columns.reserve(union_columns.size());
        fields.reserve(union_columns.size());

        for (auto const &column_name: union_columns) {
            arrow::ChunkedArrayPtr column;
            std::shared_ptr<arrow::Field> field;
            if (intersection_columns.contains(column_name)) {
                const auto column_with_suffix = column_name + suffix;
                field = table->schema()->GetFieldByName(column_name);
                AssertWithTraceFromStream(field != nullptr,
                                          "field not found: " << column_name << merged_table->ToString());
                column = merged_table->GetColumnByName(column_with_suffix);;
                AssertWithTraceFromStream(column != nullptr,
                                          "column not found: " << column_with_suffix << merged_table->ToString());
            } else {
                field = merged_table->schema()->GetFieldByName(column_name);

                auto array = arrow::MakeArrayOfNull(field->type(), merged_table->num_rows());
                AssertWithTraceFromStream(array.ok(), array.status().ToString());
                column = factory::array::MakeArray(
                        array.MoveValueUnsafe());
            }

            fields.push_back(field);
            AssertWithTraceFromStream(field != nullptr, "field not found: " << column_name << merged_table->ToString());

            columns.push_back(column);
        }

        return arrow::Table::Make(arrow::schema(fields), columns);
    }

    std::tuple<IndexPtr, arrow::TablePtr, arrow::TablePtr>
    align_by_index_and_columns(const TableComponent &left_table_,
                   const TableComponent &right_table_) {
        constexpr auto index_name = "index";
        constexpr auto left_suffix = "_l";
        constexpr auto right_suffix = "_r";

        auto [unions, intersections] = compute_sets(left_table_.second->ColumnNames(),
                                                    right_table_.second->ColumnNames());

        const auto left_rb = add_column(left_table_.second, index_name, left_table_.first->array());
        const auto right_rb = add_column(right_table_.second, index_name, right_table_.first->array());

        ac::Declaration left{"table_source", ac::TableSourceNodeOptions(left_rb)};
        ac::Declaration right{"table_source", ac::TableSourceNodeOptions(right_rb)};

        ac::HashJoinNodeOptions join_opts{
                ac::JoinType::FULL_OUTER,
                /*left_keys=*/{index_name},
                /*right_keys=*/{index_name}, cp::literal(true), left_suffix, right_suffix};

        ac::Declaration hashjoin{
                "hashjoin", {std::move(left), std::move(right)}, std::move(join_opts)};

        auto result = ac::DeclarationToTable(hashjoin);
        AssertWithTraceFromStream(result.ok(), result.status().ToString());

        auto merged = result.MoveValueUnsafe();
        auto index = make_index(merged, index_name, "_l", "_r");
        auto aligned_left = collect_table(left_rb, merged, unions, intersections, left_suffix);
        auto aligned_right = collect_table(right_rb, merged, unions, intersections, right_suffix);

        AssertWithTraceFromStream(aligned_left->schema()->Equals(aligned_right->schema()),
                                  "left columns does not equal right columns\n" << aligned_left->schema()->ToString()
                                                                                << "\n!=\n"
                                                                                << aligned_right->schema()->ToString());
        AssertWithTraceFromStream(aligned_left->num_rows() == index->size(),
                                  "row count mismatch " << aligned_left->num_rows() << " != " << index->size());
        AssertWithTraceFromStream(aligned_left->num_rows() == aligned_right->num_rows(),
                                  "row count mismatch " << aligned_left->num_rows() << " != "
                                                        << aligned_right->num_rows());

        return {index, aligned_left, aligned_right};
    }

//----------------------------------------------------------------------
// Helper: binary_op_aligned(left, right, op)
//
// Both left/right must have the same number of rows and share a row-aligned
// index column. We do column-by-column arithmetic for columns with the same name.
//
// *If you want behavior exactly like Pandas df+df = union of columns with nulls
//  for missing combos, then you'd do additional "coalesce" steps. Here we do
//  the simplest approach: only apply the arithmetic to columns found in both.*
//----------------------------------------------------------------------
    arrow::TablePtr
    unsafe_binary_op(const arrow::TablePtr &left_rb,
                      const arrow::TablePtr &right_rb,
                      const std::string &op) {

        auto schema = left_rb->schema();

        std::vector<arrow::ChunkedArrayPtr> new_arrays;
        auto fields = schema->fields();
        new_arrays.resize(fields.size());

        tbb::parallel_for(0, schema->num_fields(), [&](int i) {
            auto const &field = fields[i];

            auto const &l_column = left_rb->GetColumnByName(field->name());

            auto const &r_column = right_rb->GetColumnByName(field->name());
            AssertWithTraceFromStream(r_column != nullptr, "column not found: " << field->name());

            new_arrays[i] = arrow_utils::call_compute_array({l_column, r_column}, op);
        });

        auto new_rb = arrow::Table::Make(schema, new_arrays);
        return new_rb;
    }

    bool has_unique_type(const arrow::SchemaPtr &schema) {
        if (schema->num_fields() == 0) {
            return false;
        }
        auto type = schema->field(0)->type();
        return std::ranges::all_of(schema->fields(), [type](const auto &field) { return field->type() == type; });
    }

    DictionaryEncodeResult dictionary_encode(const arrow::ArrayPtr &array)
    {
        auto dict_res = arrow::compute::DictionaryEncode(array, arrow::compute::DictionaryEncodeOptions{});;
        if (!dict_res.ok()) {
            throw std::runtime_error(dict_res.status().ToString());
        }

        auto dict_array = std::dynamic_pointer_cast<arrow::DictionaryArray>(dict_res->make_array());
        if (!dict_array) {
            throw std::runtime_error("duplicated(): dictionary_encode didn't return DictionaryArray");
        }

        auto codes = std::dynamic_pointer_cast<arrow::Int32Array>(dict_array->indices());
        return {codes, dict_array->dictionary()};
    }

    ValueCountResult value_counts(const arrow::ArrayPtr &array) {
        // call the Arrow "value_counts" kernel
        auto struct_arr = AssertResultIsOk(arrow::compute::ValueCounts(array));

        // The struct has 2 fields: "values" (same type as m_array) and "counts" (Int64)
        auto values_arr = struct_arr->GetFieldByName("values");
        auto counts_arr = std::dynamic_pointer_cast<arrow::Int64Array>(struct_arr->GetFieldByName("counts"));
        if (!counts_arr) {
            throw std::runtime_error("value_counts: 'counts' field is not Int64Array");
        }
        return {counts_arr, values_arr};
    }
}
