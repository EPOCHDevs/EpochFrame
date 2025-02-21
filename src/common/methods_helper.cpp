//
// Created by adesola on 2/13/25.
//

#include "methods_helper.h"
#include <fmt/format.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <epoch_lab_shared/macros.h>
#include <tbb/parallel_for.h>
#include <iostream>
#include <arrow/compute/expression.h>
#include "factory/array_factory.h"
#include "factory/table_factory.h"
#include "index/arrow_index.h"
#include <unordered_set>
#include "epochframe/dataframe.h"
#include "epochframe/series.h"
#include "common/frame_or_series.h"

namespace ac = arrow::acero;
namespace cp = arrow::compute;

namespace epochframe {
    arrow::TablePtr add_column(const arrow::TablePtr &table, const std::string &name,
                               const arrow::ChunkedArrayPtr &array) {
        auto result = table->AddColumn(table->num_columns(), arrow::field(name, array->type()), array);
        AssertWithTraceFromStream(result.ok(), "Error adding column: " + result.status().ToString());
        return result.MoveValueUnsafe();
    }

    arrow::TablePtr add_column(const arrow::TablePtr &table, const std::string &name,
                               const arrow::ArrayPtr &array) {
        return add_column(table, name, factory::array::make_array(array));
    }

    // Consolidated function to compute intersection, union, left_missing, and right_missing
    std::tuple<std::vector<std::string>, std::unordered_set<std::string>>
    compute_sets(const std::vector<std::string> &left_columns, const std::vector<std::string> &right_columns) {
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

    IndexPtr make_index(arrow::TablePtr const &table,
                        std::string const &index_name,
                        std::string const &l_suffix,
                        std::string const &r_suffix) {
        const auto left_index_name = index_name + l_suffix;
        const auto right_index_name = index_name + r_suffix;

        auto l_index = table->GetColumnByName(left_index_name);
        auto r_index = table->GetColumnByName(right_index_name);

        AssertWithTraceFromStream(l_index != nullptr, "left Index column not found: " << left_index_name + l_suffix);
        AssertWithTraceFromStream(r_index != nullptr, "right Index column not found: " << right_index_name + r_suffix);

        auto merged = arrow_utils::call_compute_array({l_index, r_index}, "coalesce");

        return std::make_shared<ArrowIndex>(merged, index_name);
    }

    arrow::TablePtr collect_table(arrow::TablePtr const &table,
                                  arrow::TablePtr const &merged_table,
                                  std::vector<std::string> const &union_columns,
                                  std::unordered_set<std::string> const &intersection_columns,
                                  std::string const &suffix) {
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
                column = factory::array::make_array(
                        array.MoveValueUnsafe());
            }

            fields.push_back(field);
            AssertWithTraceFromStream(field != nullptr, "field not found: " << column_name << merged_table->ToString());

            columns.push_back(column);
        }

        return arrow::Table::Make(arrow::schema(fields), columns);
    }

    std::tuple<IndexPtr, arrow::TablePtr, arrow::TablePtr>
    align_by_index_and_columns(const TableComponent &left_component,
                               const TableComponent &right_component) {
        constexpr auto index_name = "index";
        constexpr auto left_suffix = "_l";
        constexpr auto right_suffix = "_r";

        auto left_table = left_component.second.get_table("left_array");
        auto right_table = right_component.second.get_table("right_array");

        auto [unions, intersections] = compute_sets(left_table->ColumnNames(),
                                                    right_table->ColumnNames());

        const arrow::TablePtr left_rb = add_column(left_table, index_name, left_component.first->array());
        const arrow::TablePtr right_rb = add_column(right_table, index_name, right_component.first->array());

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
        IndexPtr index = make_index(merged, index_name, "_l", "_r");
        arrow::TablePtr aligned_left = collect_table(left_rb, merged, unions, intersections, left_suffix);
        arrow::TablePtr aligned_right = collect_table(right_rb, merged, unions, intersections, right_suffix);

        AssertWithTraceFromStream(aligned_left->schema()->Equals(aligned_right->schema()),
                                  "left columns does not equal right columns\n" << aligned_left->schema()->ToString()
                                                                                << "\n!=\n"
                                                                                << aligned_right->schema()->ToString());
        AssertWithTraceFromStream(aligned_left->num_rows() == index->size(),
                                  "row count mismatch " << aligned_left->num_rows() << " != " << index->size());
        AssertWithTraceFromStream(aligned_left->num_rows() == aligned_right->num_rows(),
                                  "row count mismatch " << aligned_left->num_rows() << " != "
                                                        << aligned_right->num_rows());

        return std::tuple<IndexPtr, arrow::TablePtr, arrow::TablePtr>{index, aligned_left, aligned_right};
    }

    arrow::TablePtr
    align_by_index(const TableComponent &left_table_,
                   const IndexPtr & new_index_,
                   const Scalar &fillValue) {
        AssertWithTraceFromStream(new_index_ != nullptr, "Index cannot be null");
        AssertWithTraceFromStream(left_table_.first != nullptr, "Table Index cannot be null");

        if (left_table_.first->equals(new_index_))
        {
            return left_table_.second.get_table("series_name");
        }
        if (new_index_->size() == 0) {
            return factory::table::make_empty_table(left_table_.second.get_table("series_name")->schema());
        }

        auto left_type = left_table_.first->dtype();
        auto right_type = new_index_->dtype();
        AssertWithTraceFromStream(left_type->Equals(right_type),
                                fmt::format("Index type mismatch. Source index type: {}, Target index type: {}",
                                        left_type->ToString(),
                                        right_type->ToString()));

        const std::string index_name = "index";
        constexpr auto left_suffix = "_l";
        constexpr auto right_suffix = "_r";
        auto left_index_name = index_name + left_suffix;
        auto right_index_name = index_name + right_suffix;

        const arrow::TablePtr left_rb = add_column(left_table_.second.get_table("series_name"), index_name, left_table_.first->array());
        auto index_table = new_index_->to_table(index_name);

        ac::Declaration left{"table_source", ac::TableSourceNodeOptions(left_rb)};
        ac::Declaration right{"table_source", ac::TableSourceNodeOptions(index_table)};

        ac::HashJoinNodeOptions join_opts{
                ac::JoinType::RIGHT_OUTER,
                /*left_keys=*/{index_name},
                /*right_keys=*/{index_name}, cp::literal(true), left_suffix, right_suffix};

        ac::Declaration hashjoin{
                "hashjoin", {std::move(left), std::move(right)}, std::move(join_opts)};

        cp::Expression filter_expr = cp::is_valid(cp::field_ref(right_index_name));
        ac::Declaration filter{
                "filter", {std::move(hashjoin)}, ac::FilterNodeOptions(std::move(filter_expr))};

        arrow::TablePtr merged = AssertResultIsOk(ac::DeclarationToTable(filter));

        auto right_index_pos = merged->schema()->GetFieldIndex(right_index_name);
        auto left_index_pos = merged->schema()->GetFieldIndex(left_index_name);

        AssertWithTraceFromStream(left_index_pos != -1, "Failed to find left index after alignment merge.");
        AssertWithTraceFromStream(right_index_pos != -1, "Failed to find right index after alignment merge.");

        auto sorted_table = AssertTableResultIsOk(cp::Take(merged, AssertResultIsOk(SortIndices(
        merged,
        arrow::SortOptions({cp::SortKey{
            arrow::FieldRef{right_index_pos},
        }})))));

        auto new_table = AssertResultIsOk(sorted_table->RemoveColumn(right_index_pos));
        if (fillValue.is_valid()) {
            return AssertTableResultIsOk(arrow_utils::call_compute_fill_null_table(new_table, fillValue.value()));
        }
        return new_table;
    }

    std::pair<bool, IndexPtr> combine_index(std::vector<FrameOrSeries> const& objs, bool intersect) {
        bool index_all_equal = true;
        auto merged_index = std::accumulate(objs.begin() + 1, objs.end(), objs.at(0).index(),
                                            [&](const IndexPtr &acc, FrameOrSeries const &obj) {
                                                auto next_index = obj.index();
                                                AssertWithTraceFromStream(acc->dtype()->Equals(next_index->dtype()),
                                                                          "concat multiple frames requires same index");
                                                auto index_equal = acc->equals(next_index);
                                                if (index_equal) {
                                                    return acc;
                                                }
                                                index_all_equal = false;
                                                return intersect ? acc->intersection(next_index) : acc->union_(
                                                        next_index);
                                            });
        return {index_all_equal, merged_index};
    }

    DataFrame concat_column_unsafe(std::vector<FrameOrSeries> const& objs, IndexPtr const& newIndex,  bool ignore_index) {
        if (objs.empty()) {
            return DataFrame{};
        }

        arrow::ChunkedArrayVector arrays;
        arrow::FieldVector fields;
        int64_t i = 0;
        for (auto const &obj: objs) {
            auto [columns_, fields_] = obj.visit([&](auto &&variant_) {
                using T = std::decay_t<decltype(variant_)>;
                if constexpr (std::same_as<T, DataFrame>) {
                    return std::pair<arrow::ChunkedArrayVector, arrow::FieldVector>{
                            variant_.table()->columns(),
                            variant_.table()->schema()->fields()
                    };
                } else {
                    auto n = variant_.name();
                    if (!n) {
                        n = std::to_string(i++);
                    }
                    return std::pair<arrow::ChunkedArrayVector, arrow::FieldVector>{
                            std::vector{variant_.array()},
                            {arrow::field(n.value(), variant_.array()->type())}
                    };
                }
            });
            arrays.insert(arrays.end(), columns_.begin(), columns_.end());
            fields.insert(fields.end(), fields_.begin(), fields_.end());
        }
        auto new_table = arrow::Table::Make(arrow::schema(fields), arrays);
        return ignore_index ? DataFrame(new_table) : DataFrame(newIndex, new_table);
    }

    DataFrame concat_column_safe(std::vector<FrameOrSeries> const& objs, IndexPtr const& newIndex,  bool ignore_index) {
        if (objs.empty()) {
            return DataFrame{};
        }
        std::vector<FrameOrSeries> aligned_frames(objs.size());

        tbb::parallel_for(0UL, objs.size(), [&](size_t i) {
            auto obj = objs[i];
            aligned_frames[i] = FrameOrSeries(DataFrame(newIndex, align_by_index(TableComponent{
                                                                                       obj.index(),
                                                                                       obj.table_or_array()
                                                                               }, newIndex)));
        });
        return concat_column_unsafe(aligned_frames, newIndex, ignore_index);
    }

    DataFrame concat_row(std::vector<FrameOrSeries> const& objs, bool ignore_index, bool intersect) {
        if (objs.empty()) {
            return DataFrame{};
        }
        std::vector<arrow::TablePtr> tables(objs.size());
        tbb::parallel_for(0UL, objs.size(), [&](size_t i) {
            auto table = objs[i].table();;
            if (!ignore_index) {
                tables.push_back(objs[i].index()->to_table(std::nullopt));
            }
            tables[i] = objs[i].table();
        });

        arrow::ConcatenateTablesOptions options;
        options.field_merge_options = arrow::Field::MergeOptions::Permissive();
        options.unify_schemas = intersect;

        arrow::TablePtr merged = AssertResultIsOk(arrow::ConcatenateTables(tables));
        if (ignore_index) {
            return DataFrame{
                    merged
            };
        }
        auto indexField = merged->schema()->GetFieldIndex("");
        AssertWithTraceFromStream(indexField != -1, "Failed to find index");

        auto index = merged->column(indexField);
        return DataFrame(std::make_shared<ArrowIndex>(index),
                       AssertResultIsOk(merged->RemoveColumn(indexField)));
    }

    DataFrame concat(std::vector<FrameOrSeries> const& frames,
                   JoinType join_type,
                   AxisType axis,
                   bool ignore_index,
                   bool sort ) {
        DataFrame frame;
        if (axis == AxisType::Column) {
            auto [all_index_equals, merged_index] = combine_index(frames, join_type == JoinType::Inner);
            if (all_index_equals) {
                frame = concat_column_unsafe(frames, merged_index, ignore_index);
            } else {
                frame = concat_column_safe(frames, merged_index, ignore_index);
            }
        } else {
            frame = concat_row(frames, ignore_index, join_type == JoinType::Inner);
        }
        if (sort) {
            return axis == AxisType::Column ? frame.sort_index() : frame.sort_columns();
        }
        return frame;
    }

    DataFrame merge(FrameOrSeries const &left,
                  FrameOrSeries const& right,
                  JoinType joinType,
                  AxisType axis,
                  bool ignore_index,
                  bool sort ) {

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

    DictionaryEncodeResult dictionary_encode(const arrow::ArrayPtr &array) {
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

    arrow::ChunkedArrayPtr get_column_by_name(arrow::Table const &table, std::string const &name) {
        auto column = table.GetColumnByName(name);
        AssertWithTraceFromStream(column != nullptr, "ColumnNotFound: " << name);
        return column;
    }

    arrow::FieldPtr get_field_by_name(arrow::Schema const &schema, std::string const &name) {
        auto field = schema.GetFieldByName(name);
        AssertWithTraceFromStream(field != nullptr, "FieldNotFound: " << name);
        return field;
    }

    arrow::SchemaPtr slice_schema(arrow::Schema const &schema, std::vector<std::string> const &column_names) {
        arrow::FieldVector fields(column_names.size());
        std::ranges::transform(column_names, fields.begin(), [&](auto const &name) {
            return get_field_by_name(schema, name);
        });
        return arrow::schema(fields);
    }

    DataFrame get_variant_column(DataFrame const & frame, const LocColArgumentVariant & colVariant) {
        return std::visit([&](auto const& variant_type){
            return frame[variant_type];
        }, colVariant);
    }

    DataFrame get_variant_row(DataFrame const & frame, const LocRowArgumentVariant & rowVariant) {
        return std::visit([&](auto const& variant_type){
            return frame.loc(variant_type);
        }, rowVariant);
    }

    arrow::TablePtr make_table_from_array_schema(arrow::Table const& table, arrow::ChunkedArrayPtr const &array) {
        return arrow::Table::Make(table.schema(), std::vector{static_cast<size_t>(table.num_columns()), array});
    }

    arrow::ChunkedArrayPtr get_array(arrow::Table const &table, std::string const &name, arrow::Scalar const &default_value) {
        auto column = table.GetColumnByName(name);
        if (column) {
            return column;
        }
        return AssertArrayResultIsOk(arrow::MakeArrayFromScalar(default_value, table.num_rows()));
    }
}
