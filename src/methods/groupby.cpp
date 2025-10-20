//
// Created by adesola on 1/20/25.
//

#include "groupby.h"

#include <iostream>

#include "arrow/compute/api.h"
#include "epoch_core/macros.h"
#include "epoch_frame/factory/index_factory.h"
#include "index/datetime_index.h"
#include <common/arrow_agg.h>
#include <common/asserts.h>

#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/array_factory.h"
#include <common/methods_helper.h>
#include <epoch_frame/common.h>
#include <index/arrow_index.h>
#include <regex>
#include <utility>
#include <vector_functions/arrow_vector_functions.h>

// Include DuckDB last to avoid macro conflicts
#include "duckdb/c_api_connection.h"

namespace epoch_frame
{
    namespace cp = arrow::compute;

    // Helper to map aggregate function names to DuckDB SQL functions
    std::string mapAggregateFunction(const std::string& agg_name) {
        static const std::unordered_map<std::string, std::string> agg_map = {
            {"sum", "SUM"},
            {"mean", "AVG"},
            {"min", "MIN"},
            {"max", "MAX"},
            {"count", "COUNT"},
            {"count_distinct", "COUNT(DISTINCT "},
            {"std", "STDDEV"},
            {"stddev", "STDDEV"},
            {"var", "VARIANCE"},
            {"variance", "VARIANCE"},
            {"product", "PRODUCT"},
            {"any", "BOOL_OR"},
            {"all", "BOOL_AND"},
            {"approximate_median", "APPROX_QUANTILE"}
        };

        auto it = agg_map.find(agg_name);
        if (it != agg_map.end()) {
            return it->second;
        }

        // Default: return uppercase version
        std::string upper = agg_name;
        std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
        return upper;
    }

    // Helper to build GROUP BY SQL for DuckDB
    std::string buildGroupBySQL(
        const std::vector<std::string>& group_columns,
        const std::vector<std::pair<std::string, std::string>>& agg_functions) {

        std::string sql = "SELECT ";

        // Add group columns first
        for (size_t i = 0; i < group_columns.size(); i++) {
            if (i > 0) sql += ", ";
            sql += "\"" + group_columns[i] + "\"";
        }

        // Add aggregated columns
        for (const auto& [agg_name, col_name] : agg_functions) {
            if (!group_columns.empty() || &agg_name != &agg_functions[0].first) {
                sql += ", ";
            }

            std::string sql_func = mapAggregateFunction(agg_name);

            // Special handling for first/last
            if (agg_name == "first" || agg_name == "last") {
                if (agg_name == "first") {
                    sql += "FIRST(\"" + col_name + "\" ORDER BY rowid) AS \"" + col_name + "_" + agg_name + "\"";
                } else {
                    sql += "LAST(\"" + col_name + "\" ORDER BY rowid) AS \"" + col_name + "_" + agg_name + "\"";
                }
            } else if (agg_name == "count_distinct") {
                sql += "COUNT(DISTINCT \"" + col_name + "\") AS \"" + col_name + "_nunique\"";
            } else if (agg_name == "approximate_median") {
                sql += "APPROX_QUANTILE(\"" + col_name + "\", 0.5) AS \"" + col_name + "_" + agg_name + "\"";
            } else {
                sql += sql_func + "(\"" + col_name + "\") AS \"" + col_name + "_" + agg_name + "\"";
            }
        }

        // FROM clause
        sql += " FROM t";

        // GROUP BY clause
        if (!group_columns.empty()) {
            sql += " GROUP BY ";
            for (size_t i = 0; i < group_columns.size(); i++) {
                if (i > 0) sql += ", ";
                sql += "\"" + group_columns[i] + "\"";
            }
        }

        // ORDER BY to preserve order of group keys
        if (!group_columns.empty()) {
            sql += " ORDER BY ";
            for (size_t i = 0; i < group_columns.size(); i++) {
                if (i > 0) sql += ", ";
                sql += "\"" + group_columns[i] + "\"";
            }
        }

        return sql;
    }

    std::string get_placeholder_name(uint64_t index)
    {
        return std::format("__groupby_key_{}__", index);
    }

    bool is_placeholder(std::string const& name)
    {
        std::regex  regex("__groupby_key_(\\d+)__");
        std::smatch match;
        return std::regex_search(name, match, regex);
    }

    //-------------------------------------------------------------------------
    // Grouper implementation
    //-------------------------------------------------------------------------
    Grouper::Grouper(arrow::TablePtr table, std::optional<TimeGrouperOptions> options)
        : m_table(std::move(table))
    {
        AssertFromFormat(m_table, "table cannot be null.");

        if (options)
        {
            m_time_grouper = std::make_optional<TimeGrouper>(*options);
        }
    }

    Groups Grouper::groups()
    {
        return m_groups;
    }

    void Grouper::makeGroups(arrow::ChunkedArrayVector const& keys)
    {
        arrow::ArrayPtr key;
        if (keys.size() == 1)
        {
            auto array = m_time_grouper ? m_time_grouper->apply(keys.at(0)) : keys.at(0);
            key        = factory::array::make_contiguous_array(array);
        }
        else
        {
            arrow::ArrayVector keysAsArray(keys.size());
            std::ranges::transform(keys, keysAsArray.begin(),
                                   [&](arrow::ChunkedArrayPtr const& keyChunkedArray)
                                   {
                                       auto array = m_time_grouper
                                                        ? m_time_grouper->apply(keyChunkedArray)
                                                        : keyChunkedArray;
                                       return factory::array::make_contiguous_array(array);
                                   });
            arrow::FieldVector fields(keys.size());
            std::ranges::transform(m_keys, fields.begin(),
                                   [&](arrow::FieldRef const& keyRef)
                                   {
                                       auto field =
                                           m_table->schema()->GetFieldByName(*keyRef.name());
                                       AssertFromFormat(field, "field cannot be null.");

                                       if (is_placeholder(*keyRef.name()))
                                       {
                                           field = arrow::field("", field->type());
                                       }
                                       return field;
                                   });
            key =
                AssertResultIsOk(factory::array::array_to_struct_single_chunk(keysAsArray, fields));
        }

        ScalarMapping<int64_t>                                indexer;
        std::vector<std::pair<Scalar, std::vector<uint64_t>>> groups;

        groups.reserve(key->length());
        for (size_t i = 0; i < static_cast<size_t>(key->length()); ++i)
        {
            Scalar scalar{key->GetScalar(i).MoveValueUnsafe()};
            if (!indexer.contains(scalar))
            {
                indexer[scalar] = groups.size();
                groups.emplace_back(scalar, std::vector<uint64_t>{});
            }

            groups[indexer[scalar]].second.push_back(i);
        }

        m_groups.resize(groups.size());
        std::ranges::transform(groups, m_groups.begin(),
                               [&](auto const& item)
                               {
                                   auto const& [group_key, indices_list] = item;
                                   arrow::UInt64Builder builder;
                                   AssertStatusIsOk(builder.AppendValues(indices_list));
                                   return std::make_pair(
                                       group_key, std::dynamic_pointer_cast<arrow::UInt64Array>(
                                                      builder.Finish().MoveValueUnsafe()));
                               });
    }

    //-------------------------------------------------------------------------
    // KeyGrouper implementation
    //-------------------------------------------------------------------------
    KeyGrouper::KeyGrouper(arrow::TablePtr table, std::vector<std::string> const& by,
                           std::optional<TimeGrouperOptions> options)
        : Grouper(std::move(table), options)
    {
        for (auto const& key : by)
        {
            auto field = m_table->schema()->GetFieldByName(key);
            AssertFromStream(field != nullptr, "Column not found: " + key);
            m_keys.emplace_back(key);
        }

        std::unordered_set<std::string> by_set{by.begin(), by.end()};
        for (auto const& key : m_table->schema()->field_names())
        {
            if (!by_set.contains(key))
            {
                m_fields.emplace_back(key);
            }
        }
    }

    void KeyGrouper::makeGroups()
    {
        arrow::ChunkedArrayVector result(m_keys.size());
        std::ranges::transform(m_keys, std::ranges::begin(result),
                               [&](auto const& key)
                               {
                                   auto arr = m_table->GetColumnByName(*key.name());
                                   AssertFromStream(arr, "failed to find column: " << *key.name());
                                   return arr;
                               });
        return Grouper::makeGroups(result);
    }

    //-------------------------------------------------------------------------
    // ArrayGrouper implementation
    //-------------------------------------------------------------------------
    ArrayGrouper::ArrayGrouper(arrow::TablePtr table, arrow::ChunkedArrayVector const& by,
                               std::optional<TimeGrouperOptions> options)
        : Grouper(std::move(table), options), m_arr(by)
    {
        for (auto const& key : m_table->schema()->field_names())
        {
            m_fields.emplace_back(key);
        }

        for (auto const& [i, key] : std::views::enumerate(by))
        {
            AssertFromStream(key->length() == m_table->num_rows(),
                             "Key size does not match table rows. key: "
                                 << key->length() << " table: " << m_table->num_rows());
            auto field = get_placeholder_name(i);
            m_keys.emplace_back(field);
            m_table = AssertResultIsOk(
                m_table->AddColumn(m_table->num_columns(), arrow::field(field, key->type()), key));
        }
    }

    void ArrayGrouper::makeGroups()
    {
        Grouper::makeGroups(m_arr);
    }

    //-------------------------------------------------------------------------
    // GroupOperations implementation
    //-------------------------------------------------------------------------
    GroupOperations::GroupOperations(std::shared_ptr<Grouper> grouper)
        : m_grouper(std::move(grouper))
    {
    }

    std::pair<arrow::ChunkedArrayPtr, arrow::TablePtr>
    GroupOperations::filter_key(std::string const& key, arrow::TablePtr const& current_table) const
    {
        auto index = current_table->GetColumnByName(key);
        AssertFromStream(index, "IIndex column not found: " << key);

        int fieldIndex = current_table->schema()->GetFieldIndex(key);
        AssertFromStream(fieldIndex != -1, "Column index not found: " << key);
        return {index, AssertResultIsOk(current_table->RemoveColumn(fieldIndex))};
    }

    DataFrame GroupOperations::agg_table_to_dataFrame(arrow::TablePtr const& result) const
    {
        const auto& keys = m_grouper->keys();
        if (keys.size() == 1)
        {
            auto [index, new_table] = filter_key(*keys[0].name(), result);
            return DataFrame{factory::index::make_index(index, std::nullopt, ""), new_table};
        }

        arrow::ArrayVector indexArrays;
        arrow::FieldVector indexFields;

        arrow::TablePtr new_table = result;
        for (auto const& key : keys)
        {
            auto filter_pair = filter_key(*key.name(), new_table);
            indexArrays.emplace_back(factory::array::make_contiguous_array(filter_pair.first));
            std::string field_name = *key.name();
            if (is_placeholder(field_name))
            {
                field_name = "";
            }
            indexFields.emplace_back(arrow::field(field_name, filter_pair.first->type()));
            new_table = filter_pair.second;
        }

        const auto struct_index = factory::index::make_index(
            AssertResultIsOk(
                factory::array::array_to_struct_single_chunk(indexArrays, indexFields)),
            std::nullopt, "");
        return DataFrame{struct_index, new_table};
    }

    std::unordered_map<std::string, DataFrame>
    GroupOperations::to_dataFrame_map(std::vector<std::string> const& agg_names,
                                      DataFrame const&                result) const
    {
        std::unordered_map<std::string, DataFrame> result_map;
        for (auto const& agg_name : agg_names)
        {
            std::vector<std::string>                     fields;
            std::unordered_map<std::string, std::string> rename_field;
            for (auto const& field : m_grouper->fields())
            {
                auto current_field = std::format("{}_{}", *field.name(), agg_name);
                fields.emplace_back(current_field);
                rename_field[current_field] = *field.name();
            }

            result_map.emplace(agg_name, result[fields].rename(rename_field));
        }
        return result_map;
    }

    //-------------------------------------------------------------------------
    // AggOperations implementation
    //-------------------------------------------------------------------------
    AggOperations::AggOperations(std::shared_ptr<Grouper> grouper)
        : GroupOperations(std::move(grouper))
    {
    }

    arrow::TablePtr
    AggOperations::apply_agg(std::string const&                                      agg_name,
                             const std::shared_ptr<arrow::compute::FunctionOptions>& /* option */) const
    {
        // Get thread-local connection
        auto& conn = getSQLEngineConnection();

        // Handle empty column names by temporarily renaming them
        auto table = m_grouper->table();
        std::unordered_map<std::string, std::string> temp_name_map;

        // Check if any columns have empty names and rename them
        std::vector<std::shared_ptr<arrow::Field>> new_fields;
        bool has_empty_names = false;
        for (int i = 0; i < table->schema()->num_fields(); i++) {
            auto field = table->schema()->field(i);
            if (field->name().empty()) {
                std::string temp_name = std::format("__temp_col_{}__", i);
                temp_name_map[temp_name] = "";
                new_fields.push_back(arrow::field(temp_name, field->type(), field->nullable()));
                has_empty_names = true;
            } else {
                new_fields.push_back(field);
            }
        }

        if (has_empty_names) {
            auto new_schema = arrow::schema(new_fields);
            table = arrow::Table::Make(new_schema, table->columns());
        }

        // Add a rowid column for window functions
        arrow::Int64Builder rowid_builder;
        AssertStatusIsOk(rowid_builder.Reserve(table->num_rows()));
        for (int64_t i = 0; i < table->num_rows(); i++) {
            AssertStatusIsOk(rowid_builder.Append(i));
        }
        auto rowid_array = AssertResultIsOk(rowid_builder.Finish());
        auto rowid_chunked = std::make_shared<arrow::ChunkedArray>(rowid_array);
        table = AssertResultIsOk(table->AddColumn(
            table->num_columns(),
            arrow::field("rowid", arrow::int64()),
            rowid_chunked));

        // Build group columns and aggregation functions
        std::vector<std::string> group_columns;
        for (auto const& key : m_grouper->keys()) {
            if (key.name()) {
                group_columns.push_back(*key.name());
            }
        }

        // Build aggregation functions for all non-group columns
        std::vector<std::pair<std::string, std::string>> agg_functions;
        for (auto const& field : m_grouper->fields()) {
            if (field.name()) {
                std::string col_name = *field.name();
                // If original name was empty, use the temp name
                if (col_name.empty() && has_empty_names) {
                    for (const auto& [temp, orig] : temp_name_map) {
                        if (orig == col_name) {
                            col_name = temp;
                            break;
                        }
                    }
                }
                if (!col_name.empty()) {
                    agg_functions.emplace_back(agg_name, col_name);
                }
            }
        }

        // Build and execute GROUP BY SQL using new API
        std::string sql = buildGroupBySQL(group_columns, agg_functions);
        auto result = conn.query(table, sql);

        return result;
    }

    arrow::TablePtr AggOperations::apply_agg(
        std::vector<std::string> const&                                      agg_names,
        const std::vector<std::shared_ptr<arrow::compute::FunctionOptions>>& /* options */) const
    {
        // Get thread-local connection
        auto& conn = getSQLEngineConnection();

        // Handle empty column names by temporarily renaming them
        auto table = m_grouper->table();
        std::unordered_map<std::string, std::string> temp_name_map;

        // Check if any columns have empty names and rename them
        std::vector<std::shared_ptr<arrow::Field>> new_fields;
        bool has_empty_names = false;
        for (int i = 0; i < table->schema()->num_fields(); i++) {
            auto field = table->schema()->field(i);
            if (field->name().empty()) {
                std::string temp_name = std::format("__temp_col_{}__", i);
                temp_name_map[temp_name] = "";
                new_fields.push_back(arrow::field(temp_name, field->type(), field->nullable()));
                has_empty_names = true;
            } else {
                new_fields.push_back(field);
            }
        }

        if (has_empty_names) {
            auto new_schema = arrow::schema(new_fields);
            table = arrow::Table::Make(new_schema, table->columns());
        }

        // Add a rowid column for window functions
        arrow::Int64Builder rowid_builder2;
        AssertStatusIsOk(rowid_builder2.Reserve(table->num_rows()));
        for (int64_t i = 0; i < table->num_rows(); i++) {
            AssertStatusIsOk(rowid_builder2.Append(i));
        }
        auto rowid_array2 = AssertResultIsOk(rowid_builder2.Finish());
        auto rowid_chunked2 = std::make_shared<arrow::ChunkedArray>(rowid_array2);
        table = AssertResultIsOk(table->AddColumn(
            table->num_columns(),
            arrow::field("rowid", arrow::int64()),
            rowid_chunked2));

        // Build group columns
        std::vector<std::string> group_columns;
        for (auto const& key : m_grouper->keys()) {
            if (key.name()) {
                group_columns.push_back(*key.name());
            }
        }

        // Build aggregation functions for all combinations
        std::vector<std::pair<std::string, std::string>> agg_functions;
        for (auto const& agg_name : agg_names) {
            for (auto const& field : m_grouper->fields()) {
                if (field.name()) {
                    std::string col_name = *field.name();
                    // If original name was empty, use the temp name
                    if (col_name.empty() && has_empty_names) {
                        for (const auto& [temp, orig] : temp_name_map) {
                            if (orig == col_name) {
                                col_name = temp;
                                break;
                            }
                        }
                    }
                    if (!col_name.empty()) {
                        agg_functions.emplace_back(agg_name, col_name);
                    }
                }
            }
        }

        // Build and execute GROUP BY SQL using new API
        std::string sql = buildGroupBySQL(group_columns, agg_functions);
        auto result = conn.query(table, sql);

        return result;
    }

    DataFrame
    AggOperations::agg(std::string const&                                      agg_name,
                       const std::shared_ptr<arrow::compute::FunctionOptions>& option) const
    {
        auto result_table = apply_agg(agg_name, option);

        // Rename columns to remove the aggregation suffix and restore temp names
        std::unordered_map<std::string, std::string> rename_map;

        // First pass: collect all temp name to original name mappings
        std::unordered_map<std::string, std::string> temp_to_orig;
        for (auto const& field : m_grouper->fields()) {
            if (field.name() && field.name()->empty()) {
                // Find corresponding temp name in result_table schema
                for (int i = 0; i < result_table->schema()->num_fields(); i++) {
                    auto result_field = result_table->schema()->field(i);
                    std::string result_name = result_field->name();
                    // Check if it's a temp column with agg suffix
                    if (result_name.starts_with("__temp_col_") && result_name.find("_" + agg_name) != std::string::npos) {
                        temp_to_orig[result_name] = "";
                        break;
                    }
                }
            }
        }

        // Second pass: build rename map
        for (auto const& field : m_grouper->fields()) {
            if (field.name()) {
                std::string col_name = *field.name();

                if (col_name.empty()) {
                    // Find the corresponding aggregated temp column and map it back to empty string
                    for (int i = 0; i < result_table->schema()->num_fields(); i++) {
                        auto result_field = result_table->schema()->field(i);
                        if (temp_to_orig.count(result_field->name())) {
                            rename_map[result_field->name()] = col_name;
                            break;
                        }
                    }
                } else {
                    std::string agg_col_name = std::format("{}_{}", col_name, agg_name);
                    rename_map[agg_col_name] = col_name;
                }
            }
        }

        // Get field names and rename them
        auto schema = result_table->schema();
        std::vector<std::shared_ptr<arrow::Field>> new_fields;
        for (int i = 0; i < schema->num_fields(); i++) {
            auto field = schema->field(i);
            auto it = rename_map.find(field->name());
            if (it != rename_map.end()) {
                new_fields.push_back(arrow::field(it->second, field->type(), field->nullable()));
            } else {
                new_fields.push_back(field);
            }
        }

        auto new_schema = arrow::schema(new_fields);
        result_table = arrow::Table::Make(new_schema, result_table->columns());

        return agg_table_to_dataFrame(result_table);
    }

    std::unordered_map<std::string, DataFrame> AggOperations::agg(
        std::vector<std::string> const&                                      agg_names,
        const std::vector<std::shared_ptr<arrow::compute::FunctionOptions>>& options) const
    {
        return to_dataFrame_map(agg_names, agg_table_to_dataFrame(apply_agg(agg_names, options)));
    }

    //-------------------------------------------------------------------------
    // ApplyOperations implementation
    //-------------------------------------------------------------------------
    ApplyOperations::ApplyOperations(DataFrame const& data, std::shared_ptr<Grouper> grouper,
                                     bool groupKeys)
        : GroupOperations(std::move(grouper)), m_data(data), m_groupKeys(groupKeys)
    {
        m_grouper->makeGroups();
    }

    IndexPtr ApplyOperations::make_apply_index(IndexPtr const&         newIndex,
                                               arrow::ScalarPtr const& groupKey) const
    {
        if (!m_groupKeys)
        {
            return newIndex;
        }

        const auto      indexArray = newIndex->array().value();
        arrow::ArrayPtr mergedArray;
        auto            keys = m_grouper->keys();

        if (is_nested(groupKey->type->id()))
        {
            auto structScalar = std::dynamic_pointer_cast<arrow::StructScalar>(groupKey);
            AssertFromStream(structScalar, "struct scalar is null");
            std::vector<arrow::ArrayPtr> structArrays;
            std::vector<std::string>     fieldNames;
            for (auto const& key : keys)
            {
                auto scalar = AssertResultIsOk(structScalar->field(key));
                auto structArray =
                    AssertResultIsOk(arrow::MakeArrayFromScalar(*scalar, indexArray->length()));
                structArrays.push_back(structArray);
                fieldNames.push_back(is_placeholder(*key.name()) ? "" : *key.name());
            }
            structArrays.push_back(indexArray);
            fieldNames.emplace_back(newIndex->name());
            mergedArray = AssertResultIsOk(arrow::StructArray::Make(structArrays, fieldNames));
        }
        else
        {
            auto repeatKeyArray =
                AssertResultIsOk(arrow::MakeArrayFromScalar(*groupKey, indexArray->length()));
            std::string field_name = *keys.at(0).name();
            if (is_placeholder(field_name))
            {
                field_name = "";
            }
            mergedArray = AssertResultIsOk(arrow::StructArray::Make(
                {repeatKeyArray, newIndex->array().value()}, {field_name, newIndex->name()}));
        }
        return factory::index::make_index(mergedArray, std::nullopt, "");
    }

    Series ApplyOperations::apply(std::function<Scalar(DataFrame const&)> const& fn) const
    {
        const auto          groups = m_grouper->groups();
        std::vector<Scalar> index(groups.size());
        arrow::ScalarVector values(groups.size());

        std::transform(groups.begin(), groups.end(), values.begin(), index.begin(),
                       [&](auto const& group, arrow::ScalarPtr& value)
                       {
                           const DataFrame df = m_data.iloc(Array{group.second});
                           value              = fn(df).value();
                           return group.first;
                       });

        const IndexPtr index_ptr = factory::index::make_index(
            factory::array::make_contiguous_array(index, index.front().type()), std::nullopt, "");
        const auto arr_ptr = factory::array::make_array(values, values.front()->type);
        return Series(index_ptr, arr_ptr);
    }

    Series ApplyOperations::apply(std::function<Series(DataFrame const&)> const& fn) const
    {
        const Groups               groups = m_grouper->groups();
        std::vector<FrameOrSeries> values(groups.size());

        std::ranges::transform(groups, values.begin(),
                               [&](auto const& group)
                               {
                                   const DataFrame df          = m_data.iloc(Array(group.second));
                                   const Series    valueSeries = fn(df);
                                   return Series(
                                       make_apply_index(valueSeries.index(), group.first.value()),
                                       valueSeries.array());
                               });

        const DataFrame concatenated = concat(
            ConcatOptions{.frames = values, .joinType = JoinType::Outer, .axis = AxisType::Row});
        return concatenated.to_series();
    }

    DataFrame ApplyOperations::apply(std::function<DataFrame(DataFrame const&)> const& fn) const
    {
        const auto                 groups = m_grouper->groups();
        std::vector<FrameOrSeries> values(groups.size());
        std::transform(groups.begin(), groups.end(), values.begin(),
                       [&](auto const& group)
                       {
                           const DataFrame df             = m_data.iloc(Array(group.second));
                           const auto      valueDataFrame = fn(df);
                           return DataFrame(
                               make_apply_index(valueDataFrame.index(), group.first.value()),
                               valueDataFrame.table());
                       });

        return concat(
            ConcatOptions{.frames = values, .joinType = JoinType::Outer, .axis = AxisType::Row});
    }

    DataFrame
    ApplyOperations::apply(std::function<arrow::TablePtr(DataFrame const&)> const& fn) const
    {
        const auto                 groups = m_grouper->groups();
        std::vector<FrameOrSeries> values(groups.size());
        std::transform(groups.begin(), groups.end(), values.begin(),
                       [&](auto const& group)
                       {
                           const DataFrame df    = m_data.iloc(Array(group.second));
                           const auto      table = fn(df);
                           AssertFromStream(table, "table is null");
                           AssertFromStream(table->num_rows() == 1, "table must have 1 row");
                           return DataFrame(factory::index::make_index(
                                                AssertResultIsOk(arrow::MakeArrayFromScalar(
                                                    *group.first.value(), 1)),
                                                std::nullopt, ""),
                                            table);
                       });

        return concat(
            ConcatOptions{.frames = values, .joinType = JoinType::Outer, .axis = AxisType::Row});
    }

    //-------------------------------------------------------------------------
    // GroupByAgg implementation
    //-------------------------------------------------------------------------
    template <typename OutputType>
    GroupByAgg<OutputType>::GroupByAgg(std::shared_ptr<Grouper>       grouper,
                                       std::shared_ptr<AggOperations> operations)
        : m_grouper(std::move(grouper)), m_operations(std::move(operations))
    {
    }

    template <typename OutputType>
    OutputType GroupByAgg<OutputType>::agg(
        std::string const&                                      agg_name,
        const std::shared_ptr<arrow::compute::FunctionOptions>& option) const
    {
        DataFrame dataFrame = m_operations->agg(agg_name, option);
        if constexpr (std::is_same_v<OutputType, DataFrame>)
        {
            return dataFrame;
        }
        else
        {
            return dataFrame.to_series();
        }
    }

    template <typename OutputType>
    std::unordered_map<std::string, OutputType> GroupByAgg<OutputType>::agg(
        std::vector<std::string> const&                                      agg_names,
        const std::vector<std::shared_ptr<arrow::compute::FunctionOptions>>& options) const
    {
        std::unordered_map<std::string, DataFrame> dataFrame =
            m_operations->agg(agg_names, options);

        if constexpr (std::is_same_v<OutputType, DataFrame>)
        {
            return dataFrame;
        }
        else
        {
            std::unordered_map<std::string, OutputType> result;
            for (auto const& [key, value] : dataFrame)
            {
                result[key] = value.to_series();
            }
            return result;
        }
    }

    //-------------------------------------------------------------------------
    // GroupByApply implementation
    //-------------------------------------------------------------------------
    GroupByApply::GroupByApply(std::shared_ptr<Grouper>         grouper,
                               std::shared_ptr<ApplyOperations> operations)
        : m_grouper(std::move(grouper)), m_operations(std::move(operations))
    {
    }

    Groups GroupByApply::groups() const
    {
        return m_grouper->groups();
    }

    Series GroupByApply::apply(std::function<Scalar(DataFrame const&)> const& fn) const
    {
        return m_operations->apply(fn);
    }

    Series GroupByApply::apply(std::function<Series(DataFrame const&)> const& fn) const
    {
        return m_operations->apply(fn);
    }

    DataFrame GroupByApply::apply(std::function<DataFrame(DataFrame const&)> const& fn) const
    {
        return m_operations->apply(fn);
    }

    DataFrame GroupByApply::apply(std::function<arrow::TablePtr(DataFrame const&)> const& fn) const
    {
        return m_operations->apply(fn);
    }

    template class GroupByAgg<DataFrame>;
    template class GroupByAgg<Series>;

    //-------------------------------------------------------------------------
    // Factory functions implementation
    //-------------------------------------------------------------------------
    namespace factory::group_by
    {
        template <typename OutputType>
        GroupByAgg<OutputType> make_agg_by_key(arrow::TablePtr                   table,
                                               std::vector<std::string> const&   by,
                                               std::optional<TimeGrouperOptions> options)
        {
            auto grouper    = std::make_shared<KeyGrouper>(std::move(table), by, options);
            auto operations = std::make_shared<AggOperations>(grouper);
            return GroupByAgg<OutputType>(grouper, operations);
        }

        template <typename OutputType>
        GroupByAgg<OutputType> make_agg_by_array(arrow::TablePtr                   table,
                                                 arrow::ChunkedArrayVector const&  by,
                                                 std::optional<TimeGrouperOptions> options)
        {
            auto grouper    = std::make_shared<ArrayGrouper>(std::move(table), by, options);
            auto operations = std::make_shared<AggOperations>(grouper);
            return GroupByAgg<OutputType>(grouper, operations);
        }

        template <typename OutputType>
        GroupByAgg<OutputType> make_agg_by_index(DataFrame const&          table,
                                                 const TimeGrouperOptions& options)
        {
            TimeGrouper time_grouper(options);
            auto        datetime_index = std::dynamic_pointer_cast<DateTimeIndex>(table.index());
            AssertFromStream(datetime_index, "Index is not a DateTimeIndex");
            auto resampled = time_grouper.apply(*datetime_index);
            return make_agg_by_array<OutputType>(table.table(), {resampled}, options);
        }

        GroupByApply make_apply_by_key(DataFrame const& table, std::vector<std::string> const& by,
                                       bool groupKeys, std::optional<TimeGrouperOptions> options)
        {
            auto       grouper    = std::make_shared<KeyGrouper>(table.table(), by, options);
            const auto operations = std::make_shared<ApplyOperations>(table, grouper, groupKeys);
            return GroupByApply(grouper, operations);
        }

        GroupByApply make_apply_by_array(DataFrame const&                 table,
                                         arrow::ChunkedArrayVector const& by, bool groupKeys,
                                         std::optional<TimeGrouperOptions> options)
        {
            auto       grouper    = std::make_shared<ArrayGrouper>(table.table(), by, options);
            const auto operations = std::make_shared<ApplyOperations>(table, grouper, groupKeys);
            return GroupByApply(grouper, operations);
        }

        GroupByApply make_apply_by_index(DataFrame const& table, bool groupKeys,
                                         const TimeGrouperOptions& options)
        {
            TimeGrouper time_grouper(options);
            auto        datetime_index = std::dynamic_pointer_cast<DateTimeIndex>(table.index());
            AssertFromStream(datetime_index, "Index is not a DateTimeIndex");
            auto resampled = time_grouper.apply(*datetime_index);
            return make_apply_by_array(table, {resampled}, groupKeys, std::nullopt);
        }

        template GroupByAgg<DataFrame> make_agg_by_key(arrow::TablePtr                   table,
                                                       std::vector<std::string> const&   by,
                                                       std::optional<TimeGrouperOptions> options);
        template GroupByAgg<DataFrame> make_agg_by_array(arrow::TablePtr                   table,
                                                         arrow::ChunkedArrayVector const&  by,
                                                         std::optional<TimeGrouperOptions> options);
        template GroupByAgg<DataFrame> make_agg_by_index(DataFrame const&          table,
                                                         const TimeGrouperOptions& options);

        template GroupByAgg<Series> make_agg_by_key(arrow::TablePtr                   table,
                                                    std::vector<std::string> const&   by,
                                                    std::optional<TimeGrouperOptions> options);
        template GroupByAgg<Series> make_agg_by_array(arrow::TablePtr                   table,
                                                      arrow::ChunkedArrayVector const&  by,
                                                      std::optional<TimeGrouperOptions> options);
        template GroupByAgg<Series> make_agg_by_index(DataFrame const&          table,
                                                      const TimeGrouperOptions& options);
    } // namespace factory::group_by
} // namespace epoch_frame
