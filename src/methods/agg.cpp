//
// Created by adesola on 2/13/25.
//

#include "agg.h"
#include "epoch_frame/frame_or_series.h"
#include "common/arrow_compute_utils.h"
#include "index/arrow_index.h"
#include "common/table_or_array.h"
#include <tbb/parallel_for.h>
#include <fmt/format.h>
#include <string>
#include <memory>
#include <stdexcept>
#include <index/object_index.h>

namespace epoch_frame {
    Aggregator::Aggregator(const TableComponent& data) : MethodBase(data) {}

    SeriesOrScalar Aggregator::agg(AxisType axis, std::string const& agg, bool skip_null, arrow::compute::FunctionOptions const & options) const {
        if (m_data.second.is_chunked_array()) {
            if (m_data.second.size() == 0) {
                return SeriesOrScalar{Scalar{}};
            }

            return SeriesOrScalar{Scalar{arrow_utils::call_unary_agg_compute(m_data.second.chunked_array(), agg, options)}};
        }

        auto table = m_data.second.table();
        if (table->columns().empty() || table->num_rows() == 0) {
            return SeriesOrScalar{Series{}};
        }

        if (axis == AxisType::Row) {
            std::vector<arrow::TablePtr> tables(table->num_columns());
            arrow::FieldVector fields = table->schema()->fields();

            parallel_for(tbb::blocked_range<int64_t>(0, table->num_columns()), [&](const tbb::blocked_range<int64_t>& r) {
                for (int64_t i = r.begin(); i != r.end(); ++i) {
                    auto field = fields[i];
                    auto agg_result = AssertContiguousArrayResultIsOk(arrow::MakeArrayFromScalar(*arrow_utils::call_unary_agg_compute(table->column(i), agg, options), 1));
                    auto index = AssertContiguousArrayResultIsOk(arrow::MakeArrayFromScalar(*arrow::MakeScalar(field->name()), 1));

                    const auto schema = arrow::schema({arrow::field("index", arrow::utf8()), arrow::field("value", field->type())});
                    tables[i] = AssertTableResultIsOk(arrow::Table::Make(schema, arrow::ArrayVector{index, agg_result}));
                }
            });

            auto concat_table = AssertTableResultIsOk(arrow::ConcatenateTables(tables, arrow::ConcatenateTablesOptions{true, arrow::Field::MergeOptions::Permissive()}));
            auto index = factory::array::make_contiguous_array(concat_table->GetColumnByName("index"));
            auto value = concat_table->GetColumnByName("value");
            AssertFromStream(index != nullptr && value != nullptr, "IIndex or value column not found");
            return SeriesOrScalar{Series(std::make_shared<ObjectIndex>(index), value)};
        }
        if (agg == "min" || agg == "max") {
            const arrow::ChunkedArrayVector columns = table->columns();
            arrow::compute::ElementWiseAggregateOptions element_wise_aggregate_options{skip_null};
            std::vector<arrow::Datum> inputs;
            for (const auto &column : columns) {
                inputs.push_back(arrow::Datum(column));
            }
            auto result = CallFunction(::std::format("{}_element_wise", agg), inputs, &element_wise_aggregate_options);

            AssertFromStream(result.ok(), "mode failed: " << result.status().message());
            return SeriesOrScalar(m_data.first, result->chunked_array());
        }

        auto fields = table->schema()->fields();
        std::vector<arrow::ScalarPtr> scalarVector(table->num_rows());
        parallel_for(tbb::blocked_range<int64_t>(0, table->num_rows()), [&](const tbb::blocked_range<int64_t>& r) {
            for (int64_t i = r.begin(); i != r.end(); ++i) {
                    std::vector<arrow::ScalarPtr> row;
                    row.reserve(table->num_columns());

                    for (const auto &column: table->columns()) {
                        row.emplace_back(column->GetScalar(i).MoveValueUnsafe());
                    }

                    auto result = arrow::MakeBuilder(row.front()->type);
                    AssertStatusIsOk(result.status());
                    auto rowBuilder = result.MoveValueUnsafe();
                    AssertStatusIsOk(rowBuilder->AppendScalars(row));
                    auto array = AssertResultIsOk(rowBuilder->Finish());

                    scalarVector[i] = arrow_utils::call_unary_agg_compute(array, agg, options);
            }
        });

        auto builderResult = arrow::MakeBuilder(scalarVector.front()->type);
        AssertStatusIsOk(builderResult.status());

        auto builder = builderResult.MoveValueUnsafe();
        AssertStatusIsOk(builder->AppendScalars(scalarVector));

        auto array = AssertResultIsOk(builder->Finish());
        return {m_data.first, std::make_shared<arrow::ChunkedArray>(array)};
    }

    FrameOrSeries Aggregator::mode(AxisType axis, bool skip_null, int64_t n) const {
        arrow::compute::ModeOptions options{n, skip_null};

        if (m_data.second.is_chunked_array()) {
            auto arr = AssertResultIsOk(Mode(m_data.second.chunked_array(), options)).array_as<arrow::StructArray>();
            auto modeArr = arr->GetFieldByName("mode");
            return FrameOrSeries{Series{modeArr}};
        }

        if (axis == AxisType::Row) {
            auto table = m_data.second.table();
            return FrameOrSeries{DataFrame{arrow_utils::apply_function_to_table(table, [&](arrow::Datum const& datum, std::string const& column) {
                auto result = Mode(datum.chunked_array(), options);
                AssertFromStream(result.ok(), column << ": Mode error: " << result->ToString());
                const auto arr = result->array_as<arrow::StructArray>();
                auto mode = arr->GetFieldByName("mode");
                AssertFromFormat(mode, "mode not found");
                return arrow::Datum{std::make_shared<arrow::ChunkedArray>(mode)};
            })}};
        }

        throw std::runtime_error("Aggregator::mode Not implemented");
    }
}
