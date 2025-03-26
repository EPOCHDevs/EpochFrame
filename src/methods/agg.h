//
// Created by adesola on 2/13/25.
//

#pragma once
#include <epoch_frame/enums.h>
#include "method_base.h"
#include "epoch_frame/aliases.h"
#include "common/series_or_scalar.h"
#include <string>


namespace epoch_frame {
    class Aggregator : public MethodBase {
    public:
        explicit Aggregator(const TableComponent&  data);

        SeriesOrScalar agg(AxisType axis, std::string const& agg, bool skip_null, arrow::compute::FunctionOptions const & options) const;

        SeriesOrScalar agg(AxisType axis, std::string const& agg_name, bool skip_null, int64_t min_count=1) const {
            return agg(axis, agg_name, skip_null, arrow::compute::ScalarAggregateOptions(skip_null, min_count));
        }

        SeriesOrScalar all(AxisType axis=AxisType::Row, bool skip_null=true, int64_t min_count=1) const {
            return agg(axis, "all", skip_null, min_count);
        }

        SeriesOrScalar any(AxisType axis=AxisType::Row, bool skip_null=true, int64_t min_count=1) const {
            return agg(axis, "any", skip_null, min_count);
        }

        SeriesOrScalar approximate_median(AxisType axis=AxisType::Row, bool skip_null=true, int64_t min_count=1) const {
            return agg(axis, "approximate_median", skip_null, min_count);
        }

        SeriesOrScalar count(arrow::compute::CountOptions const & options, AxisType axis=AxisType::Row) const {
            return agg(axis, "count", options.mode == arrow::compute::CountOptions::ONLY_VALID, options);
        }

        SeriesOrScalar first(AxisType axis=AxisType::Row, bool skip_null=true, int64_t min_count=1) const {
            return agg(axis, "first", skip_null, min_count);
        }

        SeriesOrScalar last(AxisType axis=AxisType::Row, bool skip_null=true, int64_t min_count=1) const {
            return agg(axis, "last", skip_null, min_count);
        }

        SeriesOrScalar index(Scalar const &scalar, AxisType axis=AxisType::Row) const {
            arrow::compute::IndexOptions index_options{scalar.value()};
            return agg(axis, "index", true, index_options);
        }

        SeriesOrScalar max(AxisType axis=AxisType::Row, bool skip_null=true, int64_t min_count=1) const {
            return agg(axis, "max", skip_null, min_count);
        }

        SeriesOrScalar min(AxisType axis=AxisType::Row, bool skip_null=true, int64_t min_count=1) const {
            return agg(axis, "min", skip_null, min_count);
        }

        SeriesOrScalar mean(AxisType axis=AxisType::Row, bool skip_null=true, int64_t min_count=1) const {
            return agg(axis, "mean", skip_null, min_count);
        }

        FrameOrSeries mode(AxisType axis=AxisType::Row, bool skip_null=true, int64_t n=1) const;

        SeriesOrScalar product(arrow::compute::ScalarAggregateOptions const & options,
            AxisType axis=AxisType::Row) const {
            return agg(axis, "product", options.skip_nulls, options);
        }

        SeriesOrScalar quantile(arrow::compute::QuantileOptions const & options, AxisType axis=AxisType::Row) const {
            return agg(axis, "quantile", options.skip_nulls, options);
        }

        SeriesOrScalar stddev(arrow::compute::VarianceOptions const & options, AxisType axis=AxisType::Row) const {
            return agg(axis, "stddev", options.skip_nulls, options);
        }

        SeriesOrScalar sum(AxisType axis=AxisType::Row, bool skip_null=true, int64_t min_count=1) const {
            return agg(axis, "sum", skip_null, min_count);
        }

        SeriesOrScalar tdigest(arrow::compute::TDigestOptions const & options, AxisType axis=AxisType::Row) const {
            return agg(axis, "tdigest", options.skip_nulls, options);
        }

        SeriesOrScalar variance(arrow::compute::VarianceOptions const & options, AxisType axis=AxisType::Row) const {
            return agg(axis, "variance", options.skip_nulls, options);
        }
    };
}
