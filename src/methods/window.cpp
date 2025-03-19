#include "window.h"
#include "common/python_utils.h"
#include "index/index.h"
#include "index/datetime_index.h"
#include "epochframe/common.h"
#include "epochframe/dataframe.h"
#include <tbb/parallel_for.h>
#include "common/user_defined_compute_functions.h"
#include "factory/dataframe_factory.h"


namespace epochframe
{

    namespace window
    {
        RollingWindow::RollingWindow(const RollingWindowOptions& options)
            : m_window_size(options.window_size),
              m_min_periods(options.min_periods.value_or(m_window_size)), m_center(options.center),
              m_closed(options.closed), m_step(options.step)
        {
            AssertWithTraceFromStream(m_step > 0, "Step must be greater than 0, got " << m_step);
        }

        WindowBounds RollingWindow::get_window_bounds(uint64_t num_values) const
        {
            auto offset = static_cast<IndexType>(
                m_center or m_window_size == 0 ? floor_div(m_window_size - 1, 2) : 0);

            WindowBounds bounds(num_values);

            auto start_value = static_cast<IndexType>(1 + offset);
            auto end_value   = static_cast<IndexType>(num_values + 1);
            std::ranges::transform(
                std::ranges::views::iota(start_value, end_value), bounds.begin(),
                [&](auto const& n)
                {
                    auto end   = static_cast<IndexType>(start_value + n * m_step);
                    auto start = static_cast<IndexType>(end - m_window_size);
                    return WindowBound{
                        std::clamp<IndexType>(
                            (m_closed == EpochFrameRollingWindowClosedType::Both ||
                             m_closed == EpochFrameRollingWindowClosedType::Left)
                                ? start - 1
                                : start,
                            0, num_values),
                        std::clamp<IndexType>(
                            (m_closed == EpochFrameRollingWindowClosedType::Neither ||
                             m_closed == EpochFrameRollingWindowClosedType::Left)
                                ? end - 1
                                : end,
                            0, num_values)};
                });
            return bounds;
        }

        ExpandingWindow::ExpandingWindow(const ExpandingWindowOptions& options) : m_options(options)
        {
        }

        WindowBounds ExpandingWindow::get_window_bounds(uint64_t num_values) const
        {
            WindowBounds bounds(num_values);
            std::ranges::transform(std::ranges::views::iota(static_cast<IndexType>(1),
                                                            static_cast<IndexType>(num_values + 1)),
                                   bounds.begin(), [&](IndexType n) { return WindowBound{0, n}; });
            return bounds;
        }
    } // namespace window

    AggRollingWindowOperations::AggRollingWindowOperations(window::WindowBoundGeneratorPtr generator, DataFrame const& data)
        : m_generator(std::move(generator)), m_data(data)
    {
        AssertWithTraceFromStream(m_generator != nullptr, "Window generator is nullptr");
    }

    DataFrame AggRollingWindowOperations::agg(std::string const& agg_name,
                                                   bool skip_null) const
    {
        auto bounds = m_generator->get_window_bounds(m_data.size());
        std::vector<FrameOrSeries> results(bounds.size());
        tbb::parallel_for(tbb::blocked_range<size_t>(0, bounds.size()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto window = m_data.iloc({bounds[i].start, bounds[i].end});
                results[i] = window.agg(AxisType::Column, agg_name, skip_null).transpose();
            }
        });

        auto concatenated = concat({
            .frames = results,
            .axis = AxisType::Row
        });
        if (m_data.size() != concatenated.index()->size()) {
            return concatenated.reindex(m_data.index());
        }
        return concatenated;
    }

    std::unordered_map<std::string, DataFrame> AggRollingWindowOperations::agg(std::vector<std::string> const& agg_names, bool skip_null) const
    {
        std::unordered_map<std::string, DataFrame> results;
        tbb::parallel_for(tbb::blocked_range<size_t>(0, agg_names.size()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto agg_name = agg_names[i];
                results[agg_name] = agg(agg_name, skip_null);
            }
        });
        return results;
    }

    ApplyRollingWindowOperations::ApplyRollingWindowOperations(window::WindowBoundGeneratorPtr generator, DataFrame const& data)
        : m_generator(std::move(generator)), m_data(data)
    {
        AssertWithTraceFromStream(m_generator != nullptr, "Window generator is nullptr");
    }

    Series ApplyRollingWindowOperations::apply(std::function<Scalar(DataFrame const&)> const& fn) const
    {
        auto bounds = m_generator->get_window_bounds(m_data.size());
        if (bounds.size() == 0) {
            return Series();
        }

        std::vector<arrow::ScalarPtr> results(bounds.size());
        tbb::parallel_for(tbb::blocked_range<size_t>(0, bounds.size()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto window = m_data.iloc({bounds[i].start, bounds[i].end});
                results[i] = fn(window).value();
            }
        });

        Series series(m_data.index()->iloc({bounds[0].start, bounds[bounds.size() - 1].end}), factory::array::make_array(results, results.front()->type));
        if (m_data.size() != series.size()) {
            return series.reindex(m_data.index());
        }
        return series;
    }

    Series ApplyRollingWindowOperations::apply(std::function<Series(DataFrame const&)> const& fn) const
    {
        auto bounds = m_generator->get_window_bounds(m_data.size());
        if (bounds.size() == 0) {
            return Series();
        }

        std::vector<FrameOrSeries> results(bounds.size());
        tbb::parallel_for(tbb::blocked_range<size_t>(0, bounds.size()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto window = m_data.iloc({bounds[i].start, bounds[i].end});
                results[i] = fn(window);
            }
        });

        auto concatenated = concat({
            .frames = results,
            .axis = AxisType::Row
        }).to_series();
        if (m_data.size() != concatenated.size()) {
            return concatenated.reindex(m_data.index());
        }
        return concatenated;
    }

    DataFrame ApplyRollingWindowOperations::apply(std::function<Series(Series const&)> const& fn) const
    {
        auto bounds = m_generator->get_window_bounds(m_data.size());
        if (bounds.size() == 0) {
            return DataFrame();
        }

        std::vector<FrameOrSeries> results(bounds.size());
        tbb::parallel_for(tbb::blocked_range<size_t>(0, bounds.size()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                std::vector<arrow::ChunkedArrayPtr> series_results;
                series_results.reserve(m_data.num_cols());
                for (auto const& column : m_data.column_names()) {
                    series_results.emplace_back(fn(m_data[column]).array());
                }
                results[i] = make_dataframe(m_data.index(), series_results, m_data.column_names());
            }
        });

        auto concatenated = concat({
            .frames = results,
            .axis = AxisType::Row
        });
        if (m_data.size() != concatenated.size()) {
            return concatenated.reindex(m_data.index());
        }
        return concatenated;
    }

    DataFrame ApplyRollingWindowOperations::apply(std::function<DataFrame(DataFrame const&)> const& fn) const
    {
        auto bounds = m_generator->get_window_bounds(m_data.size());
        if (bounds.size() == 0) {
            return DataFrame();
        }

        std::vector<FrameOrSeries> results(bounds.size());
        tbb::parallel_for(tbb::blocked_range<size_t>(0, bounds.size()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto window = m_data.iloc({bounds[i].start, bounds[i].end});
                results[i] = fn(window);
            }
        });

        auto concatenated = concat({
            .frames = results,
            .axis = AxisType::Row
        });
        if (m_data.size() != concatenated.size()) {
            return concatenated.reindex(m_data.index());
        }
        return concatenated;
    }

    void EWMWindowOperations::calculate_deltas()
    {
        m_deltas = (m_options.times->diff() / Scalar(m_options.halflife->to_nanoseconds())).to_view<double>();
    }

    double EWMWindowOperations::get_center_of_mass(std::optional<double> comass, std::optional<double> const& span, std::optional<TimeDelta> const& halflife,
    std::optional<double> const& alpha) const
    {
        std::vector<bool> valid_counts{comass.has_value(), span.has_value(), halflife.has_value(), alpha.has_value()};
        auto valid_count = std::ranges::count(valid_counts.begin(), valid_counts.end(), true);
        AssertWithTraceFromStream(valid_count <= 1, "Only one of com, span, halflife, or alpha can be specified");

        if (comass) {
            AssertWithTraceFromStream(comass >= 0, "comass must satisfy: comass >= 0");
        }
        else if (span) {
            AssertWithTraceFromStream(span >= 1, "span must satisfy: span >= 1");
        }
        else if (halflife) {
            AssertWithTraceFromStream(halflife->to_microseconds() > 0, "halflife must satisfy: halflife >= 0");
            auto decay = 1.0 - std::exp(std::log(0.5) / halflife->to_nanoseconds());
            comass = 1.0 / (decay - 1.0);
        }
        else if (alpha) {
            AssertWithTraceFromStream(alpha >= 0 && alpha <= 1, "alpha must satisfy: 0 <= alpha <= 1");
            comass = (1.0 - alpha.value()) / alpha.value();
        }
        else {
            AssertWithTraceFromStream(false, "Must pass one of comass, span, halflife, or alpha");
        }
        return comass.value();
    }


    EWMWindowOperations::EWMWindowOperations(const EWMWindowOptions& options, DataFrame const& data)
        : m_options(options), m_data(data)
    {
        if (m_options.times != nullptr) {
            AssertWithTraceFromStream(m_data.num_rows() == m_options.times->size(), "Index must be the same size as the data");
            calculate_deltas();

            if (m_options.com || m_options.alpha || m_options.span) {
                AssertFalseFromStream(m_options.adjust, "None of com, span, or alpha can be specified if "
                        "times is provided and adjust=False");
                m_com = get_center_of_mass(m_options.com, m_options.span, std::nullopt, m_options.alpha);
            }
            else {
                m_options.com = 1.0;
            }
        }
        else {
            m_deltas = std::dynamic_pointer_cast<arrow::DoubleArray>(factory::array::make_contiguous_array(std::vector<double>(std::max(0UL, static_cast<size_t>(m_data.num_rows())-1), 1.0)));
            m_com = get_center_of_mass(m_options.com, m_options.span, m_options.halflife, m_options.alpha);
        }
    }

    DataFrame EWMWindowOperations::agg_ewm(std::string const& agg_name, bool skip_null) const
    {
        return apply([&](arrow::DoubleArray const& values) {
            return Array(ewm(values, m_options.min_periods, m_com, m_options.adjust, m_options.ignore_na, m_deltas, true)).call_aggregate_function(agg_name, skip_null);
        });
    }

    DataFrame EWMWindowOperations::apply(std::function<Scalar(arrow::DoubleArray const&)> const& fn) const
    {
        std::vector<arrow::ChunkedArrayPtr> results(m_data.num_cols());

        auto column_names = m_data.column_names();
        tbb::parallel_for(tbb::blocked_range<size_t>(0, m_data.num_cols()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto column_name = column_names[i];
                auto values = m_data[column_name].contiguous_array().cast(arrow::float64()).to_view<double>();
                results[i] = std::make_shared<arrow::ChunkedArray>(fn(*values));
            }
        });

        return make_dataframe(m_data.index(), results, m_data.column_names());
    }

    DataFrame EWMWindowOperations::mean() const
    {
        return agg_ewm("mean");
    }

    DataFrame EWMWindowOperations::sum() const
    {
        return agg_ewm("sum");
    }

    DataFrame EWMWindowOperations::var(bool bias) const
    {
        return apply([&](arrow::DoubleArray const& values) {
            auto cov = ewmcov(values, m_options.min_periods, values, m_com, m_options.adjust, m_options.ignore_na, bias);
            arrow::compute::VarianceOptions options{1};
            auto var  = arrow::compute::Variance(cov, options);
            return AssertContiguousArrayResultIsOk(var);
        });
    }

    DataFrame EWMWindowOperations::std(bool bias) const
    {
        return zsqrt(var(bias)).frame();
    }
    
} // namespace epochframe
