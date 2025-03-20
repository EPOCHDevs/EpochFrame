#include "window.h"

#include <iostream>

#include "common/python_utils.h"
#include "index/index.h"
#include "index/datetime_index.h"
#include "epochframe/common.h"
#include "epochframe/dataframe.h"
#include <tbb/parallel_for.h>
#include "factory/dataframe_factory.h"
#include "factory/table_factory.h"


namespace epochframe
{

    namespace window
    {
        RollingWindow::RollingWindow(const RollingWindowOptions& options)
            : m_window_size(options.window_size),
              m_min_periods(options.min_periods.value_or(m_window_size)), m_center(options.center),
              m_closed(options.closed), m_step(options.step)
        {
            AssertFromStream(m_step == 1, "epochframe only supports step == 1");
        }

        WindowBounds RollingWindow::get_window_bounds(uint64_t n) const
        {
            auto num_values = static_cast<int64_t>(n);
            auto offset = static_cast<int64_t>(
                m_center or m_window_size == 0 ? floor_div(m_window_size - 1, 2) : 0);

            WindowBounds bounds(num_values);

            auto start_value = 1 + offset;
            auto end_value   = num_values + 1 + offset;

            auto fn = [&](auto end)
            {
                auto start = end - m_window_size;
                start = std::clamp<int64_t>(
                        (m_closed == EpochFrameRollingWindowClosedType::Both ||
                         m_closed == EpochFrameRollingWindowClosedType::Left)
                            ? start - 1
                            : start,
                        0, num_values);
                end = std::clamp<int64_t>(
                        (m_closed == EpochFrameRollingWindowClosedType::Neither ||
                         m_closed == EpochFrameRollingWindowClosedType::Left)
                            ? end - 1
                            : end,
                        0, num_values);
                return WindowBound{start, end};
            };

            if (m_step == 1) {
                std::ranges::transform(
               std::ranges::views::iota(start_value, end_value), bounds.begin(),
               fn);
            }
            else {
                std::vector<int64_t> indices(num_values);
                auto ptr = indices.begin();
                for (int64_t i = start_value; i < end_value; i+=m_step) {
                    *ptr++ = i;
                }
                std::ranges::transform(indices, bounds.begin(), fn);
            }
            return bounds;
        }

        ExpandingWindow::ExpandingWindow(const ExpandingWindowOptions& options) : m_options(options)
        {
        }

        WindowBounds ExpandingWindow::get_window_bounds(uint64_t num_values) const
        {
            WindowBounds bounds(num_values);
            std::ranges::transform(std::ranges::views::iota(1L,
                                                            static_cast<int64_t>(num_values + 1)),
                                   bounds.begin(), [&](int64_t n) { return WindowBound{0, n}; });
            return bounds;
        }
    } // namespace window

    template<bool is_dataframe>
    AggRollingWindowOperations<is_dataframe>::AggRollingWindowOperations(window::WindowBoundGeneratorPtr generator, FrameType<is_dataframe> const& data)
        : m_generator(std::move(generator)), m_data(data)
    {
        AssertFromStream(m_generator != nullptr, "Window generator is nullptr");
    }

    template<bool is_dataframe>
    FrameType<is_dataframe> AggRollingWindowOperations<is_dataframe>::agg(std::string const& agg_name,
                                                   bool skip_null,
                                                   const arrow::compute::FunctionOptions& options) const {
        const auto bounds = m_generator->get_window_bounds(m_data.size());

        std::vector<std::conditional_t<is_dataframe, arrow::TablePtr, arrow::ScalarPtr>> results(bounds.size());
        tbb::parallel_for(tbb::blocked_range<size_t>(0, bounds.size()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto bound = bounds[i];
                if (bound.start == bound.end) {
                    if constexpr (is_dataframe) {
                        results[i] = factory::table::make_null_table(m_data.table()->schema(), 1);
                    }
                    else {
                        results[i] = Scalar{}.value();
                    }
                    continue;
                }

                auto window = m_data.iloc({bound.start, bound.end});
                auto result = window.agg(AxisType::Row, agg_name, skip_null, options );
                if constexpr (is_dataframe) {
                    auto s = result.transpose();
                    results[i] = s.table();
                } else {
                    results[i] = result.value();
                }
            }
        });

        if constexpr (is_dataframe) {
            return DataFrame{m_data.index(), AssertTableResultIsOk(arrow::ConcatenateTables(results))};
        } else {
            return Series(m_data.index(), factory::array::make_chunked_array(results, results.front()->type));
        }
    }

    template<bool is_dataframe>
    std::unordered_map<std::string, FrameType<is_dataframe>> AggRollingWindowOperations<is_dataframe>::agg(std::vector<std::string> const& agg_names, bool skip_null, const arrow::compute::FunctionOptions& options) const
    {
        std::unordered_map<std::string, FrameType<is_dataframe>> results;
        tbb::parallel_for(tbb::blocked_range<size_t>(0, agg_names.size()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto agg_name = agg_names[i];
                results[agg_name] = agg(agg_name, skip_null, options);
            }
        });
        return results;
    }

    template<class FrameType>
    Series apply_scalar_to_series(std::function<Scalar(FrameType const&)> const& fn, FrameType const& data, const window::WindowBoundGeneratorPtr& generator)
    {
        auto bounds = generator->get_window_bounds(data.size());
        if (bounds.size() == 0) {
            return Series();
        }

        std::vector<arrow::ScalarPtr> results(bounds.size());
        tbb::parallel_for(tbb::blocked_range<size_t>(0, bounds.size()), [&](tbb::blocked_range<size_t> const& range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
                auto window = data.iloc({bounds[i].start, bounds[i].end});
                results[i] = fn(window).value();
            }
        });

        return Series(data.index(), factory::array::make_array(results, results.front()->type));
    }

    ApplyDataFrameRollingWindowOperations::ApplyDataFrameRollingWindowOperations(window::WindowBoundGeneratorPtr generator, DataFrame const& data)
        : m_generator(std::move(generator)), m_data(data)
    {
        AssertFromStream(m_generator != nullptr, "Window generator is nullptr");
    }

    Series ApplyDataFrameRollingWindowOperations::apply(std::function<Scalar(DataFrame const&)> const& fn) const
    {
        return apply_scalar_to_series(fn, m_data, m_generator);
    }

    Series ApplyDataFrameRollingWindowOperations::apply(std::function<Series(DataFrame const&)> const& fn) const
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
        return concatenated.reindex(m_data.index());
    }

    DataFrame ApplyDataFrameRollingWindowOperations::apply(std::function<Series(Series const&)> const& fn) const
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
        return concatenated.reindex(m_data.index());
    }

    DataFrame ApplyDataFrameRollingWindowOperations::apply(std::function<DataFrame(DataFrame const&)> const& fn) const
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

    template class AggRollingWindowOperations<true>;
    template class AggRollingWindowOperations<false>;


    ApplySeriesRollingWindowOperations::ApplySeriesRollingWindowOperations(window::WindowBoundGeneratorPtr generator, Series const& data)
        : m_generator(std::move(generator)), m_data(data)
    {
        AssertFromStream(m_generator != nullptr, "Window generator is nullptr");
    }

    Series ApplySeriesRollingWindowOperations::apply(std::function<Scalar(Series const&)> const& fn) const
    {
        return apply_scalar_to_series(fn, m_data, m_generator);
    }

} // namespace epochframe
