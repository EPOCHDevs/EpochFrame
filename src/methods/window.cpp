#include "window.h"

#include <iostream>

#include "common/EpochThreadPool.h"
#include "common/python_utils.h"
#include "common/user_defined_compute_functions.h"
#include "epoch_frame/common.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/table_factory.h"
#include "epoch_frame/index.h"
#include "index/datetime_index.h"
#include <tbb/parallel_for.h>

namespace epoch_frame
{

    namespace window
    {
        RollingWindow::RollingWindow(const RollingWindowOptions& options)
            : m_window_size(options.window_size),
              m_min_periods(options.min_periods.value_or(m_window_size)), m_center(options.center),
              m_closed(options.closed), m_step(options.step)
        {
            AssertFromStream(m_step == 1, "epoch_frame only supports step == 1");
        }

        WindowBounds RollingWindow::get_window_bounds(uint64_t n) const
        {
            auto num_values = static_cast<int64_t>(n);
            auto offset     = static_cast<int64_t>(
                m_center or m_window_size == 0 ? floor_div(m_window_size - 1, 2) : 0);

            WindowBounds bounds(num_values);

            auto start_value = 1 + offset;
            auto end_value   = num_values + 1 + offset;

            auto fn = [&](auto end)
            {
                auto start = end - m_window_size;
                start =
                    std::clamp<int64_t>((m_closed == epoch_core::RollingWindowClosedType::Both ||
                                         m_closed == epoch_core::RollingWindowClosedType::Left)
                                            ? start - 1
                                            : start,
                                        0, num_values);
                end =
                    std::clamp<int64_t>((m_closed == epoch_core::RollingWindowClosedType::Neither ||
                                         m_closed == epoch_core::RollingWindowClosedType::Left)
                                            ? end - 1
                                            : end,
                                        0, num_values);
                return WindowBound{start, end};
            };

            if (m_step == 1)
            {
                std::ranges::transform(std::ranges::views::iota(start_value, end_value),
                                       bounds.begin(), fn);
            }
            else
            {
                std::vector<int64_t> indices(num_values);
                auto                 ptr = indices.begin();
                for (int64_t i = start_value; i < end_value; i += m_step)
                {
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
            std::ranges::transform(
                std::ranges::views::iota(1L, static_cast<int64_t>(num_values + 1)), bounds.begin(),
                [&](int64_t n) { return WindowBound{0, n}; });
            return bounds;
        }
    } // namespace window

    template <bool is_dataframe>
    AggRollingWindowOperations<is_dataframe>::AggRollingWindowOperations(
        window::WindowBoundGeneratorPtr generator, FrameType<is_dataframe> const& data)
        : m_generator(std::move(generator)), m_data(data)
    {
        AssertFromStream(m_generator != nullptr, "Window generator is nullptr");
    }

    template <bool is_dataframe>
    FrameType<is_dataframe> AggRollingWindowOperations<is_dataframe>::agg(
        std::string const& agg_name, bool skip_null,
        const arrow::compute::FunctionOptions& options) const
    {
        const auto bounds = m_generator->get_window_bounds(m_data.size());

        std::vector<std::conditional_t<is_dataframe, arrow::TablePtr, arrow::ScalarPtr>> results(
            bounds.size());
        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0, bounds.size()),
                    [&](tbb::blocked_range<size_t> const& range)
                    {
                        for (size_t i = range.begin(); i < range.end(); ++i)
                        {
                            auto bound = bounds[i];
                            if (bound.start == bound.end)
                            {
                                if constexpr (is_dataframe)
                                {
                                    results[i] = factory::table::make_null_table(
                                        m_data.table()->schema(), 1);
                                }
                                else
                                {
                                    results[i] = Scalar{}.value();
                                }
                                continue;
                            }

                            auto window = m_data.iloc({bound.start, bound.end});
                            auto result = window.agg(AxisType::Row, agg_name, skip_null, options);
                            if constexpr (is_dataframe)
                            {
                                auto s     = result.transpose(nullptr);
                                results[i] = s.table();
                            }
                            else
                            {
                                results[i] = result.value();
                            }
                        }
                    },
                    tbb::simple_partitioner());
            });

        if constexpr (is_dataframe)
        {
            return DataFrame{m_data.index(),
                             AssertTableResultIsOk(arrow::ConcatenateTables(results))};
        }
        else
        {
            return Series(m_data.index(),
                          factory::array::make_chunked_array(results, results.front()->type));
        }
    }

    template <bool is_dataframe>
    std::unordered_map<std::string, FrameType<is_dataframe>>
    AggRollingWindowOperations<is_dataframe>::agg(
        std::vector<std::string> const& agg_names, bool skip_null,
        const arrow::compute::FunctionOptions& options) const
    {
        std::unordered_map<std::string, FrameType<is_dataframe>> results;
        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0, agg_names.size()),
                    [&](tbb::blocked_range<size_t> const& range)
                    {
                        for (size_t i = range.begin(); i < range.end(); ++i)
                        {
                            auto agg_name     = agg_names[i];
                            results[agg_name] = agg(agg_name, skip_null, options);
                        }
                    },
                    tbb::simple_partitioner());
            });
        return results;
    }

    template <class FrameType>
    Series apply_scalar_to_series(std::function<Scalar(FrameType const&)> const& fn,
                                  FrameType const&                               data,
                                  const window::WindowBoundGeneratorPtr&         generator)
    {
        auto bounds = generator->get_window_bounds(data.size());
        if (bounds.size() == 0)
        {
            return Series();
        }

        std::vector<arrow::ScalarPtr> results(bounds.size());
        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0, bounds.size()),
                    [&](tbb::blocked_range<size_t> const& range)
                    {
                        for (size_t i = range.begin(); i < range.end(); ++i)
                        {
                            auto window = data.iloc({bounds[i].start, bounds[i].end});
                            results[i]  = fn(window).value();
                        }
                    },
                    tbb::simple_partitioner());
            });

        return Series(data.index(), factory::array::make_array(results, results.front()->type));
    }

    ApplyDataFrameRollingWindowOperations::ApplyDataFrameRollingWindowOperations(
        window::WindowBoundGeneratorPtr generator, DataFrame const& data)
        : m_generator(std::move(generator)), m_data(data)
    {
        AssertFromStream(m_generator != nullptr, "Window generator is nullptr");
    }

    Series ApplyDataFrameRollingWindowOperations::apply(
        std::function<Scalar(DataFrame const&)> const& fn) const
    {
        return apply_scalar_to_series(fn, m_data, m_generator);
    }

    Series ApplyDataFrameRollingWindowOperations::apply(
        std::function<Series(DataFrame const&)> const& fn) const
    {
        auto bounds = m_generator->get_window_bounds(m_data.size());
        if (bounds.size() == 0)
        {
            return Series();
        }

        std::vector<FrameOrSeries> results(bounds.size());
        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0, bounds.size()),
                    [&](tbb::blocked_range<size_t> const& range)
                    {
                        for (size_t i = range.begin(); i < range.end(); ++i)
                        {
                            auto window = m_data.iloc({bounds[i].start, bounds[i].end});
                            results[i]  = fn(window);
                        }
                    },
                    tbb::simple_partitioner());
            });

        auto concatenated = concat({.frames = results, .axis = AxisType::Row}).to_series();
        return concatenated.reindex(m_data.index());
    }

    DataFrame ApplyDataFrameRollingWindowOperations::apply(
        std::function<Series(Series const&)> const& fn) const
    {
        auto bounds = m_generator->get_window_bounds(m_data.size());
        if (bounds.size() == 0)
        {
            return DataFrame();
        }

        std::vector<FrameOrSeries> results(bounds.size());
        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0, bounds.size()),
                    [&](tbb::blocked_range<size_t> const& range)
                    {
                        for (size_t i = range.begin(); i < range.end(); ++i)
                        {
                            std::vector<arrow::ChunkedArrayPtr> series_results;
                            series_results.reserve(m_data.num_cols());
                            for (auto const& column : m_data.column_names())
                            {
                                series_results.emplace_back(fn(m_data[column]).array());
                            }
                            results[i] = make_dataframe(m_data.index(), series_results,
                                                        m_data.column_names());
                        }
                    },
                    tbb::simple_partitioner());
            });

        auto concatenated = concat({.frames = results, .axis = AxisType::Row});
        return concatenated.reindex(m_data.index());
    }

    DataFrame ApplyDataFrameRollingWindowOperations::apply(
        std::function<DataFrame(DataFrame const&)> const& fn) const
    {
        auto bounds = m_generator->get_window_bounds(m_data.size());
        if (bounds.size() == 0)
        {
            return DataFrame();
        }

        std::vector<FrameOrSeries> results(bounds.size());
        EpochThreadPool::getInstance().execute(
            [&]
            {
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0, bounds.size()),
                    [&](tbb::blocked_range<size_t> const& range)
                    {
                        for (size_t i = range.begin(); i < range.end(); ++i)
                        {
                            auto window = m_data.iloc({bounds[i].start, bounds[i].end});
                            results[i]  = fn(window);
                        }
                    },
                    tbb::simple_partitioner());
            });

        auto concatenated = concat({.frames = results, .axis = AxisType::Row});
        if (m_data.size() != concatenated.size())
        {
            return concatenated.reindex(m_data.index());
        }
        return concatenated;
    }

    template class AggRollingWindowOperations<true>;
    template class AggRollingWindowOperations<false>;

    ApplySeriesRollingWindowOperations::ApplySeriesRollingWindowOperations(
        window::WindowBoundGeneratorPtr generator, Series const& data)
        : m_generator(std::move(generator)), m_data(data)
    {
        AssertFromStream(m_generator != nullptr, "Window generator is nullptr");
    }

    Series
    ApplySeriesRollingWindowOperations::apply(std::function<Scalar(Series const&)> const& fn) const
    {
        return apply_scalar_to_series(fn, m_data, m_generator);
    }

    Series ApplySeriesRollingWindowOperations::cov(Series const& other, int64_t min_periods,
                                                   int64_t ddof) const
    {
        return apply([&](Series const& s) { return s.cov(other, min_periods, ddof); });
    }

    Series ApplySeriesRollingWindowOperations::corr(Series const& other, int64_t min_periods,
                                                    int64_t ddof) const
    {
        return apply([&](Series const& s) { return s.corr(other, min_periods, ddof); });
    }

    template <bool is_dataframe>
    double EWMWindowOperations<is_dataframe>::get_center_of_mass(
        std::optional<double> comass, std::optional<double> const& span,
        std::optional<TimeDelta> const& halflife, std::optional<double> const& alpha) const
    {
        std::vector<bool> valid_counts{comass.has_value(), span.has_value(), halflife.has_value(),
                                       alpha.has_value()};
        auto valid_count = std::ranges::count(valid_counts.begin(), valid_counts.end(), true);
        AssertFromFormatImpl(std::invalid_argument, valid_count <= 1,
                             "Only one of com, span, halflife, or alpha can be specified");

        if (comass)
        {
            AssertFromFormatImpl(std::invalid_argument, comass >= 0,
                                 "comass must satisfy: comass >= 0");
        }
        else if (span)
        {
            AssertFromFormatImpl(std::invalid_argument, span >= 1, "span must satisfy: span >= 1");
            comass = (span.value() - 1.0) / 2.0;
        }
        else if (halflife)
        {
            throw std::runtime_error("Halflife is not supported yet");
        }
        else if (alpha)
        {
            AssertFromFormatImpl(std::invalid_argument, alpha >= 0 && alpha <= 1,
                                 "alpha must satisfy: 0 <= alpha <= 1");
            comass = (1.0 - alpha.value()) / alpha.value();
        }
        else
        {
            AssertFromFormatImpl(std::invalid_argument, false,
                                 "Must pass one of comass, span, halflife, or alpha");
        }
        return comass.value();
    }

    template <bool is_dataframe>
    EWMWindowOperations<is_dataframe>::EWMWindowOperations(const EWMWindowOptions&        options,
                                                           FrameType<is_dataframe> const& data)
        : m_options(options), m_data(data), m_min_periods(std::max(options.min_periods, 1L))
    {
        m_com = get_center_of_mass(m_options.com, m_options.span, std::nullopt, m_options.alpha);
    }

    template <bool is_dataframe>
    FrameType<is_dataframe> EWMWindowOperations<is_dataframe>::apply_column_wise(
        std::function<arrow::ArrayPtr(arrow::DoubleArray const&)> const& fn) const
    {
        if constexpr (is_dataframe)
        {
            std::vector<arrow::ChunkedArrayPtr> results(m_data.num_cols());
            arrow::FieldVector                  fields;

            auto column_names = m_data.column_names();
            EpochThreadPool::getInstance().execute(
                [&]
                {
                    tbb::parallel_for(
                        tbb::blocked_range<size_t>(0, m_data.num_cols()),
                        [&](tbb::blocked_range<size_t> const& range)
                        {
                            for (size_t i = range.begin(); i < range.end(); ++i)
                            {
                                auto column_name = column_names[i];
                                auto values      = m_data[column_name]
                                                  .contiguous_array()
                                                  .cast(arrow::float64())
                                                  .template to_view<double>();
                                auto result = std::make_shared<arrow::ChunkedArray>(fn(*values));
                                results[i]  = result;
                                fields.push_back(field(column_name, result->type()));
                            }
                        },
                        tbb::simple_partitioner());
                });

            return DataFrame(m_data.index(), arrow::Table::Make(arrow::schema(fields), results));
        }
        else
        {
            auto values =
                m_data.contiguous_array().cast(arrow::float64()).template to_view<double>();
            auto result = std::make_shared<arrow::ChunkedArray>(fn(*values));
            return Series(m_data.index(), result);
        }
    }

    template <bool is_dataframe>
    FrameType<is_dataframe> EWMWindowOperations<is_dataframe>::agg_ewm(bool normalize) const
    {
        return apply_column_wise(
            [&](arrow::DoubleArray const& values)
            {
                return ewm(values, m_min_periods, m_com, m_options.adjust, m_options.ignore_na,
                           m_deltas, normalize);
            });
    }

    template <bool is_dataframe>
    FrameType<is_dataframe> EWMWindowOperations<is_dataframe>::mean() const
    {
        return agg_ewm(true);
    }

    template <bool is_dataframe>
    FrameType<is_dataframe> EWMWindowOperations<is_dataframe>::sum() const
    {
        return agg_ewm(false);
    }

    template <bool is_dataframe>
    FrameType<is_dataframe> EWMWindowOperations<is_dataframe>::var(bool bias) const
    {
        return apply_column_wise(
            [&](arrow::DoubleArray const& values)
            {
                return ewmcov(values, m_min_periods, values, m_com, m_options.adjust,
                              m_options.ignore_na, bias);
            });
    }

    template <bool is_dataframe>
    FrameType<is_dataframe> EWMWindowOperations<is_dataframe>::cov(OutputType const& other,
                                                                   bool              bias) const
    {
        return apply_column_wise(
            [&](arrow::DoubleArray const& values) -> arrow::ArrayPtr
            {
                if constexpr (is_dataframe)
                {
                    throw std::runtime_error("pairwise covariance is not supported for DataFrames");
                }
                else
                {
                    auto arr =
                        other.contiguous_array().cast(arrow::float64()).template to_view<double>();
                    return ewmcov(values, m_min_periods, *arr, m_com, m_options.adjust,
                                  m_options.ignore_na, bias);
                }
            });
    }

    template <bool is_dataframe>
    FrameType<is_dataframe> EWMWindowOperations<is_dataframe>::corr(OutputType const& other) const
    {
        return apply_column_wise(
            [&](arrow::DoubleArray const& values) -> arrow::ArrayPtr
            {
                if constexpr (is_dataframe)
                {
                    throw std::runtime_error(
                        "pairwise correlation is not supported for DataFrames");
                }
                else
                {
                    auto arr =
                        other.contiguous_array().cast(arrow::float64()).template to_view<double>();
                    auto cov   = Array(ewmcov(values, m_min_periods, *arr, m_com, m_options.adjust,
                                              m_options.ignore_na, true));
                    auto x_var = Array(ewmcov(values, m_min_periods, values, m_com,
                                              m_options.adjust, m_options.ignore_na, true));
                    auto y_var = Array(ewmcov(*arr, m_min_periods, *arr, m_com, m_options.adjust,
                                              m_options.ignore_na, true));
                    return (cov / zsqrt(x_var * y_var)).value();
                }
            });
    }

    template <bool is_dataframe>
    FrameType<is_dataframe> EWMWindowOperations<is_dataframe>::std(bool bias) const
    {
        return zsqrt(var(bias));
    }

    template class EWMWindowOperations<true>;
    template class EWMWindowOperations<false>;
} // namespace epoch_frame
