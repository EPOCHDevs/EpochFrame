#include "common/user_defined_compute_functions.h"
#include "common/asserts.h"
#include "epoch_frame/frame_or_series.h"
#include <cmath>
#include <stdexcept>

namespace epoch_frame
{
    std::optional<double> get_value(arrow::DoubleArray const& values, size_t index)
    {
        return values.IsNull(index) ? std::nullopt : std::optional<double>(values.Value(index));
    }

    arrow::ArrayPtr ewm(arrow::DoubleArray const& values, int64_t minp, double com, bool adjust,
                        bool ignore_na, std::shared_ptr<arrow::DoubleArray> const& deltas,
                        bool normalize)
    {
        auto num_values = values.length();
        if (num_values == 0)
        {
            return AssertResultIsOk(arrow::MakeEmptyArray(arrow::float64()));
        }

        arrow::DoubleBuilder builder;
        AssertStatusIsOk(builder.Reserve(num_values));

        bool use_deltas = deltas != nullptr;
        if (use_deltas)
        {
            AssertFromFormat(deltas->null_count() == 0, "Deltas array contains nulls");
        }

        double alpha         = 1. / (1. + com);
        double old_wt_factor = 1. - alpha;
        double new_wt        = adjust ? 1. : alpha;

        auto   weighted       = get_value(values, 0);
        bool   is_observation = weighted.has_value();
        size_t nobs           = static_cast<size_t>(is_observation);

        if (nobs >= static_cast<size_t>(minp) && weighted.has_value())
        {
            builder.UnsafeAppend(*weighted);
        }
        else
        {
            builder.UnsafeAppendNull();
        }
        double old_wt = 1.0;

        for (size_t i = 1; i < num_values; ++i)
        {
            auto optional_cur = get_value(values, i);
            is_observation    = optional_cur.has_value();
            std::optional<double> cur        = optional_cur;
            nobs += is_observation;

            if (weighted.has_value())
            {
                if (is_observation || !ignore_na)
                {
                    if (normalize)
                    {
                        if (use_deltas)
                        {
                            old_wt *= std::pow(old_wt_factor, deltas->Value(i - 1));
                        }
                        else
                        {
                            old_wt *= old_wt_factor;
                        }
                    }
                    else
                    {
                        weighted = old_wt_factor * weighted.value();
                    }

                    if (is_observation)
                    {
                        if (normalize)
                        {
                            // avoid numerical errors on constant series
                            if (*weighted != *cur)
                            {
                                if (!adjust && com == 1)
                                {
                                    // update in case of irregular-interval series
                                    new_wt = 1. - old_wt;
                                }
                                weighted = old_wt * (*weighted) + new_wt * (*cur);
                                *weighted /= (old_wt + new_wt);
                            }
                            if (adjust)
                            {
                                old_wt += new_wt;
                            }
                            else
                            {
                                old_wt = 1.0;
                            }
                        }
                        else
                        {
                            *weighted += *cur;
                        }
                    }
                }
            }
            else if (is_observation)
            {
                weighted = cur;
            }

            if (nobs >= static_cast<size_t>(minp) && weighted.has_value())
            {
                builder.UnsafeAppend(*weighted);
            }
            else
            {
                builder.UnsafeAppendNull();
            }
        }

        return AssertResultIsOk(builder.Finish());
    }

    arrow::ArrayPtr ewmcov(arrow::DoubleArray const& input_x, int64_t minp,
                           arrow::DoubleArray const& input_y, double com, bool adjust,
                           bool ignore_na, bool bias)
    {
        const auto num_values = input_x.length();
        if (input_y.length() != num_values)
        {
            throw std::invalid_argument("arrays are of different lengths, input_x.length() = " +
                                        std::to_string(num_values) +
                                        ", input_y.length() = " + std::to_string(input_y.length()));
        }

        if (num_values == 0)
        {
            return AssertResultIsOk(arrow::MakeEmptyArray(arrow::float64()));
        }

        arrow::DoubleBuilder builder;
        AssertStatusIsOk(builder.Reserve(num_values));

        double alpha         = 1. / (1. + com);
        double old_wt_factor = 1. - alpha;
        double new_wt        = adjust ? 1. : alpha;

        auto   mean_x         = get_value(input_x, 0);
        auto   mean_y         = get_value(input_y, 0);
        bool   is_observation = mean_x.has_value() && mean_y.has_value();
        size_t nobs           = static_cast<size_t>(is_observation);

        if (!is_observation)
        {
            mean_x = std::nullopt;
            mean_y = std::nullopt;
        }

        if (nobs >= static_cast<size_t>(minp) && bias)
        {
            builder.UnsafeAppend(0.);
        }
        else
        {
            builder.UnsafeAppendNull();
        }

        double cov     = 0.;
        double sum_wt  = 1.;
        double sum_wt2 = 1.;
        double old_wt  = 1.;

        for (size_t i = 1; i < num_values; ++i)
        {
            auto cur_x     = get_value(input_x, i);
            auto cur_y     = get_value(input_y, i);
            is_observation = cur_x.has_value() && cur_y.has_value();
            nobs += is_observation;

            if (mean_x.has_value())
            {
                if (is_observation || !ignore_na)
                {
                    sum_wt *= old_wt_factor;
                    sum_wt2 *= (old_wt_factor * old_wt_factor);
                    old_wt *= old_wt_factor;

                    if (is_observation)
                    {
                        double old_mean_x = mean_x.value();
                        double old_mean_y = mean_y.value();

                        // avoid numerical errors on constant series
                        if (mean_x != cur_x)
                        {
                            mean_x =
                                ((old_wt * old_mean_x) + (new_wt * *cur_x)) / (old_wt + new_wt);
                        }

                        // avoid numerical errors on constant series
                        if (mean_y != cur_y)
                        {
                            mean_y =
                                ((old_wt * old_mean_y) + (new_wt * *cur_y)) / (old_wt + new_wt);
                        }

                        cov =
                            ((old_wt * (cov + ((old_mean_x - *mean_x) * (old_mean_y - *mean_y)))) +
                             (new_wt * ((*cur_x - *mean_x) * (*cur_y - *mean_y)))) /
                            (old_wt + new_wt);

                        sum_wt += new_wt;
                        sum_wt2 += (new_wt * new_wt);
                        old_wt += new_wt;

                        if (!adjust)
                        {
                            sum_wt /= old_wt;
                            sum_wt2 /= (old_wt * old_wt);
                            old_wt = 1.;
                        }
                    }
                }
            }
            else if (is_observation)
            {
                mean_x = cur_x;
                mean_y = cur_y;
            }

            if (nobs >= static_cast<size_t>(minp))
            {
                if (!bias)
                {
                    double numerator   = sum_wt * sum_wt;
                    double denominator = numerator - sum_wt2;
                    if (denominator > 0)
                    {
                        builder.UnsafeAppend((numerator / denominator) * cov);
                    }
                    else
                    {
                        builder.UnsafeAppendNull();
                    }
                }
                else
                {
                    builder.UnsafeAppend(cov);
                }
            }
            else
            {
                builder.UnsafeAppendNull();
            }
        }

        return AssertResultIsOk(builder.Finish());
    }

    template<typename T>
    T zsqrt(T const& x)
    {
        auto   result = x.sqrt();
        auto   mask   = result >= Scalar{0};
        Scalar all_valid;
        if constexpr (std::is_same_v<T, Series>)
        {
            all_valid = mask.all(AxisType::Row);
        }
        else if constexpr (std::is_same_v<T, Array>)
        {
            all_valid = Scalar(mask.all());
        }
        else
        {
            all_valid = mask.all(AxisType::Row).all(AxisType::Row);
        }

        if (all_valid.as_bool())
        {
            return result;
        }
        else
        {
            return result.where(mask, Scalar{0});
        }
    }
     template Array zsqrt<Array>(Array const& values);
     template Series zsqrt<Series>(Series const& values);
     template DataFrame zsqrt<DataFrame>(DataFrame const& values);
} // namespace epoch_frame
