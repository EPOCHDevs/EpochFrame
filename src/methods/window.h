#pragma once
#include "epoch_frame/time_delta.h"
#include "epoch_frame/aliases.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <epoch_core/enum_wrapper.h>
#include <optional>
#include <vector>
#include "epoch_frame/scalar.h"


CREATE_ENUM(EWMWindowType, Alpha, HalfLife, Span, CenterOfMass);
CREATE_ENUM(RollingWindowClosedType, Left, Right, Both, Neither);

namespace epoch_frame
{

    namespace window
    {
        struct WindowBound
        {
            int64_t start{};
            int64_t end{};
        };
        using WindowBounds = std::vector<WindowBound>;
        struct IWindowBoundGenerator
        {
            virtual ~IWindowBoundGenerator()                                  = default;
            virtual WindowBounds get_window_bounds(uint64_t num_values) const = 0;
            virtual int64_t min_periods() const = 0;
        };
        using WindowBoundGeneratorPtr = std::unique_ptr<IWindowBoundGenerator>;

        struct RollingWindowOptions
        {
            int64_t                           window_size;
            std::optional<int64_t>            min_periods{};
            bool                              center{false};
            epoch_core::RollingWindowClosedType closed{epoch_core::RollingWindowClosedType::Null};
            int64_t                           step{1};
        };

        class RollingWindow : public IWindowBoundGenerator
        {
          public:
            RollingWindow(const RollingWindowOptions& options);
            WindowBounds get_window_bounds(uint64_t num_values) const final;
            int64_t min_periods() const final {
                return m_min_periods;
            }

          private:
            int64_t                           m_window_size;
            int64_t                           m_min_periods{};
            bool                              m_center{false};
            epoch_core::RollingWindowClosedType m_closed{epoch_core::RollingWindowClosedType::Null};
            int64_t                           m_step{1};
        };

        struct ExpandingWindowOptions
        {
            double min_periods{1};
        };

        class ExpandingWindow : public IWindowBoundGenerator
        {
          public:
            ExpandingWindow(const ExpandingWindowOptions& options);
            WindowBounds get_window_bounds(uint64_t num_values) const final;

            int64_t min_periods() const final {
                return m_options.min_periods;
            }

          private:
            ExpandingWindowOptions m_options;
        };
    } // namespace window


    template<bool is_dataframe>
    using FrameType = std::conditional_t<is_dataframe, DataFrame, Series>;

        #define __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(name) \
        OutputType name(bool skip_nulls=true) const { \
            const arrow::compute::ScalarAggregateOptions option(skip_nulls, m_generator->min_periods()); \
            return agg(#name, skip_nulls, option); \
        }

    template<bool is_dataframe>
    class AggRollingWindowOperations
    {
      public:
        using OutputType = FrameType<is_dataframe>;
        AggRollingWindowOperations(window::WindowBoundGeneratorPtr generator,
                                   OutputType const&                data);

        // Aggs
        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(all)
        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(any)
        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(approximate_median)
        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(first)

        OutputType index(Scalar const& value) const {
            arrow::compute::IndexOptions options(value.value());
            return agg("index", false, options);
        }

        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(last)
        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(max)
        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(min)
        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(mean)
        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(product)

        OutputType quantile(double q, arrow::compute::QuantileOptions::Interpolation interpolation = arrow::compute::QuantileOptions::Interpolation::LINEAR, bool skip_nulls = true) const {
            arrow::compute::QuantileOptions options(q, interpolation, skip_nulls, m_generator->min_periods());
            return agg("quantile", skip_nulls, options);
        }

        OutputType stddev(int ddof = 1, bool skip_nulls = true) const {
            arrow::compute::VarianceOptions options(ddof, skip_nulls, m_generator->min_periods());
            return agg("stddev", skip_nulls, options);
        }

        __MAKE_WINDOW_SCALAR_AGG_FUNCTION__(sum)

        OutputType tdigest(double q, uint32_t delta = 100, bool skip_nulls = true) const {
            arrow::compute::TDigestOptions options(q, delta, 500, skip_nulls, m_generator->min_periods());
            return agg("tdigest", skip_nulls, options);
        }

        OutputType variance(int ddof = 1, bool skip_nulls = true) const {
            arrow::compute::VarianceOptions options(ddof, skip_nulls, m_generator->min_periods());
            return agg("variance", skip_nulls, options);
        }

      private:
        window::WindowBoundGeneratorPtr m_generator;
        OutputType const&                m_data;

        [[nodiscard]] FrameType<is_dataframe> agg(std::string const& agg_name, bool skip_null, const arrow::compute::FunctionOptions& options) const;

        [[nodiscard]] std::unordered_map<std::string, FrameType<is_dataframe>>
          agg(std::vector<std::string> const& agg_names, bool skip_null, const arrow::compute::FunctionOptions& options) const;
    };

    class ApplyDataFrameRollingWindowOperations
    {
      public:
        ApplyDataFrameRollingWindowOperations(window::WindowBoundGeneratorPtr generator,
                                     DataFrame const&                data);

        Series    apply(std::function<Scalar(DataFrame const&)> const& fn) const;
        Series    apply(std::function<Series(DataFrame const&)> const& fn) const;
        DataFrame apply(std::function<Series(Series const&)> const& fn) const;
        DataFrame apply(std::function<DataFrame(DataFrame const&)> const& fn) const;

      private:
        window::WindowBoundGeneratorPtr m_generator;
        DataFrame const&                m_data;
    };

    class ApplySeriesRollingWindowOperations
    {
      public:
        ApplySeriesRollingWindowOperations(window::WindowBoundGeneratorPtr generator,
                                     Series const&                data);

        Series apply(std::function<Scalar(Series const&)> const& fn) const;


        Series cov(Series const &other, int64_t min_periods=1, int64_t ddof=1) const;

        Series corr(Series const &other, int64_t min_periods=1, int64_t ddof=1) const;
      private:
        window::WindowBoundGeneratorPtr m_generator;
        Series const&                m_data;
    };

    extern template class AggRollingWindowOperations<true>;
    extern template class AggRollingWindowOperations<false>;

    struct EWMWindowOptions
    {
        std::optional<double>                com{std::nullopt};
        std::optional<double>                span{std::nullopt};
        std::optional<double>                alpha{std::nullopt};
        int64_t                              min_periods{0};
        bool                                 adjust{true};
        bool                                 ignore_na{false};
    };

    template<bool is_dataframe>
    class EWMWindowOperations
    {
      public:
        using OutputType = FrameType<is_dataframe>;
        EWMWindowOperations(const EWMWindowOptions& options, OutputType const& data);

        OutputType mean() const;
        OutputType sum() const;
        OutputType var(bool bias = false) const;
        OutputType std(bool bias = false) const;
        OutputType cov(OutputType const& other, bool bias = false) const;
        OutputType corr(OutputType const& other) const;

      private:
        EWMWindowOptions                    m_options;
        OutputType const&                    m_data;
        int64_t                             m_min_periods{0};
        double                              m_com{1.0};
        std::shared_ptr<arrow::DoubleArray> m_deltas{nullptr};

        double get_center_of_mass(std::optional<double> com, std::optional<double> const& span,
                                  std::optional<TimeDelta> const& halflife,
                                  std::optional<double> const&    alpha) const;

        OutputType apply_column_wise(std::function<arrow::ArrayPtr(arrow::DoubleArray const&)> const& fn) const;

        [[nodiscard]] OutputType agg_ewm(bool normalize) const;
    };

    extern template class EWMWindowOperations<true>;
    extern template class EWMWindowOperations<false>;
} // namespace epoch_frame
