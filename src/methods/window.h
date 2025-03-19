#pragma once
#include "calendar/time_delta.h"
#include "epochframe/aliases.h"
#include <arrow/api.h>
#include <epoch_lab_shared/enum_wrapper.h>
#include <optional>
#include <vector>
CREATE_ENUM(EpochFrameEWMWindowType, Alpha, HalfLife, Span, CenterOfMass);
CREATE_ENUM(EpochFrameRollingWindowClosedType, Left, Right, Both, Neither);

namespace epochframe
{

    namespace window
    {
        struct WindowBound
        {
            IndexType start;
            IndexType end;
        };
        using WindowBounds = std::vector<WindowBound>;
        struct IWindowBoundGenerator
        {
            virtual ~IWindowBoundGenerator()                                  = default;
            virtual WindowBounds get_window_bounds(uint64_t num_values) const = 0;
        };
        using WindowBoundGeneratorPtr = std::unique_ptr<IWindowBoundGenerator>;

        struct RollingWindowOptions
        {
            int64_t                           window_size;
            std::optional<int64_t>            min_periods{};
            bool                              center{false};
            EpochFrameRollingWindowClosedType closed{EpochFrameRollingWindowClosedType::Null};
            int64_t                           step{1};
        };

        class RollingWindow : public IWindowBoundGenerator
        {
          public:
            RollingWindow(const RollingWindowOptions& options);
            WindowBounds get_window_bounds(uint64_t num_values) const override;

          private:
            int64_t                           m_window_size;
            int64_t                           m_min_periods{};
            bool                              m_center{false};
            EpochFrameRollingWindowClosedType m_closed{EpochFrameRollingWindowClosedType::Null};
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
            WindowBounds get_window_bounds(uint64_t num_values) const override;

          private:
            ExpandingWindowOptions m_options;
        };
    } // namespace window

    class AggRollingWindowOperations
    {
      public:
        AggRollingWindowOperations(window::WindowBoundGeneratorPtr generator,
                                   DataFrame const&                data);

        [[nodiscard]] DataFrame agg(std::string const& agg_name, bool skip_null = true) const;

        [[nodiscard]] std::unordered_map<std::string, DataFrame>
        agg(std::vector<std::string> const& agg_names, bool skip_null = true) const;

      private:
        window::WindowBoundGeneratorPtr m_generator;
        DataFrame const&                m_data;
    };

    class ApplyRollingWindowOperations
    {
      public:
        ApplyRollingWindowOperations(window::WindowBoundGeneratorPtr generator,
                                     DataFrame const&                data);

        Series    apply(std::function<Scalar(DataFrame const&)> const& fn) const;
        Series    apply(std::function<Series(DataFrame const&)> const& fn) const;
        DataFrame apply(std::function<Series(Series const&)> const& fn) const;
        DataFrame apply(std::function<DataFrame(DataFrame const&)> const& fn) const;

      private:
        window::WindowBoundGeneratorPtr m_generator;
        DataFrame const&                m_data;
    };

    struct EWMWindowOptions
    {
        std::shared_ptr<class DateTimeIndex> times{nullptr};
        std::optional<double>                com{std::nullopt};
        std::optional<double>                span{std::nullopt};
        std::optional<TimeDelta>             halflife{std::nullopt};
        std::optional<double>                alpha{std::nullopt};
        int64_t                              min_periods{0};
        bool                                 adjust{true};
        bool                                 ignore_na{false};
    };

    class EWMWindowOperations
    {
      public:
        EWMWindowOperations(const EWMWindowOptions& options, DataFrame const& data);

        DataFrame mean() const;
        DataFrame sum() const;
        DataFrame var(bool bias = false) const;
        DataFrame std(bool bias = false) const;
        DataFrame corr() const;
        DataFrame cov() const;

      private:
        EWMWindowOptions                    m_options;
        DataFrame const&                    m_data;
        std::shared_ptr<arrow::DoubleArray> m_deltas{nullptr};
        double                              m_com{1.0};

        void   calculate_deltas();
        double get_center_of_mass(std::optional<double> com, std::optional<double> const& span,
                                  std::optional<TimeDelta> const& halflife,
                                  std::optional<double> const&    alpha) const;

        DataFrame apply(std::function<Scalar(arrow::DoubleArray const&)> const& fn) const;

        [[nodiscard]] DataFrame agg_ewm(std::string const& agg_name, bool skip_null = true) const;

        [[nodiscard]] DataFrame agg_ewm_cov(std::string const& agg_name,
                                            bool               bias = false) const;
    };

} // namespace epochframe