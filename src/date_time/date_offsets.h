//
// Created by adesola on 2/15/25.
//

#pragma once
#include "day_of_week.h"
#include "epoch_frame/aliases.h"
#include "epoch_frame/enums.h"
#include "relative_delta.h"
#include <arrow/compute/api.h>
#include <arrow/scalar.h>
#include <fmt/format.h>
#include <memory>
#include "business/np_busdaycal.h"


CREATE_ENUM(EpochOffsetType, RelativeDelta, Day, Hour, Minute, Second, Milli, Micro, Nano, Week,
            Month, MonthStart, MonthEnd, Quarter, QuarterStart, QuarterEnd, Year, YearStart,
            YearEnd, BusinessDay, CustomBusinessDay);
namespace epoch_frame
{
    struct IDateOffsetHandler
    {
        virtual ~IDateOffsetHandler() = default;

        virtual int64_t n() const = 0;

        virtual int64_t diff(arrow::TimestampScalar const&,
                             arrow::TimestampScalar const&) const = 0;

        virtual bool is_fixed() const = 0;
        virtual bool is_end() const   = 0;
        virtual epoch_core::EpochOffsetType type() const = 0;


        virtual arrow::TimestampScalar              add(arrow::TimestampScalar const&) const  = 0;
        virtual std::shared_ptr<IDateOffsetHandler> mul(int64_t const&) const                 = 0;
        virtual arrow::TimestampScalar              rsub(arrow::TimestampScalar const&) const = 0;
        virtual std::shared_ptr<IDateOffsetHandler> rmul(int64_t const&) const                = 0;
        virtual std::shared_ptr<IDateOffsetHandler> negate() const                            = 0;
        virtual class Array                         add_array(Array const&) const             = 0;

        virtual bool is_on_offset(arrow::TimestampScalar const&) const    = 0;
        virtual std::shared_ptr<IDateOffsetHandler> base() const          = 0;
        virtual std::shared_ptr<IDateOffsetHandler> make(int64_t n) const = 0;

        virtual arrow::TimestampScalar rollforward(arrow::TimestampScalar const&) const = 0;
        virtual arrow::TimestampScalar rollback(arrow::TimestampScalar const&) const    = 0;
        virtual std::string            code() const                                     = 0;

        virtual std::string name() const = 0;
    };

    class OffsetHandler : public IDateOffsetHandler
    {
      public:
        OffsetHandler(int64_t n);

        [[nodiscard]] int64_t n() const override
        {
            return n_;
        }

        std::string name() const override
        {
            return std::format("{}{}", this->n(), this->code());
        }

        arrow::TimestampScalar rollforward(arrow::TimestampScalar const&) const;

        arrow::TimestampScalar rollback(arrow::TimestampScalar const&) const;

        // TODO: Allow subclasses to override this for faster vectorized operations
        Array add_array(Array const&) const final;

        std::shared_ptr<IDateOffsetHandler> mul(int64_t const&) const final;

        std::shared_ptr<IDateOffsetHandler> rmul(int64_t const& other) const final
        {
            return this->mul(other);
        }

        arrow::TimestampScalar rsub(arrow::TimestampScalar const& other) const final
        {
            return this->negate()->add(other);
        }

        std::shared_ptr<IDateOffsetHandler> negate() const final
        {
            return this->mul(-1);
        }

        std::shared_ptr<IDateOffsetHandler> base() const final
        {
            return this->make(1);
        }

      private:
        int64_t n_;
    };

    struct FixedOffsetHandler : OffsetHandler
    {
        FixedOffsetHandler(int64_t n) : OffsetHandler(n) {}

        bool is_fixed() const override
        {
            return true;
        }

        virtual arrow::compute::CalendarUnit calendar_unit() const = 0;
    };

    class RelativeDeltaOffsetHandler : public OffsetHandler
    {
      public:
        RelativeDeltaOffsetHandler(int64_t n, RelativeDelta const& offset);

        int64_t diff(const arrow::TimestampScalar& start,
                     const arrow::TimestampScalar& end) const override;

        arrow::TimestampScalar add(const arrow::TimestampScalar& other) const override;

        bool is_on_offset(const arrow::TimestampScalar& other) const override
        {
            return true;
        }

        bool is_fixed() const override
        {
            return false;
        }

        bool is_end() const override
        {
            return false;
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::RelativeDelta;
        }

        std::string code() const override
        {
            return "DateOffset(" + m_offset.repr() + ")";
        }

        std::string name() const override
        {
            return "DateOffset(" + m_offset.repr() + ")";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<RelativeDeltaOffsetHandler>(n, m_offset);
        }

      private:
        RelativeDelta m_offset;

        TimeDelta pd_timedelta() const;
    };

    struct TickHandler : FixedOffsetHandler
    {
        explicit TickHandler(int64_t n) : FixedOffsetHandler(n) {}

        virtual int64_t nano_increments() const = 0;

        int64_t nanos() const {
            return nano_increments() * n();
        }

        int64_t diff(const arrow::TimestampScalar& start,
                     const arrow::TimestampScalar& end) const override
        {
            return std::ceil((end.value - start.value) * 1.0 / (nano_increments() * n()) * 1.0);
        }

        arrow::TimestampScalar add(const arrow::TimestampScalar& other) const override
        {
            return arrow::TimestampScalar(other.value + this->n() * this->nano_increments(),
                                          other.type);
        }

        bool is_on_offset(const arrow::TimestampScalar& other) const override
        {
            return true;
        }

        bool is_end() const override
        {
            return false;
        }

        epoch_core::EpochOffsetType type() const override = 0;
    };

    constexpr size_t ONE_BILLION = 1'000'000'000;
    constexpr size_t ONE_MILLION = 1'000'000;

    struct DayHandler : TickHandler
    {
        explicit DayHandler(int64_t n) : TickHandler(n) {}

        std::string code() const override
        {
            return "D";
        }

        int64_t nano_increments() const override
        {
            return 24UL * 3600 * ONE_BILLION;
        }

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::DAY;
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<DayHandler>(n);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Day;
        }
    };

    struct HourHandler : TickHandler
    {
        HourHandler(int64_t n, std::optional<std::string> timezone = {}) : TickHandler(n) {}

        std::string code() const override
        {
            return "H";
        }

        int64_t nano_increments() const override
        {
            return 3600 * ONE_BILLION;
        }

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::HOUR;
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<HourHandler>(n);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Hour;
        }
    };

    struct MinuteHandler : TickHandler
    {
        MinuteHandler(int64_t n) : TickHandler(n) {}

        std::string code() const override
        {
            return "Min";
        }

        int64_t nano_increments() const override
        {
            return 60 * ONE_BILLION;
        }

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::MINUTE;
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<MinuteHandler>(n);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Minute;
        }
    };

    struct SecondHandler : TickHandler
    {
        SecondHandler(int64_t n) : TickHandler(n) {}

        std::string code() const override
        {
            return "S";
        }

        int64_t nano_increments() const override
        {
            return ONE_BILLION;
        }

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::SECOND;
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<SecondHandler>(n);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Second;
        }
    };

    struct MilliHandler : TickHandler
    {
        MilliHandler(int64_t n) : TickHandler(n) {}

        std::string code() const override
        {
            return "ms";
        }

        int64_t nano_increments() const override
        {
            return ONE_MILLION;
        }

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::MILLISECOND;
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<MilliHandler>(n);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Milli;
        }
    };

    struct MicroHandler : TickHandler
    {
        MicroHandler(int64_t n) : TickHandler(n) {}

        std::string code() const override
        {
            return "us";
        }

        int64_t nano_increments() const override
        {
            return 1000;
        }

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::MICROSECOND;
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<MicroHandler>(n);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Micro;
        }
    };

    struct NanoHandler : TickHandler
    {
        NanoHandler(int64_t n) : TickHandler(n) {}

        std::string code() const override
        {
            return "ns";
        }

        int64_t nano_increments() const override
        {
            return 1;
        }

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::NANOSECOND;
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<NanoHandler>(n);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Nano;
        }
    };

    enum class DayOption
    {
        START,
        END,
        BUSINESS_START,
        BUSINESS_END
    };

    chrono_day get_days_in_month(chrono_year const& year, chrono_month const& month) noexcept;
    int        roll_qtrday(chrono_year_month_day const& ymd, int64_t n, chrono_month const& month,
                           DayOption day_opt, uint32_t modby);
    chrono_day get_day_of_month(chrono_year const& year, chrono_month const& month,
                                DayOption day_opt);
    int        roll_convention(uint32_t other, int64_t n, uint32_t compare) noexcept;
    chrono_year_month_day shift_month(chrono_year_month_day const& ymd, const chrono_months& months,
                                      std::optional<DayOption> const& day_opt);

    // ---------------------------------------------------------------------
    // Week-Based Offset Classes
    class WeekHandler : public FixedOffsetHandler
    {
      public:
        WeekHandler(int64_t n, std::optional<epoch_core::EpochDayOfWeek> weekday = {});

        int64_t diff(const arrow::TimestampScalar& start,
                     const arrow::TimestampScalar& end) const override;

        arrow::TimestampScalar add(const arrow::TimestampScalar& other) const override;

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::WEEK;
        }

        bool is_on_offset(const arrow::TimestampScalar& other) const override;

        std::string code() const override
        {
            return std::format(
                "W{}",
                m_weekday ? std::format("-{}", epoch_core::EpochDayOfWeekWrapper::ToString(*m_weekday)) : "");
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<WeekHandler>(n, m_weekday);
        }

        bool is_end() const override
        {
            return false;
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Week;
        }

      private:
        std::optional<epoch_core::EpochDayOfWeek> m_weekday;
    };

    // TODO: WeekOfMonthOffsetHandler
    // TODO: LastWeekOfMonthOffsetHandler
    // -------------------------------------------------------------------------------
    class MonthOffsetHandler : public FixedOffsetHandler
    {
      public:
        MonthOffsetHandler(int64_t n, DayOption day_opt = DayOption::END)
            : FixedOffsetHandler(n), m_day_opt(day_opt)
        {
        }

        int64_t diff(const arrow::TimestampScalar& start,
                     const arrow::TimestampScalar& end) const override;

        arrow::TimestampScalar add(const arrow::TimestampScalar& other) const override;

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::MONTH;
        }

        bool is_on_offset(const arrow::TimestampScalar& other) const override;

        std::string code() const override
        {
            return "M";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<MonthOffsetHandler>(n, m_day_opt);
        }

        bool is_end() const override
        {
            return false;
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Month;
        }

      protected:
        DayOption m_day_opt;
    };

    class MonthStartHandler : public MonthOffsetHandler
    {
      public:
        MonthStartHandler(int64_t n) : MonthOffsetHandler(n, DayOption::START) {}

        std::string code() const override
        {
            return "MS";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<MonthStartHandler>(n);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::MonthStart;
        }
    };

    class MonthEndHandler : public MonthOffsetHandler
    {
      public:
        MonthEndHandler(int64_t n) : MonthOffsetHandler(n, DayOption::END) {}

        std::string code() const override
        {
            return "ME";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<MonthEndHandler>(n);
        }

        bool is_end() const override
        {
            return true;
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::MonthEnd;
        }
    };

    class QuarterOffsetHandler : public FixedOffsetHandler
    {
      public:
        QuarterOffsetHandler(int64_t n, std::optional<std::chrono::month> starting_month = {},
                             DayOption day_opt = DayOption::END);

        int64_t diff(const arrow::TimestampScalar& start,
                     const arrow::TimestampScalar& end) const override;

        arrow::TimestampScalar add(const arrow::TimestampScalar& other) const override;

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::QUARTER;
        }

        bool is_on_offset(const arrow::TimestampScalar& other) const override;

        std::string code() const override
        {
            return "Q";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<QuarterOffsetHandler>(n, m_starting_month, m_day_opt);
        }

        bool is_end() const override
        {
            return false;
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Quarter;
        }

      protected:
        std::chrono::month m_starting_month;
        DayOption          m_day_opt;
    };

    class QuarterStartHandler : public QuarterOffsetHandler
    {
      public:
        QuarterStartHandler(int64_t n, std::optional<std::chrono::month> starting_month = {},
                            DayOption day_opt = DayOption::START)
            : QuarterOffsetHandler(n, starting_month, day_opt)
        {
        }

        std::string code() const override
        {
            return "QS";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<QuarterStartHandler>(n, m_starting_month, m_day_opt);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::QuarterStart;
        }
    };

    class QuarterEndHandler : public QuarterOffsetHandler
    {
      public:
        QuarterEndHandler(int64_t n, std::optional<std::chrono::month> starting_month = {},
                          DayOption day_opt = DayOption::END)
            : QuarterOffsetHandler(n, starting_month, day_opt)
        {
        }

        std::string code() const override
        {
            return "QE";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<QuarterEndHandler>(n, m_starting_month, m_day_opt);
        }

        bool is_end() const override
        {
            return true;
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::QuarterEnd;
        }
    };

    class YearOffsetHandler : public FixedOffsetHandler
    {
      public:
        YearOffsetHandler(int64_t n, std::optional<std::chrono::month> month = {},
                          DayOption day_opt = DayOption::END)
            : FixedOffsetHandler(n), m_month(month.value_or(std::chrono::December)),
              m_day_opt(day_opt)
        {
        }

        int64_t diff(const arrow::TimestampScalar& start,
                     const arrow::TimestampScalar& end) const override;

        arrow::TimestampScalar add(const arrow::TimestampScalar& other) const override;

        arrow::compute::CalendarUnit calendar_unit() const override
        {
            return arrow::compute::CalendarUnit::YEAR;
        }

        bool is_on_offset(const arrow::TimestampScalar& other) const override;

        std::string code() const override
        {
            return "Y";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<YearOffsetHandler>(n, m_month, m_day_opt);
        }

        bool is_end() const override
        {
            return false;
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::Year;
        }

      protected:
        std::chrono::month m_month;
        DayOption          m_day_opt;
    };

    class YearStartHandler : public YearOffsetHandler
    {
      public:
        YearStartHandler(int64_t n, std::optional<std::chrono::month> month = {})
            : YearOffsetHandler(n, month, DayOption::START)
        {
        }

        std::string code() const override
        {
            return "YS";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<YearStartHandler>(n, m_month);
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::YearStart;
        }
    };

    class YearEndHandler : public YearOffsetHandler
    {
      public:
        YearEndHandler(int64_t n, std::optional<std::chrono::month> month = {})
            : YearOffsetHandler(n, month, DayOption::END)
        {
        }

        std::string code() const override
        {
            return "YE";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<YearEndHandler>(n, m_month);
        }

        bool is_end() const override
        {
            return true;
        }

        epoch_core::EpochOffsetType type() const override
        {
            return epoch_core::EpochOffsetType::YearEnd;
        }
    };

    class EasterHandler : public OffsetHandler
    {
      public:
        EasterHandler(int64_t n) : OffsetHandler(n) {}

        int64_t diff(const arrow::TimestampScalar& start,
                     const arrow::TimestampScalar& end) const override;

        arrow::TimestampScalar add(const arrow::TimestampScalar& other) const override;

        bool is_on_offset(const arrow::TimestampScalar& other) const override;

        bool is_fixed() const override
        {
            return false;
        }

        std::string code() const override
        {
            return "Easter";
        }

        std::string name() const override
        {
            return "Easter";
        }

        std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override
        {
            return std::make_shared<EasterHandler>(n);
        }

        bool is_end() const override
        {
            return false;
        }

        epoch_core::EpochOffsetType type() const override
        {
            // Easter isn't in the enum, we could add it or use something else
            // For now let's use RelativeDelta as it's also not fixed
            return epoch_core::EpochOffsetType::RelativeDelta;
        }
    };

    struct BusinessMixinParams {
        np::WeekMask weekmask{true, true, true, true, true, false, false};
        std::vector<DateTime> holidays{};
        std::optional<std::variant<np::BusinessDayCalendarPtr, calendar::AbstractHolidayCalendarPtr>> calendar{std::nullopt};
    };

    class BusinessMixin : public OffsetHandler {
        public:
            BusinessMixin(np::BusinessDayCalendarPtr  calendar, int64_t n,  std::optional<TimeDelta> timedelta=std::nullopt);
            BusinessMixin(BusinessMixinParams , int64_t n,  std::optional<TimeDelta> timedelta=std::nullopt);
        protected:
            np::BusinessDayCalendarPtr m_calendar{nullptr};
            std::optional<TimeDelta> m_offset;
    };

    class BusinessDay : public OffsetHandler {
        public:
            BusinessDay(int64_t n, std::optional<TimeDelta> timedelta=std::nullopt);

            int64_t diff(const arrow::TimestampScalar &start, const arrow::TimestampScalar &end) const override;

            arrow::TimestampScalar add(const arrow::TimestampScalar &other) const override;

            bool is_on_offset(const arrow::TimestampScalar &other) const override;

            bool is_fixed() const override {
                return false;
            }

            std::string code() const override { return "B"; }

            std::string name() const override { return "BusinessDay"; }

            std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override {
                return std::make_shared<BusinessDay>(n, m_offset);
            }

            epoch_core::EpochOffsetType type() const override {
                return epoch_core::EpochOffsetType::BusinessDay;
            }

            bool is_end() const override {
                return false;
            }

            private:
                std::optional<TimeDelta> m_offset;

                int64_t adjust_ndays(int8_t wday, int64_t weeks) const;
    };

    class CustomBusinessDay : public BusinessMixin {
        public:
            CustomBusinessDay(BusinessMixinParams, int64_t n=1, std::optional<TimeDelta> timedelta=std::nullopt);
            CustomBusinessDay(np::BusinessDayCalendarPtr  calendar, int64_t n,  std::optional<TimeDelta> timedelta=std::nullopt);

            int64_t diff(const arrow::TimestampScalar &start, const arrow::TimestampScalar &end) const override;

            arrow::TimestampScalar add(const arrow::TimestampScalar &other) const override;

            bool is_on_offset(const arrow::TimestampScalar &other) const override;

            bool is_fixed() const override {
                return false;
            }

            std::string code() const override { return "C"; }

            std::string name() const override { return "CustomBusinessDay"; }

            std::shared_ptr<IDateOffsetHandler> make(int64_t n) const override {
                return std::make_shared<CustomBusinessDay>(m_calendar, n, m_offset);
            }

            epoch_core::EpochOffsetType type() const override {
                return epoch_core::EpochOffsetType::CustomBusinessDay;
            }

            bool is_end() const override {
                return false;
            }

            auto holidays() const {
                return m_calendar->holidays();
            }
    };

    using DateOffsetHandlerPtr  = std::shared_ptr<IDateOffsetHandler>;
    using DateOffsetHandlerPtrs = std::vector<DateOffsetHandlerPtr>;
} // namespace epoch_frame
