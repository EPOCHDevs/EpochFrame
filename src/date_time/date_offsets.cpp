//
// Created by adesola on 2/15/25.
//

#include "date_offsets.h"
#include "common/arrow_compute_utils.h"
#include "common/asserts.h"
#include "common/python_utils.h"
#include "epoch_frame/aliases.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/index.h"
#include "epoch_frame/scalar.h"
#include "holiday/holiday_calendar.h"
#include "methods/temporal.h"

using namespace std::literals::chrono_literals;
namespace epoch_frame
{
    OffsetHandler::OffsetHandler(int64_t n) : n_(n) {}

    chrono_day get_days_in_month(chrono_year const& year, chrono_month const& month) noexcept
    {
        return std::chrono::year_month_day_last{year, month / std::chrono::last}.day();
    }

    chrono_day get_day_of_month(chrono_year const& year, chrono_month const& month,
                                DayOption day_opt)
    {
        if (day_opt == DayOption::START)
        {
            return 1d;
        }
        if (day_opt == DayOption::END)
        {
            return get_days_in_month(year, month);
        }
        throw std::invalid_argument(
            "Invalid day option: get_day_of_month only supports START and END");
    }

    int roll_qtrday(chrono_year_month_day const& ymd, int64_t n, int32_t months_since,
                    DayOption day_opt)
    {
        if (n > 0)
        {
            if (months_since < 0 ||
                (months_since == 0 &&
                 ymd.day() < get_day_of_month(ymd.year(), ymd.month(), day_opt)))
            {
                n -= 1;
            }
        }
        else
        {
            if (months_since > 0 ||
                (months_since == 0 &&
                 ymd.day() > get_day_of_month(ymd.year(), ymd.month(), day_opt)))
            {
                n += 1;
            }
        }
        return n;
    }

    int roll_qtrday(chrono_year_month_day const& ymd, int64_t n, chrono_month const& month,
                    DayOption day_opt, uint32_t modby)
    {
        int32_t month_since{0};
        if (modby == 12)
        {
            month_since = static_cast<int32_t>(static_cast<uint32_t>(ymd.month()) -
                                               static_cast<uint32_t>(month));
        }
        else
        {
            month_since = static_cast<int32_t>((static_cast<uint32_t>(ymd.month()) % modby) -
                                               (static_cast<uint32_t>(month) % modby));
        }
        return roll_qtrday(ymd, n, month_since, day_opt);
    }

    int roll_convention(uint32_t other, int64_t n, uint32_t compare) noexcept
    {
        if (n > 0 && other < compare)
        {
            n -= 1;
        }
        else if (n <= 0 && other > compare)
        {
            n += 1;
        }
        return n;
    }

    chrono_year_month_day shift_month(chrono_year_month_day const&    ymd,
                                      std::chrono::months const&      months,
                                      std::optional<DayOption> const& day_opt)
    {
        // Match pandas: dy = (m0 + months)//12; month = (m0 + months)%12; if month==0: month=12;
        // dy-=1
        int base_month         = static_cast<int>(static_cast<unsigned>(ymd.month())); // 1..12
        int total              = base_month + months.count();
        auto [dy_int, rem_int] = floor_div_rem<int>(total, 12);
        if (rem_int == 0)
        {
            rem_int = 12;
            dy_int -= 1;
        }

        chrono_year  year  = ymd.year() + std::chrono::years(dy_int);
        chrono_month month = chrono_month(static_cast<unsigned>(rem_int));

        chrono_day day;
        if (!day_opt)
        {
            auto days_in_month = get_days_in_month(year, chrono_month(month));
            day                = std::min(ymd.day(), days_in_month);
        }
        else if (*day_opt == DayOption::START)
        {
            day = 1d;
        }
        else if (*day_opt == DayOption::END)
        {
            day = get_days_in_month(year, chrono_month(month));
        }
        else
        {
            throw std::invalid_argument(
                "Invalid day option: shift_month only supports START and END");
        }
        return year / chrono_month(month) / day;
    }

    Array OffsetHandler::add_array(Array const& other) const
    {
        return other.map([&](const Scalar& val) { return Scalar{this->add(val.timestamp())}; },
                         true);
    }

    arrow::TimestampScalar OffsetHandler::rollforward(const arrow::TimestampScalar& dt) const
    {
        return is_on_offset(dt) ? dt : base()->add(dt);
    }

    arrow::TimestampScalar OffsetHandler::rollback(const arrow::TimestampScalar& dt) const
    {
        return is_on_offset(dt) ? dt : base()->rsub(dt);
    }

    std::shared_ptr<IDateOffsetHandler> OffsetHandler::mul(int64_t const& other) const
    {
        return make(other * n());
    }

    int64_t relative_diff(const arrow::TimestampScalar& dt, const arrow::TimestampScalar& end,
                          const IDateOffsetHandler& offset)
    {
        int64_t count      = 0;
        auto    scalar_dt  = Scalar(dt);
        auto    scalar_end = Scalar(end);
        while (scalar_dt < scalar_end)
        {
            auto next = Scalar(offset.add(scalar_dt.timestamp()));
            AssertFromStream(next > scalar_dt,
                             "offset " << offset.name() << " did not increment date");
            if (next > scalar_end)
            {
                break;
            }
            ++count;
            scalar_dt = next;
        }
        return count;
    }

    RelativeDeltaOffsetHandler::RelativeDeltaOffsetHandler(int64_t n, RelativeDelta const& offset)
        : OffsetHandler(n), m_offset(offset)
    {
    }

    int64_t RelativeDeltaOffsetHandler::diff(const arrow::TimestampScalar& start,
                                             const arrow::TimestampScalar& end) const
    {
        return relative_diff(start, end, *this);
    }

    arrow::TimestampScalar
    RelativeDeltaOffsetHandler::add(const arrow::TimestampScalar& other) const
    {
        auto other_scalar = Scalar(other);
        auto tzinfo       = other_scalar.dt().tz();
        auto dt           = other_scalar.to_datetime();

        // Special handling for timezone-aware datetimes when using RelativeDelta
        if (!tzinfo.empty())
        {
            // Remove timezone for calculation (like pandas' "perform calculation in UTC")
            dt = dt.replace_tz("");
        }

        // Apply the relative delta offset
        dt = dt + (m_offset * static_cast<double>(n()));

        // Restore timezone if needed (like pandas' "bring tz back from UTC calculation")
        if (!tzinfo.empty())
        {
            dt = dt.tz_localize(tzinfo);
        }

        return dt.timestamp();
    }

    WeekHandler::WeekHandler(int64_t n, std::optional<epoch_core::EpochDayOfWeek> weekday)
        : BaseCalendarOffsetHandler(n), m_weekday(weekday)
    {
    }

    int64_t WeekHandler::diff(const arrow::TimestampScalar& start,
                              const arrow::TimestampScalar& end) const
    {
        return relative_diff(start, end, *this);
    }

    arrow::TimestampScalar WeekHandler::add(const arrow::TimestampScalar& other) const
    {
        if (!m_weekday)
        {
            // Unanchored: follow pandas Week._apply => other + n * timedelta(weeks=1)
            return other + (n() * TimeDelta{{.weeks = 1}});
        }

        auto k            = n();
        auto other_day    = Scalar(other).weekday();
        auto other_result = other;
        if (other_day != *m_weekday)
        {
            auto mod = pymod(static_cast<int64_t>(*m_weekday) - static_cast<int64_t>(other_day), 7);
            other_result = other_result + TimeDelta{{.days = static_cast<double>(mod)}};
            if (k > 0)
            {
                k -= 1;
            }
        }
        return other_result + TimeDelta{{.weeks = static_cast<double>(k)}};
    }

    bool WeekHandler::is_on_offset(const arrow::TimestampScalar& other) const
    {
        if (!m_weekday)
        {
            return true;
        }
        return Scalar(other).weekday() == *m_weekday;
    }

    int64_t MonthOffsetHandler::diff(const arrow::TimestampScalar& start,
                                     const arrow::TimestampScalar& end) const
    {
        const auto diff = AssertCastScalarResultIsOk<arrow::MonthIntervalScalar>(
            arrow::compute::MonthsBetween(start, end));
        return diff.value;
    }

    arrow::TimestampScalar MonthOffsetHandler::add(const arrow::TimestampScalar& other) const
    {
        auto ymd = arrow_utils::get_year_month_day(other);
        auto compare_day =
            static_cast<uint32_t>(get_day_of_month(ymd.year(), ymd.month(), this->m_day_opt));
        auto n = roll_convention(static_cast<uint32_t>(ymd.day()), this->n(), compare_day);
        return factory::scalar::from_ymd(shift_month(ymd, chrono_months(n), this->m_day_opt),
                                         arrow_utils::get_tz(other));
    }

    bool MonthOffsetHandler::is_on_offset(const arrow::TimestampScalar& other) const
    {
        auto ymd = arrow_utils::get_year_month_day(other);
        return arrow_utils::get_day(other) ==
               get_day_of_month(ymd.year(), ymd.month(), this->m_day_opt);
    }

    QuarterOffsetHandler::QuarterOffsetHandler(int64_t                           n,
                                               std::optional<std::chrono::month> starting_month,
                                               DayOption                         day_opt)
        : BaseCalendarOffsetHandler(n), m_starting_month(starting_month.value_or(March)),
          m_day_opt(day_opt)
    {
    }

    int64_t QuarterOffsetHandler::diff(const arrow::TimestampScalar& start,
                                       const arrow::TimestampScalar& end) const
    {
        const auto diff = AssertCastScalarResultIsOk<arrow::Int64Scalar>(
            arrow::compute::QuartersBetween(start, end));
        return diff.value;
    }

    arrow::TimestampScalar QuarterOffsetHandler::add(const arrow::TimestampScalar& other) const
    {
        auto ymd         = arrow_utils::get_year_month_day(other);
        auto month_since = static_cast<int32_t>(static_cast<uint32_t>(ymd.month()) % 3) -
                           static_cast<int32_t>(static_cast<uint32_t>(m_starting_month) % 3);
        auto qtrs   = roll_qtrday(ymd, this->n(), this->m_starting_month, this->m_day_opt, 3);
        auto months = qtrs * 3 - month_since;
        return factory::scalar::from_ymd(shift_month(ymd, chrono_months(months), this->m_day_opt),
                                         arrow_utils::get_tz(other));
    }

    bool QuarterOffsetHandler::is_on_offset(const arrow::TimestampScalar& other) const
    {
        auto ymd       = arrow_utils::get_year_month_day(other);
        auto mod_month = (ymd.month() - this->m_starting_month) % 3;
        return (mod_month.count() == 0) &&
               ymd.day() == get_day_of_month(ymd.year(), ymd.month(), this->m_day_opt);
    }

    int64_t YearOffsetHandler::diff(const arrow::TimestampScalar& start,
                                    const arrow::TimestampScalar& end) const
    {
        const auto diff = AssertCastScalarResultIsOk<arrow::Int64Scalar>(
            arrow::compute::YearsBetween(start, end));
        return diff.value;
    }

    arrow::TimestampScalar YearOffsetHandler::add(const arrow::TimestampScalar& other) const
    {
        auto ymd    = arrow_utils::get_year_month_day(other);
        auto years  = roll_qtrday(ymd, this->n(), this->m_month, this->m_day_opt, 12);
        auto months = years * 12 + static_cast<int32_t>(static_cast<uint32_t>(this->m_month)) -
                      static_cast<int32_t>(static_cast<uint32_t>(ymd.month()));
        return factory::scalar::from_ymd(shift_month(ymd, chrono_months(months), this->m_day_opt),
                                         arrow_utils::get_tz(other));
    }

    bool YearOffsetHandler::is_on_offset(const arrow::TimestampScalar& other) const
    {
        auto ymd = arrow_utils::get_year_month_day(other);
        return ymd.month() == this->m_month &&
               ymd.day() == get_day_of_month(ymd.year(), ymd.month(), this->m_day_opt);
    }

    int64_t EasterHandler::diff(const arrow::TimestampScalar& start,
                                const arrow::TimestampScalar& end) const
    {
        return relative_diff(start, end, *this);
    }

    arrow::TimestampScalar EasterHandler::add(const arrow::TimestampScalar& other) const
    {
        auto       dt             = factory::scalar::to_datetime(other);
        const auto current_easter = easter(static_cast<int>(dt.date().year));
        auto       _n             = n();
        if (_n >= 0 && dt.date() < current_easter)
        {
            --_n;
        }
        else if (_n < 0 && dt.date() > current_easter)
        {
            ++_n;
        }

        const auto _new = easter(static_cast<int>(dt.date().year) + _n);
        return dt.set_date(_new).timestamp();
    }

    bool EasterHandler::is_on_offset(const arrow::TimestampScalar& other) const
    {
        auto date = factory::scalar::to_datetime(other).date();
        return date == easter(static_cast<int>(date.year));
    }
    BusinessMixin::BusinessMixin(np::BusinessDayCalendarPtr calendar, int64_t n,
                                 std::optional<TimeDelta> timedelta)
        : OffsetHandler(n), m_calendar(std::move(calendar)), m_offset(std::move(timedelta))
    {
        AssertFromFormat(m_calendar, "calendar is not a valid business day calendar");
    }

    BusinessMixin::BusinessMixin(BusinessMixinParams params, int64_t n,
                                 std::optional<TimeDelta> timedelta)
        : OffsetHandler(n), m_offset(std::move(timedelta))
    {
        if (params.calendar)
        {
            std::visit(
                [&]<typename T>(T x)
                {
                    if (x == nullptr)
                    {
                        return;
                    }

                    if constexpr (std::is_same_v<T, np::BusinessDayCalendarPtr>)
                    {
                        m_calendar = std::move(x);
                    }
                    else if constexpr (std::is_same_v<T, calendar::AbstractHolidayCalendarPtr>)
                    {
                        auto                                   index = x->holidays();
                        std::shared_ptr<arrow::TimestampArray> _holidays =
                            index->array().to_timestamp_view();
                        std::ranges::transform(
                            *_holidays, std::back_inserter(params.holidays),
                            [dt = index->dtype()](const std::optional<int64_t>& holiday)
                            {
                                AssertFromFormat(holiday, "holiday is not a valid timestamp");
                                return factory::scalar::to_datetime(
                                    arrow::TimestampScalar{*holiday, dt});
                            });
                        m_calendar = std::make_shared<np::BusinessDayCalendar>(params.weekmask,
                                                                               params.holidays);
                    }
                },
                *params.calendar);
        }
        else
        {
            m_calendar =
                std::make_shared<np::BusinessDayCalendar>(params.weekmask, params.holidays);
        }
    }

    BusinessDay::BusinessDay(int64_t n, std::optional<TimeDelta> timedelta)
        : OffsetHandler(n), m_offset(std::move(timedelta))
    {
    }

    int64_t BusinessDay::diff(const arrow::TimestampScalar& start,
                              const arrow::TimestampScalar& end) const
    {
        return relative_diff(start, end, *this);
    }

    int64_t BusinessDay::adjust_ndays(int8_t wday, int64_t weeks) const
    {
        auto    _n = n();
        int64_t days{};
        if (_n <= 0 && wday > 4)
        {
            _n += 1;
        }
        _n -= (5 * weeks);

        if (_n == 0 && wday > 4)
        {
            days = 4 - wday;
        }
        else if (wday > 4)
        {
            days = (7 - wday) + (_n - 1);
        }
        else if (wday + _n <= 4)
        {
            days = _n;
        }
        else
        {
            days = _n + 2;
        }
        return days;
    }

    arrow::TimestampScalar BusinessDay::add(const arrow::TimestampScalar& other) const
    {
        const auto _n     = n();
        const auto _other = factory::scalar::to_datetime(other);
        auto       wday   = _other.weekday();
        auto       weeks  = static_cast<int64_t>(floor_div(_n, 5));
        auto       days   = adjust_ndays(wday, weeks);
        auto       result = _other + TimeDelta{{.days = static_cast<double>(7 * weeks + days)}};
        if (m_offset)
        {
            result = result + *m_offset;
        }
        return result.timestamp();
    }

    bool BusinessDay::is_on_offset(const arrow::TimestampScalar& other) const
    {
        return factory::scalar::to_datetime(other).weekday() < 5;
    }
    CustomBusinessDay::CustomBusinessDay(np::BusinessDayCalendarPtr calendar, int64_t n,
                                         std::optional<TimeDelta> timedelta)
        : BusinessMixin(std::move(calendar), n, std::move(timedelta))
    {
    }

    CustomBusinessDay::CustomBusinessDay(BusinessMixinParams params, int64_t n,
                                         std::optional<TimeDelta> timedelta)
        : BusinessMixin(std::move(params), n, std::move(timedelta))
    {
    }

    int64_t CustomBusinessDay::diff(const arrow::TimestampScalar& start,
                                    const arrow::TimestampScalar& end) const
    {
        return relative_diff(start, end, *this);
    }

    arrow::TimestampScalar CustomBusinessDay::add(const arrow::TimestampScalar& other) const
    {
        if (m_calendar)
        {
            const auto roll =
                n() <= 0 ? np::BusDayOffsetRoll::Following : np::BusDayOffsetRoll::Preceding;
            auto other_dt = factory::scalar::to_datetime(other);
            auto date     = DateTime(other_dt.date());
            auto incr_dt  = m_calendar->offset(date, n(), roll);
            auto result   = DateTime(incr_dt.date(), other_dt.time());
            if (m_offset)
            {
                result = result + *m_offset;
            }
            return result.timestamp();
        }
        return other + (n() * TimeDelta{{.days = 1}});
    }

    bool CustomBusinessDay::is_on_offset(const arrow::TimestampScalar& other) const
    {
        if (m_calendar)
        {
            return m_calendar->is_busday(factory::scalar::to_datetime(other));
        }
        return true;
    }

    // -------------------- WeekOfMonth / LastWeekOfMonth --------------------
    int WeekOfMonthOffsetHandler::get_offset_day_from_ymd(chrono_year_month_day const& ymd) const
    {
        // Compute day of the month for m_week-th weekday
        auto mstart   = chrono_year_month_day{ymd.year(), ymd.month(), 1d};
        auto weekday0 = Scalar{factory::scalar::from_ymd(mstart, "")}.weekday();
        int  shift    = (static_cast<int>(m_weekday) - static_cast<int>(weekday0) + 7) % 7;
        if (m_week == -1)
        {
            // Last week of month: find month end and go backwards to weekday
            auto dim         = get_days_in_month(ymd.year(), ymd.month());
            auto mend        = chrono_year_month_day{ymd.year(), ymd.month(), dim};
            auto end_weekday = Scalar{factory::scalar::from_ymd(mend, "")}.weekday();
            int  back = (static_cast<int>(end_weekday) - static_cast<int>(m_weekday) + 7) % 7;
            return static_cast<int>(static_cast<unsigned>(dim)) - back;
        }
        return 1 + shift + m_week * 7;
    }

    int WeekOfMonthOffsetHandler::get_offset_day(const arrow::TimestampScalar& other) const
    {
        auto ymd = arrow_utils::get_year_month_day(other);
        return get_offset_day_from_ymd(ymd);
    }

    WeekOfMonthOffsetHandler::WeekOfMonthOffsetHandler(int64_t n, int week,
                                                       epoch_core::EpochDayOfWeek weekday)
        : BaseCalendarOffsetHandler(n), m_week(week), m_weekday(weekday)
    {
        AssertFromFormat(week == -1 || (week >= 0 && week <= 3), "Week must be -1 or 0..3");
    }

    int64_t WeekOfMonthOffsetHandler::diff(const arrow::TimestampScalar& start,
                                           const arrow::TimestampScalar& end) const
    {
        return relative_diff(start, end, *this);
    }

    arrow::TimestampScalar WeekOfMonthOffsetHandler::add(const arrow::TimestampScalar& other) const
    {
        // Following pandas WeekOfMonthMixin behavior
        auto ymd         = arrow_utils::get_year_month_day(other);
        int  compare_day = get_offset_day_from_ymd(ymd);

        auto months = n();
        months      = roll_convention(static_cast<uint32_t>(ymd.day()), months, compare_day);

        auto shifted = shift_month(ymd, chrono_months(months), DayOption::START);
        int  to_day  = get_offset_day_from_ymd(shifted);
        auto target  = chrono_year_month_day{shifted.year(), shifted.month(),
                                            chrono_day{static_cast<unsigned>(to_day)}};
        return factory::scalar::from_ymd(target, arrow_utils::get_tz(other));
    }

    bool WeekOfMonthOffsetHandler::is_on_offset(const arrow::TimestampScalar& other) const
    {
        auto ymd = arrow_utils::get_year_month_day(other);
        return static_cast<uint32_t>(ymd.day()) ==
               static_cast<uint32_t>(get_offset_day_from_ymd(ymd));
    }

    // -------------------- BusinessMonthOffsetHandler --------------------
    static bool is_business_day(DateTime const& dt)
    {
        auto wd = static_cast<epoch_core::EpochDayOfWeek>(dt.weekday());
        return wd >= epoch_core::EpochDayOfWeek::Monday && wd <= epoch_core::EpochDayOfWeek::Friday;
    }

    static DateTime month_business_edge(DateTime const& dt, bool begin)
    {
        if (begin)
        {
            auto first = DateTime{Date{dt.date().year, dt.date().month, 1d}};
            auto cur   = first;
            while (!is_business_day(cur))
            {
                cur = cur + TimeDelta{{.days = 1}};
            }
            return cur;
        }
        else
        {
            auto dim  = get_days_in_month(dt.date().year, dt.date().month);
            auto last = DateTime{Date{dt.date().year, dt.date().month, dim}};
            auto cur  = last;
            while (!is_business_day(cur))
            {
                cur = cur - TimeDelta{{.days = 1}};
            }
            return cur;
        }
    }

    int64_t BusinessMonthOffsetHandler::diff(const arrow::TimestampScalar& start,
                                             const arrow::TimestampScalar& end) const
    {
        return relative_diff(start, end, *this);
    }

    arrow::TimestampScalar
    BusinessMonthOffsetHandler::add(const arrow::TimestampScalar& other) const
    {
        auto dt       = factory::scalar::to_datetime(other);
        bool is_begin = (m_edge == BusinessEdge::Begin);
        auto current  = month_business_edge(dt, is_begin);

        // n == 0 → rollforward to edge on or after dt
        if (n() == 0)
        {
            if (dt.date() <= current.date())
            {
                return current.timestamp();
            }
            auto ymd_next  = shift_month(arrow_utils::get_year_month_day(other), chrono_months(1),
                                         DayOption::START);
            auto next_base = DateTime{Date{ymd_next.year(), ymd_next.month(), 1d}};
            auto next_edge = month_business_edge(next_base, is_begin);
            return next_edge.timestamp();
        }

        // General case: strictly next/previous edges
        auto months = n();
        if (is_begin)
        {
            // Begin: positive steps always move by n months; negative steps include current-month
            // edge when dt is after it
            if (months < 0 && dt.date() > current.date())
            {
                months += 1;
            }
        }
        else
        {
            // End: positive steps include current-month edge when dt is before it;
            // negative steps include current-month edge when dt is after it
            if (months > 0 && dt.date() < current.date())
            {
                months -= 1;
            }
            else if (months < 0 && dt.date() > current.date())
            {
                months += 1;
            }
        }

        auto dest_ymd = shift_month(arrow_utils::get_year_month_day(other), chrono_months(months),
                                    DayOption::START);
        auto dest_dt  = DateTime{Date{dest_ymd.year(), dest_ymd.month(), 1d}};
        auto edge_dt  = month_business_edge(dest_dt, is_begin);
        return edge_dt.timestamp();
    }

    bool BusinessMonthOffsetHandler::is_on_offset(const arrow::TimestampScalar& other) const
    {
        auto dt   = factory::scalar::to_datetime(other);
        auto edge = month_business_edge(dt, m_edge == BusinessEdge::Begin);
        return dt.date() == edge.date();
    }

    // -------------------- SessionAnchorOffsetHandler --------------------
    SessionAnchorOffsetHandler::SessionAnchorOffsetHandler(SessionRange       session,
                                                           SessionAnchorWhich which,
                                                           TimeDelta delta, int64_t n)
        : OffsetHandler(n), m_session(std::move(session)), m_which(which), m_delta(std::move(delta))
    {
        AssertFromFormat(m_session.start.tz == m_session.end.tz,
                         "SessionRange.start.tz must equal SessionRange.end.tz");
    }

    arrow::TimestampScalar SessionAnchorOffsetHandler::anchor_for_date(const Date&        date,
                                                                       const std::string& tz) const
    {
        auto base_time =
            (m_which == SessionAnchorWhich::AfterOpen) ? m_session.start : m_session.end;
        // ensure we use provided tz (must match session tz by contract)
        auto time_in_tz = base_time.replace_tz(tz);
        // Build as naive local wall time, then localize to apply tz rules (DST-aware)
        auto naive_time = time_in_tz.replace_tz("");
        auto dt         = DateTime{date, naive_time}.tz_localize(tz);

        if (m_which == SessionAnchorWhich::AfterOpen)
        {
            dt = dt + m_delta;
        }
        else
        {
            dt = dt - m_delta;
        }

        return dt.timestamp();
    }

    int64_t SessionAnchorOffsetHandler::diff(const arrow::TimestampScalar& /*start*/,
                                             const arrow::TimestampScalar& /*end*/) const
    {
        throw std::runtime_error(
            "SessionAnchorOffsetHandler::diff is not supported for SessionAnchor offsets.");
    }

    arrow::TimestampScalar
    SessionAnchorOffsetHandler::rollback(const arrow::TimestampScalar& /*dt*/) const // NOLINT
    {
        throw std::runtime_error("SessionAnchorOffsetHandler::rollback is not supported for "
                                 "SessionAnchor offsets. Use add()/base() semantics instead.");
    }

    arrow::TimestampScalar
    SessionAnchorOffsetHandler::rollforward(const arrow::TimestampScalar& /*dt*/) const
    {
        throw std::runtime_error("SessionAnchorOffsetHandler::rollforward is not supported for "
                                 "SessionAnchor offsets. Use add()/base() semantics instead.");
    }

    bool SessionAnchorOffsetHandler::is_on_offset(const arrow::TimestampScalar& other) const
    {
        auto s  = Scalar{other};
        auto dt = s.to_datetime();
        // Convert to session timezone if needed
        auto session_tz = m_session.start.tz;
        if (dt.tz().empty())
        {
            dt = dt.tz_localize("UTC").tz_convert(session_tz);
        }
        else if (dt.tz() != session_tz)
        {
            dt = dt.tz_convert(session_tz);
        }
        auto anchor = anchor_for_date(dt.date(), dt.tz());
        // Convert input to session tz for comparison
        Scalar s_in_session_tz = s;
        if (s.dt().tz().empty())
        {
            s_in_session_tz = s.dt().tz_localize("UTC");
        }
        if (session_tz != s_in_session_tz.dt().tz())
        {
            s_in_session_tz = s_in_session_tz.dt().tz_convert(session_tz);
        }
        return Scalar{anchor} == s_in_session_tz;
    }

    arrow::TimestampScalar
    SessionAnchorOffsetHandler::add(const arrow::TimestampScalar& /*other*/) const
    {
        throw std::runtime_error(
            "SessionAnchorOffsetHandler::add is not supported for SessionAnchor offsets.");
    }

} // namespace epoch_frame
