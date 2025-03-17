//
// Created by adesola on 2/15/25.
//

#include "date_offsets.h"
#include "common/asserts.h"
#include "common/arrow_compute_utils.h"
#include "factory/scalar_factory.h"
#include "epochframe/scalar.h"
#include "methods/temporal.h"
#include "common/python_utils.h"
#include <iostream>
#include "index/index.h"


using namespace std::literals::chrono_literals;
namespace epochframe {
    OffsetHandler::OffsetHandler(int64_t n) : n_(n) {}

    chrono_day get_days_in_month(chrono_year const& year, chrono_month const& month) noexcept {
        return std::chrono::year_month_day_last{year, month / std::chrono::last}.day();
    }

    chrono_day get_day_of_month(chrono_year const& year, chrono_month const& month, DayOption day_opt) {
        if (day_opt == DayOption::START) {
            return 1d;
        }
        if (day_opt == DayOption::END) {
            return get_days_in_month(year, month);
        }
        throw std::invalid_argument("Invalid day option: get_day_of_month only supports START and END");
    }

    int roll_qtrday(chrono_year_month_day const &ymd, int64_t n, int32_t months_since, DayOption day_opt) {
        if (n > 0) {
            if (months_since < 0 || (months_since == 0 && ymd.day() < get_day_of_month(ymd.year(), ymd.month(), day_opt))) {
                n -= 1;
            }
        } else {
            if (months_since > 0 || (months_since == 0 && ymd.day() > get_day_of_month(ymd.year(), ymd.month(), day_opt))) {
                n += 1;
            }
        }
        return n;
    }

    int roll_qtrday(chrono_year_month_day const &ymd, int64_t n, chrono_month const& month, DayOption day_opt, uint32_t modby){
        int32_t month_since{0};
        if (modby == 12) {
            month_since = static_cast<int32_t>(static_cast<uint32_t>(ymd.month()) - static_cast<uint32_t>(month));
        } else {
            month_since = static_cast<int32_t>((static_cast<uint32_t>(ymd.month()) % modby) - (static_cast<uint32_t>(month) % modby));
        }
        return roll_qtrday(ymd, n, month_since, day_opt);
    }

    int roll_convention(uint32_t other, int64_t n, uint32_t compare) noexcept {
        if (n > 0 && other < compare) {
            n -= 1;
        } else if (n <= 0 && other > compare) {
            n += 1;
        }
        return n;
    }

    chrono_year_month_day shift_month(chrono_year_month_day const& ymd, std::chrono::months const& months, std::optional<DayOption> const& day_opt) {
        auto num = static_cast<uint32_t>(ymd.month()) + months.count();
        auto divy = std::div(static_cast<uint32_t>(num), 12);
        std::chrono::years dy(divy.quot);

        auto month = divy.rem;
        chrono_year year = ymd.year()+dy;

        if (month == 0) {
            month = 12;
            year -= std::chrono::years(1);
        }

        chrono_day day;
        if (!day_opt) {
            auto days_in_month = get_days_in_month(year, chrono_month(month));
            day = std::min(ymd.day(), days_in_month);
        } else if (*day_opt == DayOption::START) {
            day = 1d;
        } else if (*day_opt == DayOption::END) {
            day = get_days_in_month(year, chrono_month(month));
        } else {
            throw std::invalid_argument("Invalid day option: shift_month only supports START and END");
        }
        return year/chrono_month(month)/day;
    }

    Array OffsetHandler::add_array(Array const &other) const {
        return other.map([&](const Scalar &val) {
            return Scalar{this->add(val.timestamp())};
        }, true);
    }

    arrow::TimestampScalar OffsetHandler::rollforward(const arrow::TimestampScalar &dt) const {
        return is_on_offset(dt) ? dt : base()->add(dt);
    }

    arrow::TimestampScalar OffsetHandler::rollback(const arrow::TimestampScalar &dt) const {
        return is_on_offset(dt) ? dt : base()->rsub(dt);
    }

    std::shared_ptr<IDateOffsetHandler> OffsetHandler::mul(int64_t const& other) const {
        return make(other * n());
    }

    int64_t relative_diff(const arrow::TimestampScalar &dt, const arrow::TimestampScalar &end, const IDateOffsetHandler &offset) {
        int64_t count = 0;
        auto scalar_dt = Scalar(dt);
        auto scalar_end = Scalar(end);
        while (scalar_dt < scalar_end) {
            auto next = Scalar(offset.add(scalar_dt.timestamp()));
            AssertWithTraceFromStream(next > scalar_dt, "offset " << offset.name() << " did not increment date");
            if (next > scalar_end) {
                break;
            }
            ++count;
            scalar_dt = next;
        }
        return count;
    }

    RelativeDeltaOffsetHandler::RelativeDeltaOffsetHandler(int64_t n, RelativeDelta const &offset) : OffsetHandler(n), m_offset(offset) {}

    int64_t RelativeDeltaOffsetHandler::diff(const arrow::TimestampScalar &start, const arrow::TimestampScalar &end) const {
        return relative_diff(start, end, *this);
    }

    arrow::TimestampScalar RelativeDeltaOffsetHandler::add(const arrow::TimestampScalar &other) const {
        auto other_scalar = Scalar(other);
        auto tzinfo = other_scalar.dt().tz();

        if (!tzinfo.empty()) {
            other_scalar = other_scalar.dt().tz_convert("UTC");
        }
        other_scalar = Scalar{other_scalar.to_datetime() + (m_offset * static_cast<double>(n()))};

        if (!tzinfo.empty()) {
            other_scalar = other_scalar.dt().tz_convert(tzinfo);
        }
        return other_scalar.timestamp();
    }

    WeekHandler::WeekHandler(int64_t n, std::optional<EpochDayOfWeek> weekday) : FixedOffsetHandler(n), m_weekday(weekday) {}

    int64_t WeekHandler::diff(const arrow::TimestampScalar &start, const arrow::TimestampScalar &end) const {
        return relative_diff(start, end, *this);
    }

    arrow::TimestampScalar WeekHandler::add(const arrow::TimestampScalar &other) const {
        if (!m_weekday) {
            return other + (n() * TimeDelta{{.weeks = 1}});
        }

        auto k = n();
        auto other_day = Scalar(other).weekday();
        auto other_result = other;
        if (other_day != *m_weekday) {
            auto mod = pymod(static_cast<int64_t>(*m_weekday) - static_cast<int64_t>(other_day), 7);
            other_result = other_result + TimeDelta{{.days = static_cast<double>(mod)}};
            if (k > 0) {
                k -= 1;
            }
        }
        return other_result + TimeDelta{{.weeks = static_cast<double>(k)}};
    }

    bool WeekHandler::is_on_offset(const arrow::TimestampScalar &other) const {
        if (!m_weekday) {
            return true;
        }
        return Scalar(other).weekday() == *m_weekday;
    }

    int64_t MonthOffsetHandler::diff(const arrow::TimestampScalar &start, const arrow::TimestampScalar &end) const {
        const auto diff = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::MonthsBetween(start, end));
        return diff.value;
    }

    arrow::TimestampScalar MonthOffsetHandler::add(const arrow::TimestampScalar &other) const {
        auto ymd = arrow_utils::get_year_month_day(other);
        auto compare_day = static_cast<uint32_t>(get_day_of_month(ymd.year(), ymd.month(), this->m_day_opt));
        auto n = roll_convention(static_cast<uint32_t>(ymd.day()), this->n(), compare_day);
        return factory::scalar::from_ymd(shift_month(ymd, chrono_months(n), this->m_day_opt), arrow_utils::get_tz(other));
    }

    bool MonthOffsetHandler::is_on_offset(const arrow::TimestampScalar &other) const {
        auto ymd = arrow_utils::get_year_month_day(other);
        return arrow_utils::get_day(other) == get_day_of_month(ymd.year(), ymd.month(), this->m_day_opt);
    }


    QuarterOffsetHandler::QuarterOffsetHandler(int64_t n, std::optional<std::chrono::month> starting_month, DayOption day_opt) : FixedOffsetHandler(n), m_starting_month(starting_month.value_or(std::chrono::March)), m_day_opt(day_opt) {}

    int64_t QuarterOffsetHandler::diff(const arrow::TimestampScalar &start, const arrow::TimestampScalar &end) const {
        const auto diff = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::QuartersBetween(start, end));
        return diff.value;
    }

    arrow::TimestampScalar QuarterOffsetHandler::add(const arrow::TimestampScalar &other) const {
        auto ymd = arrow_utils::get_year_month_day(other);
        auto month_since = static_cast<int32_t>(static_cast<uint32_t>(ymd.month())%3) - static_cast<int32_t>(static_cast<uint32_t>(m_starting_month)%3);
        auto qtrs = roll_qtrday(ymd, this->n(), this->m_starting_month, this->m_day_opt, 3);
        auto months = qtrs*3 - month_since;
        return factory::scalar::from_ymd(shift_month(ymd, chrono_months(months), this->m_day_opt), arrow_utils::get_tz(other));
    }

    bool QuarterOffsetHandler::is_on_offset(const arrow::TimestampScalar &other) const {
        auto ymd = arrow_utils::get_year_month_day(other);
        auto mod_month = (ymd.month() - this->m_starting_month) % 3;
        return (mod_month.count() == 0) && ymd.day() == get_day_of_month(ymd.year(), ymd.month(), this->m_day_opt);
    }

    int64_t YearOffsetHandler::diff(const arrow::TimestampScalar &start, const arrow::TimestampScalar &end) const {
        const auto diff = AssertCastScalarResultIsOk<arrow::Int64Scalar>(arrow::compute::YearsBetween(start, end));
        return diff.value;
    }

    arrow::TimestampScalar YearOffsetHandler::add(const arrow::TimestampScalar &other) const {
        auto ymd = arrow_utils::get_year_month_day(other);
        auto years = roll_qtrday(ymd, this->n(), this->m_month, this->m_day_opt, 12);
        auto months = years * 12 + static_cast<int32_t>(static_cast<uint32_t>(this->m_month)) - static_cast<int32_t>(static_cast<uint32_t>(ymd.month()));
        return factory::scalar::from_ymd(shift_month(ymd, chrono_months(months), this->m_day_opt), arrow_utils::get_tz(other));
    }

    bool YearOffsetHandler::is_on_offset(const arrow::TimestampScalar &other) const {
        auto ymd = arrow_utils::get_year_month_day(other);
        return ymd.month() == this->m_month && ymd.day() == get_day_of_month(ymd.year(), ymd.month(), this->m_day_opt);
    }

    int64_t EasterHandler::diff(const arrow::TimestampScalar &start, const arrow::TimestampScalar &end) const {
        return relative_diff(start, end, *this);
    }

    arrow::TimestampScalar EasterHandler::add(const arrow::TimestampScalar &other) const {
        auto dt = factory::scalar::to_datetime(other);
        const auto current_easter = easter(static_cast<int>(dt.date.year));
        auto _n = n();
        if (_n >= 0 && dt.date < current_easter) {
            --_n;
        }
        else if (_n < 0 && dt.date > current_easter) {
            ++_n;
        }

        auto _new = easter(static_cast<int>(dt.date.year) + _n);
        dt.date = _new;
        return dt.timestamp();
    }

    bool EasterHandler::is_on_offset(const arrow::TimestampScalar &other) const {
        auto date = factory::scalar::to_datetime(other).date;
        return date == easter(static_cast<int>(date.year));
    }
}
