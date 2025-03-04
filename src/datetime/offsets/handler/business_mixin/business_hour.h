//
// Created by adesola on 2/9/25.
//

#pragma once
#include "business_day.h"


namespace epochframe::datetime {
    class BusinessTime {
    public:
        explicit BusinessTime(int _hour, int _minute = 0) {
            AssertWithTraceFromStream(_hour >= 0 && _hour < 24, "hour must be in [0, 23]");
            AssertWithTraceFromStream(_minute >= 0 && _minute < 60, "minute must be in [0, 59]");
            m_hour = hours(_hour);
            m_minute = minutes(_minute);
        }

        hours hour() const { return m_hour; }

        minutes minute() const { return m_minute; }

        time_duration to_time_duration() const {
            return m_hour + m_minute;
        }

        std::strong_ordering operator<=>(const BusinessTime &other) const {
            auto lhs = to_time_duration();
            auto rhs = other.to_time_duration();
            if (lhs == rhs) {
                return std::strong_ordering::equal;
            }
            return lhs < rhs ? std::strong_ordering::less : std::strong_ordering::greater;
        }

    private:
        hours m_hour{0};
        minutes m_minute{0};
    };

    struct BusinessHourHandlerOption {
        BusinessMixinOption baseOption;
        std::vector<BusinessTime> start{
                BusinessTime{9},
        };
        std::vector<BusinessTime> end{
                BusinessTime{17},
        };
    };

    class BusinessHourHandler : public BusinessMixinHandler {
    public:
        explicit BusinessHourHandler(BusinessMixinOption option, BusinessTime start = BusinessTime{9},
                                     BusinessTime end = BusinessTime{17}) : BusinessHourHandler(
                {std::move(option), std::vector{std::move(start)}, std::vector{std::move(end)}}) {}

        explicit BusinessHourHandler(BusinessHourHandlerOption option);

        std::string offset_str() const override;

        Timestamp apply(const epochframe::datetime::Timestamp &value) const override;

        bool is_on_offset(const epochframe::datetime::Timestamp &value) const override {
            if (should_normalize() && !is_normalized(value)) {
                return false;
            }
            return value.weekday() < 5;
        }

        std::unique_ptr<BusinessDayHandler> next_bday() const;

    private:
        std::vector<BusinessTime> m_start, m_end;

        int _adjust_ndays(int wday, int weeks) const;

        int64_t _get_business_hours_by_sec(BusinessTime const &start, BusinessTime const &end) const;

        Timestamp _get_closing_time(Timestamp const &dt) const;

    };
}
