//
// Created by adesola on 2/9/25.
//

#include "business_day.h"


namespace epochframe::datetime {
    std::string BusinessDayHandler::offset_str() const {
        if (!m_offset) {
            return "";
        }

        return fmt::format("+{}", m_offset->to_string());
    }

    int BusinessDayHandler::_adjust_ndays(int wday, int weeks) const {
        auto n_ = n();
        if (n_ <= 0 and wday > 4) {
            n_ += 1;
        }
        n_ -= weeks * 5;

        int days{};
        if (n_ == 0 and wday > 4) {
            days = 4 - wday;
        } else if (wday > 4) {
            days = (7 - wday) + (n_ - 1);
        } else if ((wday + n_) <= 4) {
            days = n_;
        } else {
            days = n_ + 2;
        }
        return days;
    }

    Timestamp BusinessDayHandler::apply(const epochframe::datetime::Timestamp &other) const {
        auto n_ = n();
        auto wday = other.weekday();

        auto weeks = n_ / 5;
        auto adjust_days = _adjust_ndays(wday, weeks);
        auto result = other + days(7 * weeks + adjust_days);
        if (m_offset) {
            return result + *m_offset;
        }
        return result;
    }
}
