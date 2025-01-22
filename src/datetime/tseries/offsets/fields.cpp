//
// Created by adesola on 1/21/25.
//

#include "fields.h"
#include "common_utils/exceptions.h"
#include "calendar.h"
#include <unordered_set>
#include "calendar.h"


namespace epochframe::datetime {
    /**
 * Helper to check if a given month satisfies the modulo condition for quarters/years.
 */
    inline bool is_on_month(int month, int compare_month, int modby) {
        if (modby == 1) {
            return true; // Every month satisfies this for modby = 1.
        }
        if (modby == 3) {
            return (month - compare_month) % 3 == 0;
        }
        return month == compare_month;
    }

    std::vector<bool> get_start_end_field(
            std::vector<ptime> const &dtIndex,
            std::string field,
            std::string const &freqName,
            int month_kw) {
        const std::unordered_set<std::string> startFreq{"is_month_start", "is_quarter_start", "is_year_start"};
        const std::unordered_set<std::string> endFreq{"is_month_end", "is_quarter_end", "is_year_end"};

        std::size_t count = dtIndex.size();
        bool is_business = false;
        int end_month = 12;
        int start_month = 1;

        std::vector<bool> out;
        out.reserve(count);
        int modby = 0;

        // Handle frequency-specific logic.
        if (!freqName.empty()) {
            if (freqName == "C") {
                throw std::invalid_argument(
                        fmt::format("Custom business days are not supported by {}", field));
            }

            is_business = freqName.starts_with('B');

            if (freqName.find("QS") != std::string::npos || freqName.find("YS") != std::string::npos) {
                end_month = (month_kw == 1) ? 12 : month_kw - 1;
                start_month = month_kw;
            } else {
                end_month = month_kw;
                start_month = (end_month % 12) + 1;
            }
        }

        const auto compare_month = (field.find("start") != std::string::npos) ? start_month : end_month;

        if (field.find("month") != std::string::npos) {
            modby = 1;
        } else if (field.find("quarter") != std::string::npos) {
            modby = 3;
        } else {
            modby = 12;
        }

        if (startFreq.count(field)) {
            std::ranges::transform(dtIndex, std::back_inserter(out), [&](ptime const &dt) {
                const auto ymd = dt.date().year_month_day();
                return (!dt.is_not_a_date_time()) && (is_on_month(ymd.month, compare_month, modby) &&
                                                      (ymd.day ==
                                                       (is_business ? get_firstbday(ymd.year, ymd.month) : 1)));
            });
        } else if (endFreq.count(field)) {
            auto compare_fn = is_business ? get_lastbday : get_days_in_month;
            std::ranges::transform(dtIndex, std::back_inserter(out), [&](ptime const &dt) {
                const auto ymd = dt.date().year_month_day();
                return (!dt.is_not_a_date_time()) &&
                       (is_on_month(ymd.month, compare_month, modby) && compare_fn(ymd.year, ymd.month) == ymd.day);
            });
        } else {
            throw ValueError(fmt::format("Field {} not supported.", field));
        }
        return out;
    }

    std::vector<std::string> get_date_name_field(std::vector<ptime> const &dtIndex,
                                                 bool is_day_field) {
        std::vector<std::string> out;
        out.reserve(dtIndex.size());

        if (is_day_field) {
            std::ranges::transform(dtIndex, std::back_inserter(out), [&](ptime const &dt) {
                auto ymd = dt.date().year_month_day();
                return DAYS_FULL[day_of_week(ymd.year, ymd.month, ymd.day)];
            });
        } else {
            std::ranges::transform(dtIndex, std::back_inserter(out), [&](ptime const &dt) {
                auto month = dt.date().month();
                return MONTHS_FULL[month];
            });
        }
        return out;
    }
}
