//
// Created by adesola on 2/9/25.
//

#include "business_hour.h"
#include "custom_business_day.h"

namespace epochframe::datetime {
    template<typename T>
    std::vector<size_t> argsort(const std::vector<T> &array) {
        std::vector<size_t> indices(array.size());
        std::iota(indices.begin(), indices.end(), 0);

        std::sort(indices.begin(), indices.end(),
                  [&](int left, int right) -> bool {
                      return array[left] < array[right];
                  });

        return indices;
    }

    BusinessHourHandler::BusinessHourHandler(BusinessHourHandlerOption option) : BusinessMixinHandler(std::move(option.baseOption)) {
        auto& start = option.start;
        auto& end = option.end;

        AssertWithTraceFromStream(start.size() > 0, "Must include at least 1 start time");
        AssertWithTraceFromStream(start.size() == end.size(),
                                  "number of starting time and ending time must be the same");

        auto num_openings = start.size();

        auto index = argsort(start);
        std::ranges::for_each(std::views::iota(0UL, num_openings), [&](size_t i) {
            start[i] = start[index[i]];
            end[i] = end[index[i]];
        });

        int64_t total_secs = 0;
        for (auto i = 0; i < num_openings; i++) {
            total_secs += _get_business_hours_by_sec(start[i], end[i]);
            total_secs += _get_business_hours_by_sec(end[i], start[(i + 1) % num_openings]);
        }

        AssertFalseFromStream(total_secs != 24 * 60 * 60, "invalid starting and ending time(s): "
                                                          "opening hours should not touch or overlap with one another");
        m_start = std::move(start);
        m_end = std::move(end);
    }

    int64_t BusinessHourHandler::_get_business_hours_by_sec(BusinessTime const &start, BusinessTime const &end) const {
        auto dtstart = ptime(date(2014, 4, 1), start.to_time_duration());
        auto day = start < end ? 1 : 2;
        auto until = ptime(date(2014, 4, day), end.to_time_duration());
        return (until - dtstart).total_seconds();
    }


    Timestamp BusinessHourHandler::_get_closing_time(Timestamp const& dt) const {
        for (auto const& [i, st]: ranges::views::enumerate(m_start))
        {
            if ((st.hour() == hours(dt.hour())) && st.minute() == minutes(dt.minute())) {
                return dt + TimeDelta(seconds(_get_business_hours_by_sec(st, m_end[i])));
            }
        }
        throw std::runtime_error("invalid closing time");
    }


    std::unique_ptr<BusinessDayHandler> BusinessHourHandler::next_bday() const {
        const auto nb_offset = (n() >= 0) ? 1 : -1;

        if (prefix().starts_with("c")) {
            return std::make_unique<CustomBusinessDayHandler>(BusinessMixinOption{.n=nb_offset});
        }
        return std::make_unique<BusinessDayHandler>(BusinessMixinOption{{.n=nb_offset}});
    }


}
