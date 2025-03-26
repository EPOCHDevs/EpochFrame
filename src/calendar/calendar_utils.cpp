//
// Created by adesola on 3/22/25.
//

#include "calendar_utils.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_core/ranges_to.h"

namespace epoch_frame::calendar::utils
{
    std::optional<DateTime> is_single_observance(const HolidayData& holiday)
    {
        return (!holiday.start_date || !holiday.end_date || *holiday.start_date != *holiday.end_date) ? std::nullopt : holiday.start_date;
    }

    std::optional<std::vector<DateTime>> all_single_observance_rules(const AbstractHolidayCalendar& cal)
    {
        auto observances = cal.getRules() | std::views::transform(is_single_observance);
        return (std::ranges::all_of(observances, [](auto const&x) {
            return x.has_value();
        })) ? std::optional(observances | std::views::transform([](auto const& v) {
            return *v;
        }) | epoch_core::ranges::to_vector_v) : std::nullopt;
    }

    IndexPtr date_range_htf(const DateRangeHTFOptions& options)
    {
        AssertFromFormat(options.calendar, "Calendar is required");
        return factory::index::date_range({
            .start = DateTime(options.start).timestamp(),
            .end = options.end.transform([&](Date const& d) {
                return DateTime(d).timestamp();
            }),
            .periods = options.periods,
            .offset = options.calendar
        });
    }
} // namespace epoch_frame::calendar::utils
