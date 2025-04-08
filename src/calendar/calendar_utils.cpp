//
// Created by adesola on 3/22/25.
//

#include "calendar_utils.h"
#include "epoch_core/macros.h"
#include "epoch_core/ranges_to.h"
#include "epoch_frame/common.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/series.h"
#include <vector>

namespace epoch_frame::calendar::utils
{
    std::optional<DateTime> is_single_observance(const HolidayData& holiday)
    {
        return (!holiday.start_date || !holiday.end_date ||
                *holiday.start_date != *holiday.end_date)
                   ? std::nullopt
                   : holiday.start_date;
    }

    std::optional<std::vector<DateTime>>
    all_single_observance_rules(const AbstractHolidayCalendar& cal)
    {
        auto observances = cal.getRules() | std::views::transform(is_single_observance);
        return (std::ranges::all_of(observances, [](auto const& x) { return x.has_value(); }))
                   ? std::optional(observances |
                                   std::views::transform([](auto const& v) { return *v; }) |
                                   epoch_core::ranges::to_vector_v)
                   : std::nullopt;
    }

    IndexPtr date_range_htf(const DateRangeHTFOptions& options)
    {
        AssertFromFormat(options.calendar, "Calendar is required");
        return factory::index::date_range(
            {.start = DateTime(options.start).timestamp(),
             .end   = options.end.transform([&](Date const& d) { return DateTime(d).timestamp(); }),
             .periods = options.periods,
             .offset  = options.calendar});
    }

    DataFrame merge_schedules(const std::vector<DataFrame>& schedules, bool outer)
    {
        AssertFalseFromFormat(schedules.empty(), "No schedules to merge");

        std::vector<std::string>        all_cols;
        std::unordered_set<std::string> all_cols_set;
        all_cols.reserve(2);
        for (auto const& schedule : schedules)
        {
            auto cols = schedule.column_names();
            std::copy_if(cols.begin(), cols.end(), std::back_inserter(all_cols),
                         [&](auto const& col)
                         {
                             if (col != "BreakStart" && col != "BreakEnd")
                             {
                                 return all_cols_set.insert(col).second;
                             }
                             SPDLOG_WARN("Ignoring BreakStart and BreakEnd columns");
                             return false;
                         });
        }

        auto market_open  = schedules[0]["MarketOpen"];
        auto market_close = schedules[0]["MarketClose"];

        for (auto const& schedule : schedules | std::views::drop(1))
        {
            auto market_open_df =
                concat({{market_open, schedule["MarketOpen"]}, JoinType::Outer, AxisType::Column});
            auto market_close_df = concat(
                {{market_close, schedule["MarketClose"]}, JoinType::Outer, AxisType::Column});
            if (outer)
            {
                market_open  = market_open_df.min(AxisType::Column, true);
                market_close = market_close_df.max(AxisType::Column, true);
            }
            else
            {
                market_open  = market_open_df.max(AxisType::Column, true);
                market_close = market_close_df.min(AxisType::Column, true);
            }
        }
        return concat({{market_open.rename("MarketOpen"), market_close.rename("MarketClose")},
                       JoinType::Outer,
                       AxisType::Column});
    }
} // namespace epoch_frame::calendar::utils