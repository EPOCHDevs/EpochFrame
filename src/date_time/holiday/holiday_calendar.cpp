#include "holiday_calendar.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/common.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include <iostream>
#include <memory>

namespace epoch_frame::calendar
{

    AbstractHolidayCalendar::AbstractHolidayCalendar(const AbstractHolidayCalendarData& data,
                                                     DateTime start_date, DateTime end_date)
        : name(data.name), rules(data.rules), start_date(std::move(start_date)),
          end_date(std::move(end_date))
    {
        AssertFromFormat(data.rules.size() > 0, "Rules must contain at least one holiday");
    }

    // Find a rule by name
    std::optional<HolidayData> AbstractHolidayCalendar::ruleFromName(const std::string& name) const
    {
        auto it = std::find_if(rules.begin(), rules.end(),
                               [&name](const HolidayData& rule) { return rule.name == name; });
        return (it != rules.end()) ? std::optional<HolidayData>(*it) : std::nullopt;
    }

    // Get holidays between dates
    DataFrame AbstractHolidayCalendar::holidays_with_names(const std::optional<DateTime>& start,
                                                           const std::optional<DateTime>& end) const
    {
        if (rules.empty())
        {
            throw std::runtime_error("Holiday Calendar " + name +
                                     " does not have any rules specified");
        }

        DateTime start_date_to_use = start.value_or(this->start_date);
        DateTime end_date_to_use   = end.value_or(this->end_date);

        // Check if cache is valid
        if (!cache || start_date_to_use < std::get<0>(*cache) ||
            end_date_to_use > std::get<1>(*cache))
        {
            // Collect holidays from all rules
            std::vector<FrameOrSeries> pre_holidays(rules.size());
            std::ranges::transform(rules, pre_holidays.begin(),
                                   [&](const HolidayData& rule)
                                   {
                                       return FrameOrSeries{Holiday{rule}.dates_with_name(
                                           start_date_to_use.timestamp(),
                                           end_date_to_use.timestamp())};
                                   });

            // Update cache
            cache = std::make_tuple(start_date_to_use, end_date_to_use,
                                    concat({.frames = pre_holidays}).sort_index());
        }
        auto   df = std::get<2>(*cache);
        Scalar start_scalar{start_date_to_use};
        Scalar end_scalar{end_date_to_use};

        return df.loc({start_scalar, end_scalar});
    }

    // Static method to merge holiday calendars
    std::vector<HolidayData>
    AbstractHolidayCalendar::mergeCalendars(const AbstractHolidayCalendar& base,
                                            const AbstractHolidayCalendar& other)
    {
        std::unordered_map<std::string, HolidayData> merged_rules;

        // Add rules from other calendar
        for (const auto& rule : other.getRules())
        {
            merged_rules[rule.name] = rule;
        }

        // Add/override with rules from base calendar
        for (const auto& rule : base.getRules())
        {
            merged_rules[rule.name] = rule;
        }

        // Convert map to vector
        std::vector<HolidayData> result;
        for (const auto& pair : merged_rules)
        {
            result.push_back(pair.second);
        }

        return result;
    }

    // Merge another calendar into this one
    std::vector<HolidayData> AbstractHolidayCalendar::merge(const AbstractHolidayCalendar& other,
                                                            bool                           inplace)
    {
        auto merged_rules = mergeCalendars(*this, other);

        if (inplace)
        {
            setRules(merged_rules);
            return {};
        }

        return merged_rules;
    }

    void HolidayCalendarRegistry::registerCalendar(const AbstractHolidayCalendarData& data)
    {
        calendar_factories[data.name] = [data]()
        { return std::make_shared<AbstractHolidayCalendar>(data); };
    }
} // namespace epoch_frame::calendar
