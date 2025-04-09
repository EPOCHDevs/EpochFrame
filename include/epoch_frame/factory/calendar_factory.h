#pragma once
#include "../aliases.h"
#include <unordered_map>

namespace epoch_frame::calendar
{
    using FactoryFunction = std::function<MarketCalendarPtr(
        std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time)>;

    class CalendarFactory
    {
      public:
        static CalendarFactory& instance()
        {
            static CalendarFactory factory;
            return factory;
        }

        MarketCalendarPtr get_calendar(const std::string& name) const;

        MarketCalendarPtr create_calendar(const std::string&               name,
                                          std::optional<MarketTime> const& open_time,
                                          std::optional<MarketTime> const& close_time) const;

        void add_calendar(FactoryFunction factory);

      private:
        CalendarFactory();

        std::unordered_map<std::string, FactoryFunction>   m_calendars_functions{};
        std::unordered_map<std::string, MarketCalendarPtr> m_default_calendars{};
    };

#define REGISTER_CALENDAR(calendar)                                                                \
add_calendar(  \
[](std::optional<MarketTime> const& open_time,                                             \
std::optional<MarketTime> const& close_time)                                            \
{ return std::make_shared<calendar>(open_time, close_time); })

} // namespace epoch_frame::calendar
