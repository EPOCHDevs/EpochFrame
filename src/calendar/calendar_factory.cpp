//
// Created by adesola on 4/7/25.
//
#include "epoch_frame/factory/calendar_factory.h"
#include "calendars/all.h"

namespace epoch_frame::calendar
{
    void CalendarFactory::Init() {
        REGISTER_CALENDAR(NYSEExchangeCalendar);
        REGISTER_CALENDAR(CMEEquityExchangeCalendar);
        REGISTER_CALENDAR(CMEAgricultureExchangeCalendar);
        REGISTER_CALENDAR(CMEBondExchangeCalendar);
        REGISTER_CALENDAR(CFEExchangeCalendar);
        REGISTER_CALENDAR(CBOEEquityOptionsExchangeCalendar);
        REGISTER_CALENDAR(CBOEIndexOptionsExchangeCalendar);
        REGISTER_CALENDAR(CMEGlobexFXExchangeCalendar);
        REGISTER_CALENDAR(CMEGlobexCryptoExchangeCalendar);
        REGISTER_CALENDAR(CMEGlobexEquitiesExchangeCalendar);
        REGISTER_CALENDAR(CMEGlobexLivestockExchangeCalendar);
        REGISTER_CALENDAR(CMEGlobexGrainsAndOilseedsExchangeCalendar);
        REGISTER_CALENDAR(CMEGlobexFixedIncomeCalendar);
        REGISTER_CALENDAR(CMEGlobexEnergyAndMetalsExchangeCalendar);
        REGISTER_CALENDAR(ICEExchangeCalendar);
        REGISTER_CALENDAR(FXExchangeCalendar);
        REGISTER_CALENDAR(CryptoExchangeCalendar);
    }

    MarketCalendarPtr CalendarFactory::get_calendar(const std::string& name) const
    {
        auto it = m_default_calendars.find(name);
        if (it == m_default_calendars.end())
        {
            throw std::invalid_argument("Calendar not found");
        }
        return it->second;
    }

    MarketCalendarPtr
    CalendarFactory::create_calendar(const std::string&               name,
                                     std::optional<MarketTime> const& open_time,
                                     std::optional<MarketTime> const& close_time) const
    {
        auto it = m_calendars_functions.find(name);
        if (it == m_calendars_functions.end())
        {
            throw std::invalid_argument("Calendar not found");
        }
        return it->second(open_time, close_time);
    }

    void CalendarFactory::add_calendar(FactoryFunction factory)
    {
        auto default_cal = factory(std::nullopt, std::nullopt);
        for (auto const& name : default_cal->aliases())
        {
            m_calendars_functions[name] = factory;
            m_default_calendars[name]   = default_cal;
        }
    }
} // namespace epoch_frame::calendar
