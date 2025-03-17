#pragma once

#include "../holidays/holiday.h"
#include <string>
#include <unordered_map>
#include <functional>
#include <memory>
#include <vector>
#include <algorithm>
#include <chrono>
#include <stdexcept>
#include "epochframe/dataframe.h"
#include <optional>
#include <tuple>
#include "calendar/datetime.h"


namespace epochframe::calendar{
    // Forward declaration
    class AbstractHolidayCalendar;

    using CalendarRules = std::vector<HolidayData>;
    struct AbstractHolidayCalendarData {
        CalendarRules rules;
        std::string name;
    };

    /**
     * Global registry for holiday calendars
     */
    class HolidayCalendarRegistry {
    private:
        // Singleton instance
        static HolidayCalendarRegistry* instance;

        // Map of calendar name to factory function
        std::unordered_map<std::string, std::function<std::shared_ptr<AbstractHolidayCalendar>()>> calendar_factories;

        // Private constructor for singleton
        HolidayCalendarRegistry() = default;

    public:
        // Delete copy/move constructors and assignments
        HolidayCalendarRegistry(const HolidayCalendarRegistry&) = delete;
        HolidayCalendarRegistry& operator=(const HolidayCalendarRegistry&) = delete;
        HolidayCalendarRegistry(HolidayCalendarRegistry&&) = delete;
        HolidayCalendarRegistry& operator=(HolidayCalendarRegistry&&) = delete;

        // Get singleton instance
        static HolidayCalendarRegistry& getInstance() {
            static HolidayCalendarRegistry instance;
            return instance;
        }

        // Register a calendar type with factory function
        void registerCalendar(const AbstractHolidayCalendarData& data);

        // Get a calendar instance by name
        std::shared_ptr<AbstractHolidayCalendar> getCalendar(const std::string& name) {
            auto it = calendar_factories.find(name);
            if (it != calendar_factories.end()) {
                return it->second();
            }
            throw std::runtime_error("Calendar not found: " + name);
        }

        // Check if a calendar exists
        bool hasCalendar(const std::string& name) const {
            return calendar_factories.find(name) != calendar_factories.end();
        }

        // Get list of all registered calendar names
        std::vector<std::string> getRegisteredCalendarNames() const {
            std::vector<std::string> names;
            names.reserve(calendar_factories.size());
            for (const auto& pair : calendar_factories) {
                names.push_back(pair.first);
            }
            return names;
        }
    };

    // Initialize static member
    inline HolidayCalendarRegistry* HolidayCalendarRegistry::instance = nullptr;

    /**
     * Abstract holiday calendar class that serves as the base for all calendar implementations
     */
    class AbstractHolidayCalendar {
    protected:
        std::string name;
        std::vector<HolidayData> rules;
        DateTime start_date = DateTime({1970y, std::chrono::January, 1d});
        DateTime end_date = DateTime({2200y, std::chrono::December, 31d});

        // Cache for holidays
        mutable std::optional<std::tuple<DateTime, DateTime, DataFrame>> cache;
    public:
        // Constructor
        AbstractHolidayCalendar(const AbstractHolidayCalendarData& data);

        // Virtual destructor
        virtual ~AbstractHolidayCalendar() = default;

        // Get calendar name
        const std::string& getName() const {
            return name;
        }

        // Set calendar name
        void setName(const std::string& name) {
            this->name = name;
        }

        // Get rules
        const std::vector<HolidayData>& getRules() const {
            return rules;
        }

        // Set rules
        void setRules(const std::vector<HolidayData>& rules) {
            this->rules = rules;
            cache = std::nullopt;
        }

        // Find a rule by name
        std::optional<HolidayData> ruleFromName(const std::string& name) const;

        // Get holidays between dates
        IndexPtr holidays(const std::optional<DateTime>& start = std::nullopt,
                        const std::optional<DateTime>& end = std::nullopt) const {
            return holidays_with_names(start, end).index();
        }

        DataFrame holidays_with_names(const std::optional<DateTime>& start = std::nullopt,
                                      const std::optional<DateTime>& end = std::nullopt) const;
        // Static method to merge holiday calendars
        static std::vector<HolidayData> mergeCalendars(const AbstractHolidayCalendar& base,
                                                  const AbstractHolidayCalendar& other);

        // Merge another calendar into this one
        std::vector<HolidayData> merge(const AbstractHolidayCalendar& other, bool inplace = false);
    };

    using AbstractHolidayCalendarPtr = std::shared_ptr<AbstractHolidayCalendar>;

    // Macro to register a holiday calendar class
#define REGISTER_HOLIDAY_CALENDAR(CalendarRules) \
namespace { \
static bool CalendarRules##_registered = []() { \
registerHolidayCalendar(CalendarRules, #CalendarRules); \
return true; \
}(); \
}


    const CalendarRules USFederalHolidayCalendar {
        HolidayData{
            .name = "New Year's Day",
            .month = std::chrono::January,
            .day = 1d,
            .observance=nearest_workday
        },
        USMartinLutherKingJr,
        USPresidentsDay,
        USMemorialDay,
        HolidayData{
            .name = "Juneteenth National Independence Day",
            .month = std::chrono::June,
            .day = 19d,
            .start_date = DateTime{.date={2021y, std::chrono::June, 18d}},
            .observance=nearest_workday
        },
        HolidayData{
            .name = "Independence Day",
            .month = std::chrono::July,
            .day = 4d,
            .observance=nearest_workday
        },
        USLaborDay,
        USColumbusDay,
        HolidayData{
            .name = "Veterans Day",
            .month = std::chrono::November,
            .day = 11d,
            .observance=nearest_workday
        },
        USThanksgivingDay,
        HolidayData{
            .name = "Christmas Day",
            .month = std::chrono::December,
            .day = 25d,
            .observance=nearest_workday
        }
    };

    // Helper function to register a calendar
    inline void registerHolidayCalendar(const std::vector<HolidayData> & rules, std::string const& name="") {
        HolidayCalendarRegistry::getInstance().registerCalendar(AbstractHolidayCalendarData{rules, name});
    }

    // Helper function to get a calendar
    inline std::shared_ptr<AbstractHolidayCalendar> getHolidayCalendar(const std::string& name) {
        return HolidayCalendarRegistry::getInstance().getCalendar(name);
    }

    REGISTER_HOLIDAY_CALENDAR(USFederalHolidayCalendar);
} // namespace epochframe
