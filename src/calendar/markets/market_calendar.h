#pragma once

#include <common/python_utils.h>
#include <string>
#include "../datetime.h"
#include <unordered_map>
#include <optional>
#include <vector>
#include <epoch_lab_shared/enum_wrapper.h>
#include "../calendars/holiday_calendar.h"
#include "../class_registry.h"


// Define enum for OpenCloseType with serialization support
CREATE_ENUM(EpochFrameOpenCloseType,
    Default,   // Use default behavior defined by the class
    True,      // Time opens the market (equivalent to true)
    False      // Time closes the market (equivalent to false) 
);

CREATE_ENUM(EpochFrameMarketTimeType,
    MarketOpen,
    MarketClose,
    BreakStart,
    BreakEnd,
    Pre,
    Post
);


namespace epochframe::calendar {

// MarketTime structure with optional cutoff date
struct MarketTime {
    Time time;
    int64_t day_offset{0};
    Date date;
};
// For representing sequences of market times (for time transitions)
using MarketTimes = std::vector<MarketTime>;
using RegularMarketTimes = ProtectedDict<EpochFrameMarketTimeType, MarketTimes>;
using OpenCloseMap = ProtectedDict<EpochFrameMarketTimeType, EpochFrameOpenCloseType>;
using namespace std::chrono_literals;

struct Interruption {
    Date date;
    MarketTime start_time;
    MarketTime end_time;
};
using Interruptions = std::vector<Interruption>;

// Or continue using std::optional<bool> as a simpler alternative
using OpensType = std::optional<bool>;


struct SpecialTime {
    Time time;
    AbstractHolidayCalendarPtr calendar;
};

using SpecialTimes = std::vector<SpecialTime>;

struct MarketCalendarOptions {
    std::string name;
    RegularMarketTimes regular_market_times;
    OpenCloseMap open_close_map;
    bool has_market_times = true;
    std::string tz;

    AbstractHolidayCalendarPtr regular_holidays;
    np::HolidayList adhoc_holidays;
    std::vector<std::string> aliases;
    np::WeekSet weekmask;
    SpecialTimes special_opens;
    SpecialTimes special_opens_adhoc;
    SpecialTimes special_closes;
    SpecialTimes special_closes_adhoc;
    Interruptions interruptions;
};

const RegularMarketTimes REGULAR_MARKET_TIMES{
    {EpochFrameMarketTimeType::MarketOpen, MarketTimes{MarketTime{Time{0h}}}},
    {EpochFrameMarketTimeType::MarketClose, MarketTimes{MarketTime{Time{23h}}}}
};

const OpenCloseMap OPEN_CLOSE_MAP{
    {EpochFrameMarketTimeType::MarketOpen, EpochFrameOpenCloseType::True},
    {EpochFrameMarketTimeType::MarketClose, EpochFrameOpenCloseType::False},
    {EpochFrameMarketTimeType::BreakStart, EpochFrameOpenCloseType::False},
    {EpochFrameMarketTimeType::BreakEnd, EpochFrameOpenCloseType::True},
    {EpochFrameMarketTimeType::Pre, EpochFrameOpenCloseType::True},
    {EpochFrameMarketTimeType::Post, EpochFrameOpenCloseType::False}
};

class MarketCalendar {
    public:
        MarketCalendar(std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time, const MarketCalendarOptions& options);

        std::string name() const noexcept {
            return m_options.name;
        }

        std::string tz() const noexcept {
            return m_options.tz;
        }

        std::vector<EpochFrameMarketTimeType> market_times() const noexcept {
            return m_market_times;
        }

        void add_time(EpochFrameMarketTimeType market_time, const std::vector<MarketTime>& times, EpochFrameOpenCloseType opens = EpochFrameOpenCloseType::Default);

        void remove_time(EpochFrameMarketTimeType market_time);

        bool is_custom(EpochFrameMarketTimeType market_time) const noexcept {
            return m_customized_market_times.contains(market_time);
        }

        bool has_custom() const noexcept {
            return !m_customized_market_times.empty();
        }

        bool is_discontinued(EpochFrameMarketTimeType market_time) const noexcept {
            return m_discontinued_market_times.contains(market_time);
        }

        bool has_discontinued() const noexcept {
            return !m_discontinued_market_times.empty();
        }

        std::vector<MarketTime> get_time(EpochFrameMarketTimeType market_time, bool all_times = false) const;

        std::optional<MarketTime> get_time_on(EpochFrameMarketTimeType market_time, const Date& date) const;

        std::optional<MarketTime> open_time_on(const Date& date) const {
            return get_time_on(EpochFrameMarketTimeType::MarketOpen, date);
        }

        std::optional<MarketTime> close_time_on(const Date& date) const {
            return get_time_on(EpochFrameMarketTimeType::MarketClose, date);
        }
        
        std::optional<MarketTime> break_start_on(const Date& date) const {
            return get_time_on(EpochFrameMarketTimeType::BreakStart, date);
        }

        std::optional<MarketTime> break_end_on(const Date& date) const {
            return get_time_on(EpochFrameMarketTimeType::BreakEnd, date);
        }

        std::vector<MarketTime> open_time () const {
            return get_time(EpochFrameMarketTimeType::MarketOpen);
        }

        std::vector<MarketTime> close_time() const {
            return get_time(EpochFrameMarketTimeType::MarketClose);
        }

        std::vector<MarketTime> break_start() const {
            return get_time(EpochFrameMarketTimeType::BreakStart);
        }

        std::vector<MarketTime> break_end() const {
            return get_time(EpochFrameMarketTimeType::BreakEnd);
        }

        AbstractHolidayCalendarPtr regular_holidays() const {
            return m_options.regular_holidays;
        }

        np::HolidayList adhoc_holidays() const {
            return m_options.adhoc_holidays;
        }

        np::WeekSet weekmask() const {
            return m_options.weekmask;
        }

        SpecialTimes special_opens() const {
            return m_options.special_opens;
        }

        SpecialTimes special_opens_adhoc() const {
            return m_options.special_opens_adhoc;
        }

        SpecialTimes special_closes() const {
            return m_options.special_closes;
        }

        SpecialTimes special_closes_adhoc() const {
            return m_options.special_closes_adhoc;
        }

        SpecialTimes get_special_times(EpochFrameMarketTimeType market_time) const;

        SpecialTimes get_special_times_adhoc(EpochFrameMarketTimeType market_time) const;

        int64_t get_offset(EpochFrameMarketTimeType market_time) const{
            return get_time(market_time, true).back().day_offset;
        }

        int64_t open_offset() const {
            return get_offset(EpochFrameMarketTimeType::MarketOpen);
        }

        int64_t close_offset() const {
            return get_offset(EpochFrameMarketTimeType::MarketClose);
        }        
        
        Interruptions interruptions() const noexcept {
            return m_options.interruptions;
        }

        DataFrame interruptions_df() const;

        std::shared_ptr<CustomBusinessDay> holidays() const{
            return m_holidays;
        }

        IndexPtr valid_days(const Date& start_date, const Date& end_date, std::string const& tz = "UTC") const;

        IndexPtr days_at_time(IndexPtr const& days, Time market_time, int64_t day_offset = 0);

        Series special_dates(EpochFrameMarketTimeType market_time, const Date& start, const Date& end, bool filter_holidays = true);

        DataFrame schedule(const Date& start_date, const Date& end_date, std::string const& tz = "UTC", 
        EpochFrameMarketTimeType start = EpochFrameMarketTimeType::MarketOpen, 
        EpochFrameMarketTimeType end = EpochFrameMarketTimeType::MarketClose, 
        bool force_special_times = true, std::vector<EpochFrameMarketTimeType> market_times = {}, bool interruptions = false);

        DataFrame schedule_from_days(IndexPtr const& days, std::string const& tz = "UTC", 
        EpochFrameMarketTimeType start = EpochFrameMarketTimeType::MarketOpen, 
        EpochFrameMarketTimeType end = EpochFrameMarketTimeType::MarketClose, 
        bool force_special_times = true, std::vector<EpochFrameMarketTimeType> market_times = {}, bool interruptions = false);

        DataFrame date_range_htf(const DateOffsetHandlerPtr& frequency, const Date& start, const Date& end, 
        std::optional<int64_t> periods = {}, std::optional<bool> closed = {});

    private:
        MarketCalendarOptions m_options;
        std::shared_ptr<CustomBusinessDay> m_holidays;
        std::unordered_map<EpochFrameMarketTimeType, std::vector<TimeDelta>> m_regular_tds;
        std::unordered_map<EpochFrameMarketTimeType, std::vector<Time>> m_discontinued_market_times;
        std::vector<EpochFrameMarketTimeType> m_market_times;
        std::vector<EpochFrameMarketTimeType> m_oc_market_times;
        std::set<EpochFrameMarketTimeType> m_customized_market_times;

        static TimeDelta _tdelta(const Time& time, int64_t day_offset = 0);

        void change_time(EpochFrameMarketTimeType type, const std::vector<MarketTime>& times, EpochFrameOpenCloseType opens = EpochFrameOpenCloseType::Default);
        void set_time(EpochFrameMarketTimeType type, const std::vector<MarketTime>& times, EpochFrameOpenCloseType opens = EpochFrameOpenCloseType::Default);
        void prepare_regular_market_times();

        Series convert(Series const& col) const;

        std::string col_name(uint64_t n) const{
            return n%2 == 1 ? fmt::format("interruption_start_{}", std::floor(n/2 + 1)) : fmt::format("interruption_end_{}", std::floor(n/2));
        }

        std::vector<EpochFrameMarketTimeType> market_times(EpochFrameMarketTimeType start, EpochFrameMarketTimeType end) const noexcept;

        IndexPtr tryholidays(const AbstractHolidayCalendarPtr& cal, const Date& s, const Date& e);

        Series special_dates(std::vector<std::pair<Time, std::variant<IndexPtr, uint8_t>>> calendars, 
        std::vector<std::pair<Time, IndexPtr>> ad_hoc_dates, const Date& start, const Date& end);

};
}
