#pragma once

#include "calendar_common.h"
#include <common/python_utils.h>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace epoch_frame::calendar
{
    class MarketCalendar
    {
      public:
        MarketCalendar(std::optional<MarketTime> const& open_time,
                       std::optional<MarketTime> const& close_time,
                       const MarketCalendarOptions&     options);

        std::string name() const noexcept
        {
            return m_options.name;
        }

        std::vector<std::string> aliases() const noexcept
        {
            return m_options.aliases;
        }

        std::string tz() const noexcept
        {
            return m_options.tz;
        }

        std::vector<epoch_core::MarketTimeType> market_times() const noexcept
        {
            return m_market_times;
        }

        void add_time(epoch_core::MarketTimeType market_time, const std::vector<MarketTime>& times,
                      epoch_core::OpenCloseType opens = epoch_core::OpenCloseType::Default);

        void remove_time(epoch_core::MarketTimeType market_time);

        bool is_custom(epoch_core::MarketTimeType market_time) const noexcept
        {
            return m_customized_market_times.contains(market_time);
        }

        bool has_custom() const noexcept
        {
            return !m_customized_market_times.empty();
        }

        bool is_discontinued(epoch_core::MarketTimeType market_time) const noexcept
        {
            return m_discontinued_market_times.contains(market_time);
        }

        bool has_discontinued() const noexcept
        {
            return !m_discontinued_market_times.empty();
        }

        void change_time(epoch_core::MarketTimeType type, const std::vector<MarketTime>& times,
                         epoch_core::OpenCloseType opens = epoch_core::OpenCloseType::Default);

        std::vector<MarketTimeWithTZ> get_time(epoch_core::MarketTimeType market_time,
                                               bool                       all_times = false) const;

        std::optional<MarketTimeWithTZ> get_time_on(epoch_core::MarketTimeType market_time,
                                                    const Date&                date) const;

        std::optional<MarketTimeWithTZ> open_time_on(const Date& date) const
        {
            return get_time_on(epoch_core::MarketTimeType::MarketOpen, date);
        }

        std::optional<MarketTimeWithTZ> close_time_on(const Date& date) const
        {
            return get_time_on(epoch_core::MarketTimeType::MarketClose, date);
        }

        std::optional<MarketTimeWithTZ> break_start_on(const Date& date) const
        {
            return get_time_on(epoch_core::MarketTimeType::BreakStart, date);
        }

        std::optional<MarketTimeWithTZ> break_end_on(const Date& date) const
        {
            return get_time_on(epoch_core::MarketTimeType::BreakEnd, date);
        }

        std::vector<MarketTimeWithTZ> open_time() const
        {
            return get_time(epoch_core::MarketTimeType::MarketOpen);
        }

        std::vector<MarketTimeWithTZ> close_time() const
        {
            return get_time(epoch_core::MarketTimeType::MarketClose);
        }

        std::vector<MarketTimeWithTZ> break_start() const
        {
            return get_time(epoch_core::MarketTimeType::BreakStart);
        }

        std::vector<MarketTimeWithTZ> break_end() const
        {
            return get_time(epoch_core::MarketTimeType::BreakEnd);
        }

        AbstractHolidayCalendarPtr regular_holidays() const
        {
            return m_options.regular_holidays;
        }

        np::HolidayList adhoc_holidays() const
        {
            return m_options.adhoc_holidays;
        }

        np::WeekSet weekmask() const
        {
            return m_options.weekmask;
        }

        SpecialTimes special_opens() const
        {
            return m_options.special_opens;
        }

        SpecialTimesAdHoc special_opens_adhoc() const
        {
            return m_options.special_opens_adhoc;
        }

        SpecialTimes special_closes() const
        {
            return m_options.special_closes;
        }

        SpecialTimesAdHoc special_closes_adhoc() const
        {
            return m_options.special_closes_adhoc;
        }

        SpecialTimes get_special_times(epoch_core::MarketTimeType market_time) const;

        SpecialTimesAdHoc get_special_times_adhoc(epoch_core::MarketTimeType market_time) const;

        int64_t get_offset(epoch_core::MarketTimeType market_time) const
        {
            return get_time(market_time, true).back().day_offset.value_or(0);
        }

        int64_t open_offset() const
        {
            return get_offset(epoch_core::MarketTimeType::MarketOpen);
        }

        int64_t close_offset() const
        {
            return get_offset(epoch_core::MarketTimeType::MarketClose);
        }

        Interruptions interruptions() const noexcept
        {
            return m_options.interruptions;
        }

        DataFrame interruptions_df() const;

        std::shared_ptr<CustomBusinessDay> holidays() const
        {
            return m_holidays;
        }

        virtual IndexPtr valid_days(const Date& start_date, const Date& end_date,
                                    std::string const& tz = "UTC") const;

        virtual Series days_at_time(IndexPtr const& days, const MarketTimeVariant& market_time,
                                    int64_t day_offset = 0) const;

        Series special_dates(epoch_core::MarketTimeType market_time, const Date& start,
                             const Date& end, bool filter_holidays = true) const;

        DataFrame schedule(const Date& start_date, const Date& end_date,
                           ScheduleOptions const& options) const;

        DataFrame schedule_from_days(IndexPtr const&        days,
                                     ScheduleOptions const& options = {}) const;

        virtual IndexPtr date_range_htf(Date const& start, Date const& end,
                                        std::optional<int64_t> periods = {}) const;

        bool open_at_time(DataFrame const& schedule, DateTime const& timestamp,
                          bool include_close = false, bool only_rth = false) const;

      protected:

        MarketCalendarOptions                                                        m_options;
        std::shared_ptr<CustomBusinessDay>                                           m_holidays;
        std::unordered_map<epoch_core::MarketTimeType, std::vector<MarketTimeDelta>> m_regular_tds;
        std::unordered_map<epoch_core::MarketTimeType, Date> m_discontinued_market_times;
        std::vector<epoch_core::MarketTimeType>              m_market_times;
        std::vector<epoch_core::MarketTimeType>              m_oc_market_times;
        std::set<epoch_core::MarketTimeType>                 m_customized_market_times;
        RegularMarketTimesWithTZ                             m_regular_market_times;

        static TimeDelta _tdelta(const std::optional<Time>& time,
                                 std::optional<int64_t>     day_offset);

        void set_time(epoch_core::MarketTimeType type, const std::vector<MarketTime>& times,
                      epoch_core::OpenCloseType opens = epoch_core::OpenCloseType::Default);
        void prepare_regular_market_times();

        Series convert(Series const& col) const;

        std::string col_name(uint64_t n) const
        {
            return n % 2 == 1 ? fmt::format("interruption_start_{}", std::floor(n / 2 + 1))
                              : fmt::format("interruption_end_{}", std::floor(n / 2));
        }

        std::vector<epoch_core::MarketTimeType>
        market_times(epoch_core::MarketTimeType start,
                     epoch_core::MarketTimeType end) const noexcept;

        IndexPtr try_holidays(const AbstractHolidayCalendarPtr& cal, const Date& s,
                              const Date& e) const;

        Series
        special_dates(std::vector<std::pair<Time, epoch_core::EpochDayOfWeek>> const& calendars,
                      SpecialTimesAdHoc const& ad_hoc_dates, const Date& start,
                      const Date& end) const;

        Series special_dates(SpecialTimes const& calendars, SpecialTimesAdHoc const& ad_hoc_dates,
                             const Date& start, const Date& end) const;

        Series special_dates(std::vector<FrameOrSeries>& indexes,
                             SpecialTimesAdHoc const& ad_hoc_dates, const Date& start,
                             const Date& end) const;

        static arrow::SchemaPtr
        get_schedule_schema(const std::vector<epoch_core::MarketTimeType>& market_times);

        std::vector<epoch_core::MarketTimeType>
        get_market_times_from_filter(epoch_core::MarketTimeType start,
                                     epoch_core::MarketTimeType end,
                                     MarketTimeFilter const&    filter) const;

        std::vector<epoch_core::MarketTimeType>
        get_market_times(epoch_core::MarketTimeType start, epoch_core::MarketTimeType end) const;
    };

    using MarketCalendarPtr = std::shared_ptr<MarketCalendar>;

} // namespace epoch_frame::calendar
