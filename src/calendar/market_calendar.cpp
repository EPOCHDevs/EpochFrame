#include "epoch_frame/market_calendar.h"
#include "epoch_frame/calendar_common.h"
#include <algorithm>
#include <chrono>
#include <epoch_core/common_utils.h>
#include <epoch_core/ranges_to.h>
#include <epoch_frame/common.h>
#include <epoch_frame/series.h>

#include "epoch_frame/calendar_utils.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/frame_or_series.h"
#include "epoch_frame/index.h"

namespace cal_utils = epoch_frame::calendar::utils;
namespace epoch_frame::calendar
{

    TimeDelta MarketCalendar::_tdelta(const std::optional<Time>& time,
                                      std::optional<int64_t>     day_offset)
    {
        AssertFromFormat(time.has_value(), "Time is not set");
        auto time_value = time.value();
        return TimeDelta({.days    = static_cast<double>(day_offset.value_or(0)),
                          .seconds = static_cast<double>(time_value.second.count()),
                          .minutes = static_cast<double>(time_value.minute.count()),
                          .hours   = static_cast<double>(time_value.hour.count())});
    }

    MarketCalendar::MarketCalendar(std::optional<MarketTime> const& open_time,
                                   std::optional<MarketTime> const& close_time,
                                   const MarketCalendarOptions&     options)
        : m_options(options)
    {
        // Then set custom open/close times if provided
        if (open_time)
        {
            change_time(epoch_core::MarketTimeType::MarketOpen, {*open_time});
        }

        if (close_time)
        {
            change_time(epoch_core::MarketTimeType::MarketClose, {*close_time});
        }

        if (m_market_times.empty())
        {
            prepare_regular_market_times();
        }

        m_holidays = std::make_shared<CustomBusinessDay>(
            BusinessMixinParams{np::to_weekmask(m_options.weekmask), m_options.adhoc_holidays,
                                m_options.regular_holidays});
    }

    void MarketCalendar::prepare_regular_market_times()
    {
        auto oc_map = m_options.open_close_map;
        AssertFromFormat(std::ranges::all_of(oc_map | std::views::values,
                                             [](epoch_core::OpenCloseType value)
                                             {
                                                 return value == epoch_core::OpenCloseType::True ||
                                                        value == epoch_core::OpenCloseType::False;
                                             }),
                         "Values in open_close_map need to be true or false");

        auto                regular = m_options.regular_market_times;
        ProtectedDict<Date> discontinued;
        std::stringstream   disconnected_keys;
        for (const auto& [market_time, times] : m_options.regular_market_times)
        {
            int offset = 0;
            if (times.back().time == std::nullopt)
            {
                try
                {
                    discontinued.insert_or_assign(market_time, times.back().date.value());
                }
                catch (std::exception& e)
                {
                    SPDLOG_ERROR("Error setting discontinued market time: {}", e.what());
                }
                offset = 1;
                disconnected_keys << epoch_core::MarketTimeTypeWrapper::ToString(market_time)
                                  << ", ";
            }

            MarketTimesWithTZ            market_times_with_tz;
            std::vector<MarketTimeDelta> tds;
            for (auto const& t : times | std::views::take(times.size() - offset))
            {
                tds.emplace_back(t.date, _tdelta(t.time, t.day_offset));
                market_times_with_tz.emplace_back(t.time.value(), t.day_offset, t.date);
            }
            m_regular_tds.insert_or_assign(market_time, tds);
            m_regular_market_times.insert_or_assign(market_time, market_times_with_tz);
        }

        if (!discontinued.empty())
        {
            SPDLOG_WARN("Discontinued market times: {}", disconnected_keys.str());
        }

        m_discontinued_market_times = discontinued;

        auto keys = m_options.regular_market_times | std::views::keys;
        m_market_times.assign(keys.begin(), keys.end());
        std::ranges::sort(
            m_market_times, [this](auto const& a, auto const& b)
            { return m_regular_tds[a].back().time_delta < m_regular_tds[b].back().time_delta; });

        m_oc_market_times = m_market_times |
                            std::views::filter([&](auto const& market_time)
                                               { return oc_map.contains(market_time); }) |
                            epoch_core::ranges::to_vector_v;
    }

    void MarketCalendar::set_time(epoch_core::MarketTimeType     market_time,
                                  const std::vector<MarketTime>& times,
                                  epoch_core::OpenCloseType      opens)
    {
        if (opens == epoch_core::OpenCloseType::Default)
        {
            opens = lookupDefault(OPEN_CLOSE_MAP, market_time, epoch_core::OpenCloseType::Null);
        }

        if (opens == epoch_core::OpenCloseType::True || opens == epoch_core::OpenCloseType::False)
        {
            m_options.open_close_map.insert_or_assign(market_time, opens);
        }
        else if (opens == epoch_core::OpenCloseType::Null)
        {
            m_options.open_close_map.erase(market_time);
        }

        m_options.regular_market_times.insert_or_assign(market_time, times);

        if (!is_custom(market_time))
        {
            m_customized_market_times.insert(market_time);
        }

        prepare_regular_market_times();
    }

    void MarketCalendar::change_time(epoch_core::MarketTimeType     type,
                                     const std::vector<MarketTime>& times,
                                     epoch_core::OpenCloseType      opens)
    {
        // Check if the market time type exists
        AssertFromFormat(m_options.regular_market_times.contains(type),
                         "{} is not in regular market times",
                         epoch_core::MarketTimeTypeWrapper::ToString(type));
        set_time(type, times, opens);
    }

    void MarketCalendar::add_time(epoch_core::MarketTimeType     market_time,
                                  const std::vector<MarketTime>& times,
                                  epoch_core::OpenCloseType      opens)
    {
        AssertFromFormat(!m_options.regular_market_times.contains(market_time),
                         "{} is already in regular market times",
                         epoch_core::MarketTimeTypeWrapper::ToString(market_time));
        set_time(market_time, times, opens);
    }

    void MarketCalendar::remove_time(epoch_core::MarketTimeType market_time)
    {
        m_options.regular_market_times.erase(market_time);
        m_options.open_close_map.erase(market_time);
        prepare_regular_market_times();
        if (is_custom(market_time))
        {
            m_customized_market_times.erase(market_time);
        }
    }

    std::vector<MarketTimeWithTZ> MarketCalendar::get_time(epoch_core::MarketTimeType market_time,
                                                           bool all_times) const
    {
        if (!m_options.regular_market_times.contains(market_time))
        {
            if (market_time == epoch_core::MarketTimeType::BreakStart ||
                market_time == epoch_core::MarketTimeType::BreakEnd)
            {
                return {};
            }
            if (market_time == epoch_core::MarketTimeType::MarketOpen ||
                market_time == epoch_core::MarketTimeType::MarketClose)
            {
                throw std::runtime_error("You need to set market_times");
            }
            throw std::runtime_error("Market time " +
                                     epoch_core::MarketTimeTypeWrapper::ToString(market_time) +
                                     " is not in regular market times");
        }

        const auto times = epoch_core::lookup(m_regular_market_times, market_time);
        if (all_times)
        {
            return times;
        }
        auto time    = times.back();
        time.time.tz = m_options.tz;
        return {time};
    }

    std::optional<MarketTimeWithTZ>
    MarketCalendar::get_time_on(epoch_core::MarketTimeType market_time, const Date& date) const
    {
        auto times = get_time(market_time, true);
        if (times.empty())
        {
            return {};
        }
        auto view = times | std::views::reverse;
        auto iter = std::ranges::find_if(view,
                                 [&](auto const& t) { return !t.date || t.date.value() < date; });
        if (iter == view.end())
        {
            return {};
        }
        auto time    = *iter;
        time.time.tz = m_options.tz;
        return time;
    }

    SpecialTimes MarketCalendar::get_special_times(epoch_core::MarketTimeType market_time) const
    {
        switch (market_time)
        {
            case epoch_core::MarketTimeType::MarketOpen:
                return m_options.special_opens;
            case epoch_core::MarketTimeType::MarketClose:
                return m_options.special_closes;
            default:
                return {};
        }
    }

    SpecialTimesAdHoc
    MarketCalendar::get_special_times_adhoc(epoch_core::MarketTimeType market_time) const
    {
        switch (market_time)
        {
            case epoch_core::MarketTimeType::MarketOpen:
                return m_options.special_opens_adhoc;
            case epoch_core::MarketTimeType::MarketClose:
                return m_options.special_closes_adhoc;
            default:
                return {};
        }
    }

    IndexPtr MarketCalendar::valid_days(const Date& start_date, const Date& end_date,
                                        std::string const& tz) const
    {
        return factory::index::date_range({.start  = DateTime{start_date}.timestamp(),
                                           .end    = DateTime{end_date}.timestamp(),
                                           .offset = m_holidays,
                                           .tz     = tz});
    }

    std::vector<epoch_core::MarketTimeType>
    MarketCalendar::market_times(epoch_core::MarketTimeType start,
                                 epoch_core::MarketTimeType end) const noexcept
    {
        return m_market_times |
               std::views::filter([&](auto const& market_time)
                                  { return market_time >= start && market_time <= end; }) |
               epoch_core::ranges::to_vector_v;
    }

    Series MarketCalendar::days_at_time(IndexPtr const& days, const MarketTimeVariant& market_time,
                                        int64_t day_offset) const
    {
        const Array localized_days = days->tz_localize("")->array();

        auto datetimes = std::visit(
            [&]<typename T>(T const& arg) -> Array
            {
                if constexpr (std::is_same_v<T, Time>)
                {
                    return localized_days + Scalar(_tdelta(arg, day_offset));
                }
                else
                {
                    auto timedeltas = m_regular_tds.at(arg);
                    auto _datetimes = localized_days + Scalar{timedeltas.front().time_delta};
                    for (auto const& [cut_off, timedelta] : timedeltas | std::views::drop(1))
                    {
                        AssertFromFormat(cut_off, "cut_off is none");
                        _datetimes = _datetimes.where(localized_days < Scalar{*cut_off},
                                                      localized_days + Scalar{timedelta});
                    }
                    return _datetimes;
                }
            },
            market_time);

        const auto arr = datetimes.dt().tz_localize(m_options.tz).dt().tz_convert("UTC");
        return Series(days, arr.value());
    }

    IndexPtr MarketCalendar::try_holidays(const AbstractHolidayCalendarPtr& cal, const Date& s,
                                          const Date& e) const
    {
        AssertFromFormat(cal, "Calendar must be a valid calendar");
        auto observed_dates = cal_utils::all_single_observance_rules(*cal);
        if (!observed_dates)
        {
            return cal->holidays(DateTime{s}, DateTime{e});
        }

        auto timestamps =
            std::views::filter(*observed_dates, [&](DateTime const& date_time)
                               { return s <= date_time.date() && date_time.date() <= e; }) |
            epoch_core::ranges::to_vector_v;

        return factory::index::make_datetime_index(timestamps);
    }

    Series MarketCalendar::special_dates(epoch_core::MarketTimeType market_time,
                                         const Date& start_date, const Date& end_date,
                                         bool filter_holidays) const
    {
        SpecialTimes      calendars = get_special_times(market_time);
        SpecialTimesAdHoc ad_hoc    = get_special_times_adhoc(market_time);
        Series            special   = special_dates(calendars, ad_hoc, start_date, end_date);

        if (filter_holidays)
        {
            Array valid = valid_days(start_date, end_date, "")->array();
            special     = special.loc(special.index()->isin(valid));
        }

        return special;
    }

    Series MarketCalendar::special_dates(SpecialTimes const&      calendars,
                                         SpecialTimesAdHoc const& ad_hoc_dates, const Date& start,
                                         const Date& end) const
    {
        std::vector<FrameOrSeries> indexes;
        for (auto const& [time, calendar, day_offset] : calendars)
        {
            indexes.emplace_back(
                days_at_time(try_holidays(calendar, start, end), time, day_offset));
        }
        return special_dates(indexes, ad_hoc_dates, start, end);
    }

    Series MarketCalendar::special_dates(
        std::vector<std::pair<Time, epoch_core::EpochDayOfWeek>> const& calendars,
        SpecialTimesAdHoc const& ad_hoc_dates, const Date& start, const Date& end) const
    {
        std::vector<FrameOrSeries> indexes;
        for (auto const& [time, day_of_week] : calendars)
        {
            auto day_of_week_freq =
                factory::offset::cbday({.weekmask = np::to_weekmask({day_of_week})});
            indexes.emplace_back(
                days_at_time(factory::index::date_range({.start  = DateTime{start}.timestamp(),
                                                         .end    = DateTime{end}.timestamp(),
                                                         .offset = day_of_week_freq}),
                             time));
        }
        return special_dates(indexes, ad_hoc_dates, start, end);
    }

    Series MarketCalendar::special_dates(std::vector<FrameOrSeries>& indexes,
                                         SpecialTimesAdHoc const& ad_hoc_dates, const Date& start,
                                         const Date& end) const
    {
        for (auto const& [time, dates, day_offset] : ad_hoc_dates)
        {
            indexes.push_back(days_at_time(dates, time, day_offset));
        }

        if (indexes.empty())
        {
            return Series(factory::index::make_datetime_index(std::vector<DateTime>{}),
                          factory::array::make_chunked_array(
                              arrow::ScalarVector{}, arrow::timestamp(arrow::TimeUnit::NANO)));
        }
        auto concat_series = concat({.frames = indexes}).to_series();
        return concat_series.loc(
            {Scalar(start),
             Scalar(DateTime{end, Time{.hour = 23h, .minute = 59min, .second = 59s}})});
    }

    arrow::SchemaPtr
    MarketCalendar::get_schedule_schema(const std::vector<epoch_core::MarketTimeType>& market_times)
    {
        arrow::FieldVector fields;
        for (auto const& market_time : market_times)
        {
            fields.push_back(arrow::field(epoch_core::MarketTimeTypeWrapper::ToString(market_time),
                                          arrow::timestamp(arrow::TimeUnit::NANO)));
        }
        return arrow::schema(fields);
    }

    std::vector<epoch_core::MarketTimeType>
    MarketCalendar::get_market_times(epoch_core::MarketTimeType start,
                                     epoch_core::MarketTimeType end) const
    {
        auto start_it = std::ranges::find(m_market_times, start);
        auto end_it   = std::find(start_it, m_market_times.end(), end);
        AssertFromFormat(start_it != m_market_times.end(), "Start market time {} not found",
                         epoch_core::MarketTimeTypeWrapper::ToString(start));
        AssertFromFormat(end_it != m_market_times.end(), "End market time {} not found",
                         epoch_core::MarketTimeTypeWrapper::ToString(end));
        return std::vector(start_it, end_it + 1);
    }

    std::vector<epoch_core::MarketTimeType>
    MarketCalendar::get_market_times_from_filter(epoch_core::MarketTimeType start,
                                                 epoch_core::MarketTimeType end,
                                                 MarketTimeFilter const&    filter) const
    {
        return std::visit(
            [&]<typename T>(T const& arg) -> std::vector<epoch_core::MarketTimeType>
            {
                if constexpr (std::is_same_v<T, NoMarketTime>)
                {
                    return get_market_times(start, end);
                }
                else if constexpr (std::is_same_v<T, AllMarketTimes>)
                {
                    return m_market_times;
                }
                else
                {
                    return arg;
                }
            },
            filter);
    }

    DataFrame MarketCalendar::schedule(const Date& start_date, const Date& end_date,
                                       ScheduleOptions const& options) const
    {
        AssertFromFormat(start_date <= end_date, "start_date must be before or equal to end_date");
        auto all_days = valid_days(start_date, end_date);

        const auto market_times =
            get_market_times_from_filter(options.start, options.end, options.market_times);

        if (all_days->empty())
        {
            return DataFrame(
                factory::index::make_datetime_index(std::vector<arrow::TimestampScalar>{}),
                arrow::Table::Make(get_schedule_schema(market_times), arrow::ChunkedArrayVector{}));
        }

        auto clone_options         = options;
        clone_options.market_times = market_times;
        return schedule_from_days(all_days, clone_options);
    }

    DataFrame MarketCalendar::schedule_from_days(IndexPtr const&        days_,
                                                 ScheduleOptions const& options) const
    {
        auto days = days_->normalize()->tz_localize("");
        auto market_times =
            get_market_times_from_filter(options.start, options.end, options.market_times);
        auto adj_others = options.force_special_times == epoch_core::BooleanEnum::True;
        auto adj_col    = options.force_special_times != epoch_core::BooleanEnum::Null;

        IndexPtr  open_adj, close_adj;
        DataFrame schedule;

        auto start_date = days->at(0).to_datetime().date();
        auto end_date   = days->at(-1).to_datetime().date();

        for (auto const& market_time : market_times)
        {
            Series temp = days_at_time(days, market_time, 0);
            if (adj_col)
            {
                auto     special = special_dates(market_time, start_date, end_date, false);
                IndexPtr special_ix =
                    special.index()->loc(special.index()->isin(temp.index()->array()));
                temp = temp.assign(special_ix, special.array());

                if (adj_others)
                {
                    if (market_time == epoch_core::MarketTimeType::MarketOpen)
                    {
                        open_adj = special_ix;
                    }
                    else if (market_time == epoch_core::MarketTimeType::MarketClose)
                    {
                        close_adj = special_ix;
                    }
                }
            }
            schedule =
                schedule.assign(epoch_core::MarketTimeTypeWrapper::ToString(market_time), temp);
        }

        auto schema = schedule.table()->schema();
        if (adj_others && open_adj && open_adj->size() > 0)
        {
            auto mk_open_ind = schema->GetFieldIndex(epoch_core::MarketTimeTypeWrapper::ToString(
                epoch_core::MarketTimeType::MarketOpen));
            AssertFromFormat(mk_open_ind != -1, "Market open index not found");
            auto adj_opens = [&](Array const& x)
            { return x.where(x > x[mk_open_ind], x[mk_open_ind]); };
            schedule = schedule.assign(schedule.loc(open_adj).apply(adj_opens, AxisType::Row));
        }

        if (adj_others && close_adj && close_adj->size() > 0)
        {
            auto mk_close_ind = schema->GetFieldIndex(epoch_core::MarketTimeTypeWrapper::ToString(
                epoch_core::MarketTimeType::MarketClose));
            AssertFromFormat(mk_close_ind != -1, "Market close index not found");
            auto adj_close = [&](Array const& x)
            { return x.where(x < x[mk_close_ind], x[mk_close_ind]); };
            schedule = schedule.assign(schedule.loc(close_adj).apply(adj_close, AxisType::Row));
        }

        AssertFalseFromFormat(options.interruptions, "Interruptions are not supported yet");

        if (options.tz != "UTC")
        {
            schedule = schedule.apply(
                [&](Series const& x)
                { return Series(x.index(), x.dt().tz_convert(options.tz).as_chunked_array()); });
        }

        return schedule;
    }

    IndexPtr MarketCalendar::date_range_htf(Date const& start, Date const& end,
                                            std::optional<int64_t> periods) const
    {
        auto options = cal_utils::DateRangeHTFOptions{
            .calendar = m_holidays, .start = start, .end = end, .periods = periods};
        return cal_utils::date_range_htf(options);
    }

    // open_at_time MarketCalendar::open_at_time(DataFrame const& schedule, DateTime const& dt, bool
    // include_close, bool only_rth) const {
    //     auto timestamp = dt.tz_localize("UTC").timestamp();
    //     auto cols = schedule.column_names();
    //     // TODO: check interruptions when implemented

    //     epoch_core::MarketTimeType lowest{epoch_core::MarketTimeType::MarketOpen},
    //     highest{epoch_core::MarketTimeType::MarketClose}; if (!only_rth) {
    //         auto ix = cols | std::views::transform([&](auto const& col) {
    //             auto str = epoch_core::MarketTimeTypeWrapper::FromString(col);
    //             auto idx = std::ranges::find(m_market_times, str);
    //             AssertFromFormat(idx != m_market_times.end(), "Market time {} not found", col);
    //             return std::distance(m_market_times.begin(), idx);
    //         }) | epoch_core::ranges::to_vector_v;
    //         std::ranges::sort(ix);
    //         lowest = ix.front();
    //         highest = ix.back();
    //     }
    //     std::string lowest_str = epoch_core::MarketTimeTypeWrapper::ToString(lowest);
    //     std::string highest_str = epoch_core::MarketTimeTypeWrapper::ToString(highest);

    //     AssertFromFormat(timestamp < schedule[lowest_str].at(0) || timestamp >
    //     schedule[highest_str].at(-1), "The provided timestamp is not covered by the schedule");

    //     Series day = schedule.loc(schedule[lowest_str] <=
    //     timestamp).iloc(-1).dropna().sortvalues(); day = day.loc({Scalar{lowest_str},
    //     Scalar{highest_str}}); day = day.index()->to_series(day.array());

    //     // TODO filter if interruptions are implemented

    //     const Array below = (include_close) ? (day.index()->array() < timestamp) :
    //     (day.index()->array() <= timestamp); return day.loc(below).at(-1).as_bool();
    //     }
    // }
} // namespace epoch_frame::calendar
