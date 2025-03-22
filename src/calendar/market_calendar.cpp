#include "market_calendar.h"
#include <algorithm>
#include <chrono>
#include <common/asserts.h>
#include <epoch_frame/series.h>


namespace epoch_frame::calendar {

    TimeDelta MarketCalendar::_tdelta(const Time& time, int64_t day_offset) {
        return TimeDelta({.days = static_cast<double>(day_offset),
                         .seconds = static_cast<double>(time.second.count()),
                         .minutes = static_cast<double>(time.minute.count()),
                         .hours = static_cast<double>(time.hour.count())});
    }


MarketCalendar::MarketCalendar(std::optional<MarketTime> const& open_time, std::optional<MarketTime> const& close_time, const MarketCalendarOptions& options)
    : m_options(options) {

    // Then set custom open/close times if provided
    if (!open_time) {
       change_time(EpochFrameMarketTimeType::MarketOpen, {*open_time});
    }

    if (!close_time) {
        change_time(EpochFrameMarketTimeType::MarketClose, {*close_time});
    }

    if (!m_options.has_market_times) {
        prepare_regular_market_times();
    }

    m_holidays = std::make_shared<CustomBusinessDay>(1, np::to_weekmask(m_options.weekmask), m_options.adhoc_holidays, m_options.regular_holidays);
}

void MarketCalendar::prepare_regular_market_times() {
    auto oc_map = m_options.open_close_map;
    AssertFromFormat(std::all_of(oc_map.begin(), oc_map.end(), [](auto const& pair) {
        return pair.second == true || pair.second == false;
    }), "Values in open_close_map need to be true or false");

    auto regular = m_options.regular_market_times;
    ProtectedDict<std::optional<Time>> discontinued;
    std::stringstream disconnected_keys;
    for (auto const& [market_time, times] : regular) {

        if (!times.back().time) {
            discontinued._set(market_time, times.back().time);
            times = times[:-1];
            regular._set(market_time, times);
            disconnected_keys << EpochFrameMarketTimeTypeWrapper::ToString(market_time) << ", ";
        }

        for (auto const& t : times) {
            m_regular_tds[market_time].push_back(_tdelta(t.time, t.day_offset));
        }
    }

    if (!discontinued.empty()) {
        SPDLOG_WARN("Discontinued market times: {}", disconnected_keys.str());
    }

    m_discontinued_market_times = discontinued.dict();
    m_options.regular_market_times = regular;
    m_regular_market_time_deltas = regular_tds;

    m_market_times = m_options.regular_market_times | ranges::views::keys | ranges::to_vector | ranges::actions::sort([](auto const& a, auto const& b) {
        return m_regular_market_time_deltas[a].back().time < m_regular_market_time_deltas[b].back().time;
    });

    m_oc_market_times = m_market_times | ranges::views::filter([&](auto const& market_time) {
        return m_options.open_close_map.contains(market_time);
    }) | ranges::to_vector;

}


void MarketCalendar::set_time(EpochFrameMarketTimeType market_time, const std::vector<MarketTime>& times, EpochFrameOpenCloseType opens) {
    if (opens == EpochFrameOpenCloseType::Default) {
        opens = m_options.open_close_map[market_time];
    }

    AssertWithTraceFromStream(m_options.regular_market_times.contains(market_time),
                             EpochFrameMarketTimeTypeWrapper::ToString(market_time) + " is not in regular market times");

    if (opens == EpochFrameOpenCloseType::Default && m_options.open_close_map.contains(market_time)) {
        opens = EpochFrameOpenCloseType::Null;
    }

    if (opens == EpochFrameOpenCloseType::Null) {
        m_options.open_close_map._del(market_time);
    } else if (opens != EpochFrameOpenCloseType::Default) {
        m_options.open_close_map._set(market_time, opens);
    }

    m_options.regular_market_times._set(market_time, times);

    if (!is_custom(market_time)) {
        m_customized_market_times.insert(market_time);
    }

    prepare_regular_market_times();
}

void MarketCalendar::change_time(EpochFrameMarketTimeType type, const std::vector<MarketTime>& times, EpochFrameOpenCloseType opens) {
    // Check if the market time type exists
    AssertWithTraceFromStream(m_options.regular_market_times.contains(type),
                             EpochFrameMarketTimeTypeWrapper::ToString(type) + " is not in regular market times");
    set_time(type, times, opens);
}

void MarketCalendar::add_time(EpochFrameMarketTimeType market_time, const std::vector<MarketTime>& times, EpochFrameOpenCloseType opens) {
    AssertWithTraceFromStream(!m_options.regular_market_times.contains(market_time),
                             EpochFrameMarketTimeTypeWrapper::ToString(market_time) + " is already in regular market times");
    set_time(market_time, times, opens);
}

void MarketCalendar::remove_time(EpochFrameMarketTimeType market_time) {
    m_options.regular_market_times._del(market_time);
    m_options.open_close_map._del(market_time);
    prepare_regular_market_times();
    if (is_custom(market_time)) {
        m_customized_market_times.erase(market_time);
    }
}

std::vector<MarketTime> MarketCalendar::get_time(EpochFrameMarketTimeType market_time, bool all_times) const {
    if (!m_options.regular_market_times.contains(market_time)) {
        if (market_time == EpochFrameMarketTimeType::BreakStart || market_time == EpochFrameMarketTimeType::BreakEnd) {
            return {};
        }
        else if (market_time == EpochFrameMarketTimeType::MarketOpen || market_time == EpochFrameMarketTimeType::MarketClose) {
            throw std::runtime_error("You need to set market_times");
        }
        else {
            throw std::runtime_error("Market time " + EpochFrameMarketTimeTypeWrapper::ToString(market_time) + " is not in regular market times");
        }
    }

    const auto times = m_options.regular_market_times[market_time];
    if (all_times) {
        return times;
    }
    auto time = times.back();
    time.time.tz = m_options.tz;
    return {time};
}

std::optional<MarketTime> MarketCalendar::get_time_on(EpochFrameMarketTimeType market_time, const Date& date) const {
    auto times = get_time(market_time, true);
    if (times.empty()) {
        return {};
    }
    auto iter = std::find_if(times.begin(), times.end()-1, [&](auto const& t) {
        return t.date < date;
    });
    if (iter == times.end()) {
        return {};
    }
    auto time = *iter;
    time.time.tz = m_options.tz;
    return time;
}

SpecialTimes MarketCalendar::get_special_times(EpochFrameMarketTimeType market_time) const {
    switch (market_time) {
        case EpochFrameMarketTimeType::MarketOpen:
            return m_options.special_opens;
        case EpochFrameMarketTimeType::MarketClose:
            return m_options.special_closes;
        default:
            return {};
    }
}

SpecialTimes MarketCalendar::get_special_times_adhoc(EpochFrameMarketTimeType market_time) const {
    switch (market_time) {
        case EpochFrameMarketTimeType::MarketOpen:
            return m_options.special_opens_adhoc;
        case EpochFrameMarketTimeType::MarketClose:
            return m_options.special_closes_adhoc;
        default:
            return {};
    }
}

Series MarketCalendar::convert(Series const& col) const {
    throw std::runtime_error("Not implemented");
}

DataFrame MarketCalendar::interruptions_df() const {
    throw std::runtime_error("Not implemented");
}

IndexPtr MarketCalendar::valid_days(const Date& start_date, const Date& end_date, std::string const& tz) const {
    return pd::date_range({.start = DateTime{start_date}, .end = DateTime{end_date}, .freq = m_holidays, .tz = tz});
}

std::vector<EpochFrameMarketTimeType> MarketCalendar::market_times(EpochFrameMarketTimeType start, EpochFrameMarketTimeType end) const noexcept {
    return m_market_times | ranges::views::filter([&](auto const& market_time) {
        return market_time >= start && market_time <= end;
    }) | ranges::to_vector;
}

IndexPtr MarketCalendar::days_at_time(IndexPtr const& days, Time market_time, int64_t day_offset) {
    throw std::runtime_error("Not implemented");
}

IndexPtr MarketCalendar::tryholidays(const AbstractHolidayCalendarPtr& cal, const Date& s, const Date& e) {
    throw std::runtime_error("Not implemented");
}

Series MarketCalendar::special_dates(std::vector<std::pair<Time, std::variant<IndexPtr, uint8_t>>> calendars,
        std::vector<std::pair<Time, IndexPtr>> ad_hoc_dates, const Date& start, const Date& end) {
    throw std::runtime_error("Not implemented");
}

Series MarketCalendar::special_dates(EpochFrameMarketTimeType market_time, const Date& start, const Date& end, bool filter_holidays) {
    throw std::runtime_error("Not implemented");
}

} // namespace epoch_frame::calendar
