#include "holiday.h"
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include "index/datetime_index.h"
#include <arrow/scalar.h>
#include <arrow/type_fwd.h>
#include <iostream>

namespace epoch_frame::calendar
{

    Holiday::Holiday(const HolidayData& data)
        : m_data(data), m_days_of_week_array(get_days_of_week_array())
    {
        if (!m_data.offset.empty())
        {
            if (m_data.observance)
            {
                throw std::invalid_argument("Cannot use both offset and observance.");
            }
        }
    }

    IndexPtr Holiday::dates(arrow::TimestampScalar const& start_date,
                            arrow::TimestampScalar const& end_date) const
    {
        Scalar filter_start_date{start_date};
        Scalar filter_end_date{end_date};

        if (m_data.year)
        {
            AssertFromFormat(m_data.month, "Month must be specified for yearly holidays");
            AssertFromFormat(m_data.day, "Day must be specified for yearly holidays");
            auto dt  = factory::scalar::from_ymd(m_data.year.value() / *m_data.month / *m_data.day);
            auto dt1 = factory::array::make_timestamp_array({dt});
            return std::make_shared<DateTimeIndex>(dt1);
        }

        auto     dates         = reference_dates(filter_start_date, filter_end_date);
        IndexPtr holiday_dates = apply_rule(dates);

        if (!m_data.days_of_week.empty())
        {
            auto filter = holiday_dates->day_of_week(arrow::compute::DayOfWeekOptions{})
                              .is_in(m_days_of_week_array);
            holiday_dates = holiday_dates->filter(filter, false);
        }

        if (m_data.start_date)
        {
            filter_start_date = std::max(
                Scalar(m_data.start_date->tz_localize(filter_start_date.dt().tz()).timestamp()),
                filter_start_date);
        }

        if (m_data.end_date)
        {
            filter_end_date = std::min(
                Scalar(m_data.end_date->tz_localize(filter_end_date.dt().tz()).timestamp()),
                filter_end_date);
        }

        return holiday_dates->filter((holiday_dates->array() >= filter_start_date) &&
                                     (holiday_dates->array() <= filter_end_date));
    }

    Series Holiday::dates_with_name(arrow::TimestampScalar const& start_date,
                                    arrow::TimestampScalar const& end_date) const
    {
        return Series(arrow::MakeScalar(m_data.name), dates(start_date, end_date));
    }

    IndexPtr Holiday::reference_dates(Scalar start_date, Scalar end_date) const
    {
        auto tz = start_date.dt().tz();
        if (m_data.start_date)
        {
            start_date = Scalar{m_data.start_date->tz_localize(tz).timestamp()};
        }

        if (m_data.end_date)
        {
            end_date = Scalar{m_data.end_date->tz_localize(tz).timestamp()};
        }

        auto start_date_dt = start_date.dt();
        auto end_date_dt   = end_date.dt();
        tz                 = start_date_dt.tz();

        auto year_offset          = date_offset({.years = 1});
        auto reference_start_date = factory::scalar::from_ymd(
            chrono_year(start_date_dt.year().value<int64_t>().value() - 1) / m_data.month.value_or(January) /
            m_data.day.value_or(1d));
        auto reference_end_date =
            factory::scalar::from_ymd(chrono_year(end_date_dt.year().value<int64_t>().value() + 1) /
                                      m_data.month.value_or(December) / m_data.day.value_or(31d));

        return factory::index::date_range({.start  = reference_start_date,
                                           .end    = reference_end_date,
                                           .offset = year_offset,
                                           .tz     = tz});
    }

    IndexPtr Holiday::apply_rule(IndexPtr const& dates) const
    {
        if (dates->empty())
        {
            return dates;
        }

        if (m_data.observance)
        {
            auto filter = dates->array().map(
                [this](Scalar const& date)
                {
                    auto dt         = date.to_datetime();
                    auto observance = m_data.observance(dt);
                    if (observance)
                    {
                        return Scalar{observance->timestamp()};
                    }
                    return Scalar{
                        arrow::MakeNullScalar(arrow::timestamp(arrow::TimeUnit::NANO, dt.tz))};
                });

            filter = filter[filter.is_not_null()];
            return dates->Make(filter.value());
        }

        auto dates_array = dates->array();
        for (auto const& offset : m_data.offset)
        {
            dates_array = offset->add_array(dates_array);
        }

        return dates->Make(dates_array.value());
    }

    Array Holiday::get_days_of_week_array() const
    {
        std::vector<int64_t> values(m_data.days_of_week.size());
        std::ranges::transform(m_data.days_of_week, values.begin(),
                               [](auto dayofweek) { return static_cast<int64_t>(dayofweek); });
        return Array(factory::array::make_contiguous_array(values));
    }
} // namespace epoch_frame::calendar
