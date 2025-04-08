//
// Created by adesola on 4/8/25.
//
#include "calendar/calendar_factory.h"
#include "date_time/time_delta.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/scalar.h"
#include <catch.hpp>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("Crypto Calendar Test")
{
    auto calendar = calendar::CalendarFactory::instance().get_calendar("Crypto");
    auto schedule = calendar->schedule("2012-07-01"__date.date, "2012-07-10"__date.date, {});

    auto expected_index = factory::index::date_range(
        {.start = "2012-07-01"_date, .end = "2012-07-10"_date, .offset = offset::days(1)});

    auto dt = arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
    auto market_open  = expected_index->array().cast(dt).as_chunked_array();
    auto market_close = expected_index->array().cast(dt) + Scalar{TimeDelta{{.days = 1}}};

    DataFrame expected_df =
        make_dataframe(expected_index, {market_open, market_close.as_chunked_array()},
                       {"MarketOpen", "MarketClose"});

    INFO(schedule);
    REQUIRE(schedule.equals(expected_df));
}
