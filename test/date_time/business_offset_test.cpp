//
// Created based on test_business_day.py and test_custom_business_day.py
//
#include "date_time/date_offsets.h"
#include <catch2/catch_test_macros.hpp>
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/index.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "common/asserts.h"
#include <iostream>
#include <date_time/holiday/holiday_calendar.h>

#include "epoch_frame/array.h"
#include "date_time/relative_delta_options.h"
#include "date_time/day_of_week.h"
#include "date_time/business/np_busdaycal.h"

using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory::scalar;
namespace efo = epoch_frame::factory::offset;
using namespace epoch_frame;
using namespace std::chrono;
using namespace std::literals::chrono_literals;

// Helper function to get a DateTime from an IIndex at a specific position
DateTime get_datetime_from_index(const std::shared_ptr<epoch_frame::IIndex>& index, int pos) {
    auto scalar = index->array().value()->GetScalar(pos).ValueOrDie();
    auto ts_scalar = std::static_pointer_cast<arrow::TimestampScalar>(scalar);
    REQUIRE(ts_scalar != nullptr);
    return to_datetime(*ts_scalar);
}

TEST_CASE("Test Business Day") {
    DateTime dt{2008y, January, 1d};
    DateTime dt2{2008y, January, 5d};

    SECTION("test with offset") {
        auto offset  = factory::offset::bday(1, TimeDelta{{.hours = 2}});
        REQUIRE(to_datetime(offset->add(dt.timestamp())) == DateTime{2008y, January, 2d, 2h});
    }

    SECTION("offset index") {
        auto offset  = factory::offset::bday(1, TimeDelta{{.hours = 2}});
        auto index = factory::index::date_range({
            .start = dt.timestamp(),
            .periods = 2,
            .offset = offset
        });
        REQUIRE(get_datetime_from_index(index, 0) == dt);
        REQUIRE(get_datetime_from_index(index, 1) == DateTime{2008y, January, 2d, 2h});
    }

    SECTION("add datetime") {
        auto offset  = factory::offset::bday(2);
        REQUIRE(to_datetime(offset->add(dt.timestamp())) == DateTime{2008y, January, 3d});
    }

    SECTION("rollback 1") {
        auto offset  = factory::offset::bday(10);
        REQUIRE(to_datetime(offset->rollback(dt.timestamp())) == dt);
    }

    SECTION("rollback 2") {
        auto offset  = factory::offset::bday(10);
        REQUIRE(to_datetime(offset->rollback(dt2.timestamp())) == DateTime{2008y, January, 4d});
    }

    SECTION("roll_forward 1") {
        auto offset  = factory::offset::bday(10);
        REQUIRE(to_datetime(offset->rollforward(dt.timestamp())) == dt);
    }

    SECTION("roll_forward 2") {
        auto offset  = factory::offset::bday(10);
        REQUIRE(to_datetime(offset->rollforward(dt2.timestamp())) == DateTime{2008y, January, 7d});
    }

    SECTION("test is_on_offset") {
        auto offset  = factory::offset::bday();
        REQUIRE(offset->is_on_offset(dt.timestamp()) == true);
        REQUIRE(offset->is_on_offset(dt2.timestamp()) == false);
    }

    SECTION("multiple cases") {
        struct TestCase {
            int64_t n;
            std::vector<std::pair<DateTime, DateTime>> input_expected;
        };
        std::vector<TestCase> cases{
            {1, {
                {DateTime{2008y, January, 1d}, DateTime{2008y, January, 2d}},
                {DateTime{2008y, January, 4d}, DateTime{2008y, January, 7d}},
                {DateTime{2008y, January, 5d}, DateTime{2008y, January, 7d}},
                {DateTime{2008y, January, 6d}, DateTime{2008y, January, 7d}},
                {DateTime{2008y, January, 7d}, DateTime{2008y, January, 8d}}
            }},
            {2, {
                {DateTime{2008y, January, 1d}, DateTime{2008y, January, 3d}},
                {DateTime{2008y, January, 4d}, DateTime{2008y, January, 8d}},
                {DateTime{2008y, January, 5d}, DateTime{2008y, January, 8d}},
                {DateTime{2008y, January, 6d}, DateTime{2008y, January, 8d}},
                {DateTime{2008y, January, 7d}, DateTime{2008y, January, 9d}}
            }},
            {-1, {
                {DateTime{2008y, January, 1d}, DateTime{2007y, December, 31d}},
                {DateTime{2008y, January, 4d}, DateTime{2008y, January, 3d}},
                {DateTime{2008y, January, 5d}, DateTime{2008y, January, 4d}},
                {DateTime{2008y, January, 6d}, DateTime{2008y, January, 4d}},
                {DateTime{2008y, January, 7d}, DateTime{2008y, January, 4d}},
                {DateTime{2008y, January, 8d}, DateTime{2008y, January, 7d}}
            }},
            {-2, {
                {DateTime{2008y, January, 1d}, DateTime{2007y, December, 28d}},
                {DateTime{2008y, January, 4d}, DateTime{2008y, January, 2d}},
                {DateTime{2008y, January, 5d}, DateTime{2008y, January, 3d}},
                {DateTime{2008y, January, 6d}, DateTime{2008y, January, 3d}},
                {DateTime{2008y, January, 7d}, DateTime{2008y, January, 3d}},
                {DateTime{2008y, January, 8d}, DateTime{2008y, January, 4d}},
                {DateTime{2008y, January, 9d}, DateTime{2008y, January, 7d}}
            }},
            {0, {
                {DateTime{2008y, January, 1d}, DateTime{2008y, January, 1d}},
                {DateTime{2008y, January, 4d}, DateTime{2008y, January, 4d}},
                {DateTime{2008y, January, 5d}, DateTime{2008y, January, 7d}},
                {DateTime{2008y, January, 6d}, DateTime{2008y, January, 7d}},
                {DateTime{2008y, January, 7d}, DateTime{2008y, January, 7d}}
            }}
        };

        for (auto const& [n, input_expected] : cases) {
            auto offset = factory::offset::bday(n);
            for (auto const& [input, expected] : input_expected) {
                REQUIRE(to_datetime(offset->add(input.timestamp())) == expected);
            }
        }
    }

    SECTION("test apply large n") {
        // Test applying offset with large n

        // 10 business days from Oct 23, 2012 should be Nov 6, 2012
        dt = DateTime{2012y, October, 23d};
        auto offset = efo::bday(10);
        auto result = to_datetime(offset->add(dt.timestamp()));
        auto expected = DateTime{2012y, November, 6d};
        REQUIRE(result == expected);

        // Add 100 then subtract 100 business days should return to original date
        auto offset100 = efo::bday(100);
        auto finalResult = offset100->rsub(offset100->add(dt.timestamp()));
        REQUIRE(to_datetime(finalResult) == dt);

        // Test business day offset with multiplication factor
        auto offset6 = efo::bday(6);

        // Test backward application (subtract)
        auto start1 = DateTime{2012y, January, 1d};
        auto result1 = to_datetime(offset6->rsub(start1.timestamp()));
        auto expected1 = DateTime{2011y, December, 23d};
        REQUIRE(result1 == expected1);

        // Test forward application
        auto start2 = DateTime{2011y, December, 18d};
        auto result2 = to_datetime(offset6->add(start2.timestamp()));
        auto expected2 = DateTime{2011y, December, 26d};
        REQUIRE(result2 == expected2);

        // Test from issue #5890
        auto start3 = DateTime{2014y, January, 5d};
        auto result3 = to_datetime(offset->add(start3.timestamp()));
        auto expected3 = DateTime{2014y, January, 17d};
        REQUIRE(result3 == expected3);
    }
}

TEST_CASE("Custom Business Day") {
    using namespace epoch_frame::factory::offset;
    SECTION("holidays") {
        np::HolidayList holidays{
            "2012-05-01"__date,
            "2013-05-01"__date,
            "2014-05-01"__date,
        };

        auto tday = cbday({.holidays = holidays});
        for (auto year = 2012; year < 2015; year++) {
            auto dt = DateTime{std::chrono::year(year), April, 30d};
            auto xp = DateTime{std::chrono::year(year), May, 2d};

            auto rs = to_datetime(tday->add(dt.timestamp()));
            REQUIRE(rs == xp);
        }
    }

    SECTION("weekmask") {
        auto weekmask_saudi = np::to_weekmask({
        epoch_core::EpochDayOfWeek::Saturday, epoch_core::EpochDayOfWeek::Sunday, epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday, epoch_core::EpochDayOfWeek::Wednesday});
        auto weekmask_uae = np::WeekMask{true, true, true, true, false, false, true};

        auto bday_saudi = cbday({.weekmask = weekmask_saudi});
        auto bday_uae = cbday({.weekmask = weekmask_uae});

        auto dt = "2013-05-01"__date;
        auto xp_saudi = "2013-05-04"__date;
        auto xp_uae = "2013-05-02"__date;

        REQUIRE(to_datetime(bday_saudi->add(dt.timestamp())) == xp_saudi);
        REQUIRE(to_datetime(bday_uae->add(dt.timestamp())) == xp_uae);

        const auto xp2 = "2013-05-05"__date;
        REQUIRE(to_datetime(bday_saudi->make(2)->add(dt.timestamp())) == xp2);
        REQUIRE(to_datetime(bday_uae->make(2)->add(dt.timestamp())) == xp2);
    }

    SECTION("weekmask and holidays") {
        np::HolidayList holidays{
            "2012-05-01"__date,
            "2013-05-01"__date,
            "2014-05-01"__date,
        };

        auto weekmask_egypt = np::to_weekmask({epoch_core::EpochDayOfWeek::Sunday, epoch_core::EpochDayOfWeek::Monday, epoch_core::EpochDayOfWeek::Tuesday, epoch_core::EpochDayOfWeek::Wednesday, epoch_core::EpochDayOfWeek::Thursday});
        auto bday_egypt = cbday({.weekmask = weekmask_egypt, .holidays = holidays });
        auto dt = "2013-04-30"__date;
        auto xp_egypt = "2013-05-05"__date;
        REQUIRE(to_datetime(bday_egypt->make(2)->add(dt.timestamp())) == xp_egypt);
    }

    SECTION("federal holiday") {
        auto calendar = calendar::getHolidayCalendar("USFederalHolidayCalendar");
        auto dt = "2014-01-17"__date;
        REQUIRE(to_datetime(cbday({.calendar = calendar})->add(dt.timestamp())) == DateTime{2014y, January, 21d});
    }
}
