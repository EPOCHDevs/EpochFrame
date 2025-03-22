//
// Created based on test_business_day.py and test_custom_business_day.py
//
#include "calendar/date_offsets.h"
#include <catch2/catch_test_macros.hpp>
#include "factory/date_offset_factory.h"
#include "factory/index_factory.h"
#include "index/index.h"
#include "factory/scalar_factory.h"
#include "common/asserts.h"
#include <iostream>
#include "epochframe/array.h"
#include "calendar/relative_delta_options.h"
#include "calendar/day_of_week.h"
#include "calendar/business_calendar/np_busdaycal.h"

using namespace epochframe::factory::index;
using namespace epochframe::factory::scalar;
namespace efo = epochframe::factory::offset;
using namespace epochframe;
using namespace std::chrono;
using namespace std::literals::chrono_literals;

// Helper function to get a DateTime from an IIndex at a specific position
DateTime get_datetime_from_index(const std::shared_ptr<epochframe::IIndex>& index, int pos) {
    auto scalar = index->array().value()->GetScalar(pos).ValueOrDie();
    auto ts_scalar = std::static_pointer_cast<arrow::TimestampScalar>(scalar);
    REQUIRE(ts_scalar != nullptr);
    return to_datetime(*ts_scalar);
}

// Common test dates
namespace {
    // Test dates for January 2008
    const DateTime DATE_2008_01_01 = DateTime{{2008y, January, 1d}};  // Tuesday
    const DateTime DATE_2008_01_02 = DateTime{{2008y, January, 2d}};  // Wednesday
    const DateTime DATE_2008_01_03 = DateTime{{2008y, January, 3d}};  // Thursday
    const DateTime DATE_2008_01_04 = DateTime{{2008y, January, 4d}};  // Friday
    const DateTime DATE_2008_01_05 = DateTime{{2008y, January, 5d}};  // Saturday
    const DateTime DATE_2008_01_06 = DateTime{{2008y, January, 6d}};  // Sunday
    const DateTime DATE_2008_01_07 = DateTime{{2008y, January, 7d}};  // Monday
    const DateTime DATE_2008_01_08 = DateTime{{2008y, January, 8d}};  // Tuesday
    const DateTime DATE_2008_01_09 = DateTime{{2008y, January, 9d}};  // Wednesday
    const DateTime DATE_2008_01_10 = DateTime{{2008y, January, 10d}};  // Thursday
    const DateTime DATE_2008_01_11 = DateTime{{2008y, January, 11d}};  // Friday
    
    // Date with time component
    const DateTime DATETIME_2008_01_01_02H = DateTime{{2008y, January, 1d}, 2h, 0min, 0s, 0us};
    
    // December 2007 dates for negative offsets
    const DateTime DATE_2007_12_28 = DateTime{{2007y, December, 28d}};  // Friday
    const DateTime DATE_2007_12_31 = DateTime{{2007y, December, 31d}};  // Monday
    
    // Other test dates
    const DateTime DATE_2008_01_14 = DateTime{{2008y, January, 14d}};
    const DateTime DATE_2008_01_15 = DateTime{{2008y, January, 15d}};
    const DateTime DATE_2008_01_18 = DateTime{{2008y, January, 18d}};
    const DateTime DATE_2008_01_21 = DateTime{{2008y, January, 21d}};
    const DateTime DATE_2008_01_22 = DateTime{{2008y, January, 22d}};
    const DateTime DATE_2008_02_18 = DateTime{{2008y, February, 18d}}; // Presidents Day
    
    // May 2013 test dates (for custom business day tests)
    const DateTime DATE_2013_04_30 = DateTime{{2013y, April, 30d}};
    const DateTime DATE_2013_05_01 = DateTime{{2013y, May, 1d}};
    const DateTime DATE_2013_05_02 = DateTime{{2013y, May, 2d}};
    const DateTime DATE_2013_05_04 = DateTime{{2013y, May, 4d}};
    const DateTime DATE_2013_05_05 = DateTime{{2013y, May, 5d}};
    const DateTime DATE_2013_05_06 = DateTime{{2013y, May, 6d}};
    const DateTime DATE_2013_05_07 = DateTime{{2013y, May, 7d}};
    const DateTime DATE_2013_05_08 = DateTime{{2013y, May, 8d}};
    const DateTime DATE_2013_05_20 = DateTime{{2013y, May, 20d}};

    // After the other date constants, add the missing date constants for 2013
    const DateTime DATE_2012_12_31 = DateTime{{2012y, December, 31d}};
    const DateTime DATE_2013_01_01 = DateTime{{2013y, January, 1d}};
    const DateTime DATE_2013_01_02 = DateTime{{2013y, January, 2d}};
    const DateTime DATE_2013_01_21 = DateTime{{2013y, January, 21d}};
    const DateTime DATE_2013_08_07 = DateTime{{2013y, August, 7d}};
    const DateTime DATE_2013_08_08 = DateTime{{2013y, August, 8d}};
    const DateTime DATE_2013_08_11 = DateTime{{2013y, August, 11d}};

    // Add missing 2008 holiday dates
    const DateTime DATE_2008_05_26 = DateTime{{2008y, May, 26d}};
    const DateTime DATE_2008_07_04 = DateTime{{2008y, July, 4d}};
    const DateTime DATE_2008_09_01 = DateTime{{2008y, September, 1d}};
    const DateTime DATE_2008_10_13 = DateTime{{2008y, October, 13d}};
    const DateTime DATE_2008_11_11 = DateTime{{2008y, November, 11d}};
    const DateTime DATE_2008_11_27 = DateTime{{2008y, November, 27d}};
    const DateTime DATE_2008_12_25 = DateTime{{2008y, December, 25d}};
}

// Helper function to test if a date is a business day
bool is_business_day(const DateOffsetHandlerPtr& offset, const DateTime& date) {
    return offset->is_on_offset(date.timestamp());
}

// Helper function to advance a date by an offset
DateTime advance_by_offset(const DateOffsetHandlerPtr& offset, const DateTime& date) {
    return to_datetime(offset->add(date.timestamp()));
}

// Add helper function to convert WeekSet to WeekMask
np::WeekMask to_weekmask(const np::WeekSet& weekset) {
    np::WeekMask mask{false, false, false, false, false, false, false};
    for (auto day : weekset) {
        mask[static_cast<size_t>(day)] = true;
    }
    return mask;
}

TEST_CASE("BusinessDay - Basic Functionality", "[business_day]") {
    // Create default weekmask (Mon-Fri) with no holidays
    np::WeekSet weekmask = {
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
        EpochDayOfWeek::Friday
    }; // Excludes Saturday and Sunday
    
    std::vector<DateTime> holidays;
    
    SECTION("Constructor and Basic Properties") {
        // Use factory function instead of direct construction
        auto offset = efo::bday(1);
        
        REQUIRE(offset->code() == "B");
        REQUIRE(offset->name() == "BusinessDay");
        REQUIRE(offset->is_fixed() == false);
        REQUIRE(offset->n() == 1);
    }
    
    SECTION("Equality and Copy") {
        auto offset1 = efo::bday(1);
        auto offset2 = efo::bday(1);
        auto offset3 = efo::bday(2);
        
        // Test equality by comparing properties
        REQUIRE(offset1->code() == offset2->code());
        REQUIRE(offset1->n() == offset2->n());
        REQUIRE(offset1->n() != offset3->n());
        
        // Test make method
        auto offset4 = offset1->make(2);
        REQUIRE(offset4->n() == 2);
    }
    
    SECTION("Simple Date Addition") {
        auto offset = efo::bday(1);
        
        // Tuesday Jan 1, 2008 -> Wednesday Jan 2, 2008
        auto result = offset->add(DATE_2008_01_01.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_02);
        
        // Friday Jan 4, 2008 -> Monday Jan 7, 2008 (skips weekend)
        result = offset->add(DATE_2008_01_04.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_07);
    }
    
    SECTION("Multiple Day Addition") {
        auto offset = efo::bday(2);
        
        // Tuesday Jan 1, 2008 -> Thursday Jan 3, 2008 (2 business days)
        auto result = offset->add(DATE_2008_01_01.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_03);
        
        // Friday Jan 4, 2008 -> Tuesday Jan 8, 2008 (skips weekend)
        result = offset->add(DATE_2008_01_04.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_08);
    }
    
    SECTION("Negative Offset") {
        auto neg_offset = efo::bday(-1);
        
        // Tuesday Jan 1, 2008 -> Monday Dec 31, 2007 (1 business day back)
        auto result = neg_offset->add(DATE_2008_01_01.timestamp());
        REQUIRE(to_datetime(result) == DATE_2007_12_31);
        
        // Monday Jan 7, 2008 -> Friday Jan 4, 2008 (skips weekend)
        result = neg_offset->add(DATE_2008_01_07.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_04);
    }
    
    SECTION("Zero Offset") {
        auto zero_offset = efo::bday(0);
        
        // Tuesday Jan 1, 2008 stays the same
        auto result = zero_offset->add(DATE_2008_01_01.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_01);
        
        // Saturday Jan 5, 2008 rolls forward to Monday
        result = zero_offset->add(DATE_2008_01_05.timestamp()); // Saturday
        REQUIRE(to_datetime(result) == DATE_2008_01_07); // Monday
    }
    
    SECTION("With Time Component") {
        auto offset = efo::bday(1);
        
        // Tuesday Jan 1, 2008 2:00 -> Wednesday Jan 2, 2008 2:00
        auto result = offset->add(DATETIME_2008_01_01_02H.timestamp());
        REQUIRE(to_datetime(result).hour == 2h);
        REQUIRE(to_datetime(result).date == DATE_2008_01_02.date);
    }
    
    SECTION("Business Day Identification") {
        auto offset = efo::bday(1);
        
        // Tuesday Jan 1, 2008 is a business day
        REQUIRE(offset->is_on_offset(DATE_2008_01_01.timestamp()) == true);
        
        // Saturday Jan 5, 2008 is not a business day
        REQUIRE(offset->is_on_offset(DATE_2008_01_05.timestamp()) == false);
    }
    
    SECTION("Roll Forward") {
        auto offset = efo::bday(1);
        
        // Saturday Jan 5, 2008 rolls forward to Monday Jan 7, 2008
        auto result = offset->rollforward(DATE_2008_01_05.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_07);
    }
    
    SECTION("Roll Back") {
        auto offset = efo::bday(1);
        
        // Saturday Jan 5, 2008 rolls back to Friday Jan 4, 2008
        auto result = offset->rollback(DATE_2008_01_05.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_04);
    }
    
    SECTION("Apply Large n") {
        auto offset10 = efo::bday(10);
        
        // Tuesday Jan 1, 2008 + 10 business days -> Tuesday Jan 15, 2008
        auto result = offset10->add(DATE_2008_01_01.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_15);
    }
    
    SECTION("Date Difference") {
        auto offset = efo::bday(1);
        
        // Tuesday Jan 1, 2008 to Monday Jan 7, 2008 = 4 business days
        REQUIRE(offset->diff(DATE_2008_01_01.timestamp(), DATE_2008_01_07.timestamp()) == 4);
        
        // Going backward should give negative number
        REQUIRE(offset->diff(DATE_2008_01_07.timestamp(), DATE_2008_01_01.timestamp()) == 0);
    }
    
    SECTION("With TimeDelta") {
        // Test adding business day with time component
        auto offset = efo::bday(1);
        
        // Create a datetime with 2-hour time component
        auto dt_with_hour = DATETIME_2008_01_01_02H; // Jan 1, 2:00 AM
        
        // Add business day to datetime with time component
        auto result = offset->add(dt_with_hour.timestamp());
        auto result_dt = to_datetime(result);
        
        // Should maintain the time component
        REQUIRE(result_dt.date.day == 2d); // Jan 2
        REQUIRE(result_dt.hour == 2h);     // Original 2:00 AM
        
        // Test another approach - adding time component to the result
        result = offset->add(DATE_2008_01_01.timestamp());
        auto with_time = to_datetime(result);
        with_time.hour = 2h;
        
        // Should be Jan 2, 2:00 AM
        REQUIRE(with_time.date.day == 2d); // Jan 2
        REQUIRE(with_time.hour == 2h);     // 2:00 AM
    }
    
    SECTION("Date Range with BusinessDay") {
        // Test date range with a business day offset
        using namespace epochframe::factory::index;
        
        auto start = DATE_2008_01_01;
        int periods = 10;
        
        auto offset = efo::bday(1);
        
        // Create DateRangeOptions with all required fields
        epochframe::factory::index::DateRangeOptions options{
            start.timestamp(),                  // start
            std::nullopt,                       // end
            periods,                            // periods
            offset,                             // offset
            "",                                 // tz
            AmbiguousTimeHandling::RAISE,       // ambiguous
            NonexistentTimeHandling::RAISE      // nonexistent
        };
        
        auto index = date_range(options);
        REQUIRE(index->size() == periods);
        
        // Expected dates (skipping weekends)
        std::vector<DateTime> expected_dates = {
            DATE_2008_01_01,  // Tuesday
            DATE_2008_01_02,  // Wednesday
            DATE_2008_01_03,  // Thursday
            DATE_2008_01_04,  // Friday
            DATE_2008_01_07,  // Monday (skip weekend)
            DATE_2008_01_08,  // Tuesday
            DATE_2008_01_09,  // Wednesday
            DATE_2008_01_10,  // Thursday
            DATE_2008_01_11,  // Friday
            DATE_2008_01_14   // Monday (skip weekend)
        };
        
        for (size_t i = 0; i < expected_dates.size(); ++i) {
            REQUIRE(get_datetime_from_index(index, i) == expected_dates[i]);
        }
    }
}

TEST_CASE("BusinessDay - Advanced Use Cases", "[business_day]") {
    // Create default weekmask (Mon-Fri) with no holidays
    np::WeekSet weekmask = {
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
        EpochDayOfWeek::Friday
    }; // Excludes Saturday and Sunday
    
    std::vector<DateTime> holidays;
    
    SECTION("Apply Offset Cases") {
        // Test different combinations of offsets and dates
        // These cases are from the Python test_apply method
        
        struct TestCase {
            int n;
            DateTime base_date;
            DateTime expected_date;
        };
        
        // 1 day offset cases
        std::vector<TestCase> offset1_cases = {
            {1, DATE_2008_01_01, DATE_2008_01_02}, // Tue -> Wed
            {1, DATE_2008_01_04, DATE_2008_01_07}, // Fri -> Mon (skip weekend)
            {1, DATE_2008_01_05, DATE_2008_01_07}, // Sat -> Mon (non-business day start)
            {1, DATE_2008_01_06, DATE_2008_01_07}, // Sun -> Mon (non-business day start)
            {1, DATE_2008_01_07, DATE_2008_01_08}  // Mon -> Tue
        };
        
        auto offset1 = efo::bday(1);
        for (const auto& tc : offset1_cases) {
            auto result = offset1->add(tc.base_date.timestamp());
            REQUIRE(to_datetime(result) == tc.expected_date);
        }
        
        // 2 day offset cases
        std::vector<TestCase> offset2_cases = {
            {2, DATE_2008_01_01, DATE_2008_01_03}, // Tue -> Thu
            {2, DATE_2008_01_04, DATE_2008_01_08}, // Fri -> Tue
            {2, DATE_2008_01_05, DATE_2008_01_08}, // Sat -> Tue
            {2, DATE_2008_01_06, DATE_2008_01_08}, // Sun -> Tue
            {2, DATE_2008_01_07, DATE_2008_01_09}  // Mon -> Wed
        };
        
        auto offset2 = efo::bday(2);
        for (const auto& tc : offset2_cases) {
            auto result = offset2->add(tc.base_date.timestamp());
            REQUIRE(to_datetime(result) == tc.expected_date);
        }
        
        // -1 day offset cases
        std::vector<TestCase> neg_offset1_cases = {
            {-1, DATE_2008_01_01, DATE_2007_12_31}, // Tue -> Mon (Dec 31)
            {-1, DATE_2008_01_04, DATE_2008_01_03}, // Fri -> Thu
            {-1, DATE_2008_01_05, DATE_2008_01_04}, // Sat -> Fri
            {-1, DATE_2008_01_06, DATE_2008_01_04}, // Sun -> Fri
            {-1, DATE_2008_01_07, DATE_2008_01_04}, // Mon -> Fri
            {-1, DATE_2008_01_08, DATE_2008_01_07}  // Tue -> Mon
        };
        
        auto neg_offset1 = efo::bday(-1);
        for (const auto& tc : neg_offset1_cases) {
            auto result = neg_offset1->add(tc.base_date.timestamp());
            REQUIRE(to_datetime(result) == tc.expected_date);
        }
        
        // -2 day offset cases
        std::vector<TestCase> neg_offset2_cases = {
            {-2, DATE_2008_01_01, DATE_2007_12_28}, // Tue -> Fri (Dec 28)
            {-2, DATE_2008_01_04, DATE_2008_01_02}, // Fri -> Wed
            {-2, DATE_2008_01_05, DATE_2008_01_03}, // Sat -> Thu
            {-2, DATE_2008_01_06, DATE_2008_01_03}, // Sun -> Thu
            {-2, DATE_2008_01_07, DATE_2008_01_03}, // Mon -> Thu
            {-2, DATE_2008_01_08, DATE_2008_01_04}, // Tue -> Fri
            {-2, DATE_2008_01_09, DATE_2008_01_07}  // Wed -> Mon
        };
        
        auto neg_offset2 = efo::bday(-2);
        for (const auto& tc : neg_offset2_cases) {
            auto result = neg_offset2->add(tc.base_date.timestamp());
            REQUIRE(to_datetime(result) == tc.expected_date);
        }
        
        // 0 offset cases - rolls non-business days
        std::vector<TestCase> offset0_cases = {
            {0, DATE_2008_01_01, DATE_2008_01_01}, // Tue stays Tue
            {0, DATE_2008_01_04, DATE_2008_01_04}, // Fri stays Fri
            {0, DATE_2008_01_05, DATE_2008_01_07}, // Sat rolls to Mon
            {0, DATE_2008_01_06, DATE_2008_01_07}, // Sun rolls to Mon
            {0, DATE_2008_01_07, DATE_2008_01_07}  // Mon stays Mon
        };
        
        auto offset0 = efo::bday(0);
        for (const auto& tc : offset0_cases) {
            auto result = offset0->add(tc.base_date.timestamp());
            REQUIRE(to_datetime(result) == tc.expected_date);
        }
    }
    
    SECTION("With Holidays") {
        // Define a set of holidays
        std::vector<DateTime> custom_holidays = {
            DATE_2008_01_02,  // Wednesday
            DATE_2008_01_21,  // Monday (MLK Day)
            DATE_2008_02_18   // Monday (Presidents Day)
        };
        
        // For BusinessDay with custom holidays, use efo::cbday with default weekmask
        efo::CustomBusinessDayParams holiday_params;
        holiday_params.n = 1;
        holiday_params.holidays = custom_holidays;
        auto offset = efo::cbday(holiday_params, np::DEFAULT_BUSDAYCAL);
        
        // Tuesday Jan 1, 2008 + 1 -> Thursday Jan 3, 2008 (skips Jan 2 holiday)
        auto result = offset->add(DATE_2008_01_01.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_03);
        
        // Jan 18, 2008 (Friday) + 1 -> Jan 22, 2008 (Tuesday) (skips MLK Day)
        result = offset->add(DATE_2008_01_18.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_22);
    }
}

TEST_CASE("CustomBusinessDay - Basic Functionality", "[custom_business_day]") {
    // Define a weekmask for Saudi Arabia (Sat-Wed work week, Thu-Fri weekend)
    np::WeekSet saudi_weekmask = {
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Saturday,
        EpochDayOfWeek::Sunday
    };
    
    // Define a weekmask for UAE (Sun-Thu work week, Fri-Sat weekend)
    np::WeekSet uae_weekmask = {
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
        EpochDayOfWeek::Sunday
    };
    
    // Define Egyptian work week (Sun-Thu, Fri-Sat weekend)
    np::WeekSet egypt_weekmask = {
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Thursday,
        EpochDayOfWeek::Sunday
    };
    
    std::vector<DateTime> holidays;
    
    SECTION("Constructor and Basic Properties") {
        // Use CustomBusinessDay instead of BusinessDay with custom weekmask
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.weekmask = to_weekmask(saudi_weekmask);
        params.holidays = holidays;
        auto offset_saudi = efo::cbday(params);
        
        REQUIRE(offset_saudi->code() == "C");
        REQUIRE(offset_saudi->name() == "CustomBusinessDay");
        REQUIRE(offset_saudi->is_fixed() == false);
        REQUIRE(offset_saudi->n() == 1);
    }
    
    SECTION("Different Weekmasks") {
        // Create params for each country
        efo::CustomBusinessDayParams saudi_params;
        saudi_params.n = 1;
        saudi_params.weekmask = to_weekmask(saudi_weekmask);
        saudi_params.holidays = holidays;
        auto offset_saudi = efo::cbday(saudi_params);
        
        efo::CustomBusinessDayParams uae_params;
        uae_params.n = 1;
        uae_params.weekmask = to_weekmask(uae_weekmask);
        uae_params.holidays = holidays;
        auto offset_uae = efo::cbday(uae_params);
        
        efo::CustomBusinessDayParams egypt_params;
        egypt_params.n = 1;
        egypt_params.weekmask = to_weekmask(egypt_weekmask);
        egypt_params.holidays = holidays;
        auto offset_egypt = efo::cbday(egypt_params);
        
        // Test for May 1, 2013 (Wednesday)
        auto may1 = DATE_2013_05_01;
        
        // Saudi: Wed is workday, Thu-Fri weekend -> next is Sat
        auto result_saudi = offset_saudi->add(may1.timestamp());
        REQUIRE(to_datetime(result_saudi) == DATE_2013_05_04); // May 4 (Saturday)
        
        // UAE: Wed is workday, Fri-Sat weekend -> next is Thu
        auto result_uae = offset_uae->add(may1.timestamp());
        REQUIRE(to_datetime(result_uae) == DATE_2013_05_02); // May 2 (Thursday)
        
        // Egypt: Wed is workday, Fri-Sat weekend -> next is Thu
        auto result_egypt = offset_egypt->add(may1.timestamp());
        REQUIRE(to_datetime(result_egypt) == DATE_2013_05_02); // May 2 (Thursday) 
    }
    
    SECTION("Multiple Day Offsets") {
        // Create params for each country
        efo::CustomBusinessDayParams saudi_params;
        saudi_params.n = 2;
        saudi_params.weekmask = to_weekmask(saudi_weekmask);
        saudi_params.holidays = holidays;
        auto offset_saudi = efo::cbday(saudi_params);
        
        efo::CustomBusinessDayParams uae_params;
        uae_params.n = 2;
        uae_params.weekmask = to_weekmask(uae_weekmask);
        uae_params.holidays = holidays;
        auto offset_uae = efo::cbday(uae_params);
        
        efo::CustomBusinessDayParams egypt_params;
        egypt_params.n = 2;
        egypt_params.weekmask = to_weekmask(egypt_weekmask);
        egypt_params.holidays = holidays;
        auto offset_egypt = efo::cbday(egypt_params);
        
        // Test for May 1, 2013 (Wednesday)
        auto may1 = DATE_2013_05_01;
        
        // For all calendars, +2 business days from Wed should be Sun
        auto expected = DATE_2013_05_05; // May 5 (Sunday)
        
        auto result_saudi = offset_saudi->add(may1.timestamp());
        REQUIRE(to_datetime(result_saudi) == expected);
        
        auto result_uae = offset_uae->add(may1.timestamp());
        REQUIRE(to_datetime(result_uae) == expected);
        
        auto result_egypt = offset_egypt->add(may1.timestamp());
        REQUIRE(to_datetime(result_egypt) == expected);
    }
    
    SECTION("Custom Holidays") {
        // Define a set of holidays
        std::vector<DateTime> custom_holidays = {
            DATE_2013_05_01,    // Labor Day
            DATE_2013_05_20     // Another holiday
        };
        
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.weekmask = to_weekmask(egypt_weekmask);
        params.holidays = custom_holidays;
        auto offset = efo::cbday(params);
        
        // April 30, 2013 + 1 business day -> May 2 (skips May 1 holiday)
        auto apr30 = DATE_2013_04_30;
        auto result = offset->add(apr30.timestamp());
        REQUIRE(to_datetime(result) == DATE_2013_05_02);
    }
    
    SECTION("Weekmask and Holidays Combined") {
        // Define Egypt weekend (Fri-Sat) and holidays
        std::vector<DateTime> custom_holidays = {
            DATE_2013_05_01     // Labor Day
        };
        
        efo::CustomBusinessDayParams params;
        params.n = 2;
        params.weekmask = to_weekmask(egypt_weekmask);
        params.holidays = custom_holidays;
        auto offset = efo::cbday(params);
        
        // April 30, 2013 + 2 business days -> May 5 (skips May 1 holiday and May 3-4 weekend)
        auto apr30 = DATE_2013_04_30;
        auto result = offset->add(apr30.timestamp());
        REQUIRE(to_datetime(result) == DATE_2013_05_05);
    }
    
    SECTION("Binary String Weekmask") {
        // Testing with a binary string weekmask representation
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.weekmask = to_weekmask(uae_weekmask);
        params.holidays = holidays;
        auto offset_uae = efo::cbday(params);
        
        // Test for May 1, 2013 (Wednesday)
        auto may1 = DATE_2013_05_01;
        auto result = offset_uae->add(may1.timestamp());
        REQUIRE(to_datetime(result) == DATE_2013_05_02); // May 2 (Thursday)
    }
}

TEST_CASE("CustomBusinessDay - Comprehensive Tests", "[custom_business_day]") {
    // Define a weekmask for Saudi Arabia (Sat-Wed work week, Thu-Fri weekend)
    np::WeekSet saudi_weekmask = {
        EpochDayOfWeek::Monday,
        EpochDayOfWeek::Tuesday,
        EpochDayOfWeek::Wednesday,
        EpochDayOfWeek::Saturday,
        EpochDayOfWeek::Sunday
    };
    
    std::vector<DateTime> holidays;
    
    SECTION("DateRange with CustomBusinessDay") {
        // Custom business day calendar for Saudi Arabia
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.weekmask = to_weekmask(saudi_weekmask);
        params.holidays = holidays;
        auto offset_saudi = efo::cbday(params);
        
        // Test date range generation
        auto start = DATE_2013_05_01;  // Wednesday
        uint32_t periods = 5;
        
        // Create DateRangeOptions with all required fields
        epochframe::factory::index::DateRangeOptions options{
            start.timestamp(),                  // start
            std::nullopt,                       // end
            periods,                            // periods
            offset_saudi,                       // offset
            "",                                 // tz
            AmbiguousTimeHandling::RAISE,       // ambiguous
            NonexistentTimeHandling::RAISE      // nonexistent
        };
        
        auto range = date_range(options);
        REQUIRE(range->size() == periods);
        
        // Expected dates: May 1 (Wed), 4 (Sat), 5 (Sun), 6 (Mon), 7 (Tue)
        // Skipping the Saudi weekend (Thu-Fri)
        DateTime expected_dates[] = {
            DATE_2013_05_01,  // Wed - Start
            DATE_2013_05_04,  // Sat - After weekend
            DATE_2013_05_05,  // Sun
            DATE_2013_05_06,  // Mon
            DATE_2013_05_07   // Tue
        };
        
        for (uint32_t i = 0; i < periods; i++) {
            auto datetime = get_datetime_from_index(range, i);
            REQUIRE(datetime == expected_dates[i]);
        }
    }
    
    SECTION("Complex Holiday and Weekmask Scenario") {
        // Define a custom business day with both holidays and custom weekmask
        std::vector<DateTime> complex_holidays = {
            DATE_2013_05_01,  // Wednesday
            DATE_2013_05_07   // Tuesday
        };
        
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.weekmask = to_weekmask(saudi_weekmask);
        params.holidays = complex_holidays;
        auto offset = efo::cbday(params);
        
        // Start from April 30, 2013 (Tuesday) and move forward
        auto apr30 = DATE_2013_04_30;
        
        // Expected progression: Apr 30 -> May 4 (skipping May 1 holiday and Thu-Fri weekend)
        auto result1 = offset->add(apr30.timestamp());
        REQUIRE(to_datetime(result1) == DATE_2013_05_04);
        
        // May 4 -> May 5
        auto result2 = offset->add(to_datetime(result1).timestamp());
        REQUIRE(to_datetime(result2) == DATE_2013_05_05);
        
        // May 5 -> May 6
        auto result3 = offset->add(to_datetime(result2).timestamp());
        REQUIRE(to_datetime(result3) == DATE_2013_05_06);
        
        // May 6 -> May 8 (skipping May 7 holiday)
        auto result4 = offset->add(to_datetime(result3).timestamp());
        REQUIRE(to_datetime(result4) == DATE_2013_05_08);
    }
}

TEST_CASE("Calendar Integration", "[business_day]") {
    SECTION("US Federal Holidays") {
        // US Federal Holidays in 2014
        std::vector<DateTime> us_federal_holidays = {
            DATE_2008_01_01,   // New Year's Day
            DATE_2008_01_21,   // Martin Luther King Jr. Day
            DATE_2008_02_18,   // Presidents Day
            DATE_2008_05_26,   // Memorial Day
            DATE_2008_07_04,   // Independence Day
            DATE_2008_09_01,   // Labor Day
            DATE_2008_10_13,   // Columbus Day
            DATE_2008_11_11,   // Veterans Day
            DATE_2008_11_27,   // Thanksgiving
            DATE_2008_12_25    // Christmas
        };
        
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.holidays = us_federal_holidays;
        auto offset = efo::cbday(params);
        
        // Test that specific holidays are correctly skipped
        auto jan17 = DateTime{{2014y, January, 17d}};
        auto result = offset->add(jan17.timestamp());
        auto jan21 = DateTime{{2014y, January, 21d}};
        REQUIRE(to_datetime(result) == jan21);
        
        // July 3, 2014 is a Thursday, next business day is Monday (July 7) because Friday is Independence Day
        auto jul3 = DateTime{{2014y, July, 3d}};
        result = offset->add(jul3.timestamp());
        REQUIRE(to_datetime(result) == DateTime{{2014y, July, 7d}});
    }
    
    SECTION("Alternative Weekmask and Calendar") {
        // Islamic Holidays for 2013
        std::vector<DateTime> islamic_holidays = {
            DateTime{{2013y, August, 8d}}, // Eid al-Fitr
            DateTime{{2013y, October, 15d}}, // Eid al-Adha
            DateTime{{2013y, November, 5d}}  // Islamic New Year
        };
        
        // Saudi weekmask (Sat-Wed work week, Thu-Fri weekend)
        np::WeekMask saudi_weekmask = {
            true,  // Monday
            true,  // Tuesday
            true,  // Wednesday
            false, // Thursday  
            false, // Friday
            true,  // Saturday
            true   // Sunday
        };
        
        // Create a CustomBusinessDay with Saudi weekmask and Islamic holidays
        efo::CustomBusinessDayParams saudi_params;
        saudi_params.n = 1;
        saudi_params.weekmask = saudi_weekmask;
        saudi_params.holidays = islamic_holidays;
        auto saudi_cal = efo::cbday(saudi_params);
        
        auto aug7 = DateTime{{2013y, August, 7d}};
        
        // Aug 7 + 1 business day = Aug 10 (Aug 8 is holiday, Aug 9 is weekend)
        auto result = saudi_cal->add(aug7.timestamp());
        REQUIRE(to_datetime(result).timestamp() == DateTime{{2013y, August, 10d}}.timestamp());
    }
    
    // Original section left for reference
    SECTION("US Federal Holidays (Placeholder)") {
        // This would require integration with a holiday calendar
        // In the Python code, this uses the USFederalHolidayCalendar
        
        /* Test placeholder for US Federal Holiday Calendar integration
        auto calendar = create_us_federal_holiday_calendar();
        auto offset = std::make_shared<BusinessDay>(1, default_weekmask, {}, calendar);
        
        // January 17, 2014 (Friday) + 1 -> January 21, 2014 (Tuesday)
        // (skips January 20, which is Martin Luther King, Jr. Day)
        auto jan17 = DateTime::parse("2014-01-17");
        auto result = offset->add(jan17.timestamp());
        REQUIRE(to_datetime(result) == DateTime::parse("2014-01-21"));
        */
    }
    
    SECTION("Pickle Compatibility") {
        // This test is for Python serialization, not applicable for C++
        // Comment: Not applicable for C++, refers to serialization in Python
    }

    SECTION("With Holidays") {
        // Define some holidays
        std::vector<DateTime> holidays = {DATE_2008_01_02, DATE_2008_01_03};
        
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.holidays = holidays;
        
        auto offset = efo::cbday(params, static_cast<np::BusinessDayCalendarPtr>(nullptr));
        
        // Jan 2 and 3 are now holidays, so Jan 1 + 1 business day = Jan 4
        auto result = offset->add(DATE_2008_01_01.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_04);
        
        // Jan 4 + 1 business day = Jan 7 (since Jan 5-6 is weekend)
        result = offset->add(DATE_2008_01_04.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_07);
    }

    SECTION("Calendar Integration") {
        // Define a weekmask for Saudi Arabia (Sat-Wed work week, Thu-Fri weekend)
        np::WeekMask saudi_weekmask = {
            true,  // Monday
            true,  // Tuesday
            true,  // Wednesday
            false, // Thursday
            false, // Friday
            true,  // Saturday
            true   // Sunday
        };
        
        // Define some holidays
        std::vector<DateTime> holidays = {DATE_2013_05_01};
        
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.weekmask = saudi_weekmask;
        params.holidays = holidays;
        auto offset_saudi = efo::cbday(params);
        
        // Test date range with the business day offset
        using namespace epochframe::factory::index;
        
        auto start = DATE_2013_04_30;
        int periods = 5;
        
        // Create DateRangeOptions with all required fields
        epochframe::factory::index::DateRangeOptions options{
            start.timestamp(),                  // start
            std::nullopt,                       // end
            periods,                            // periods
            offset_saudi,                       // offset
            "",                                 // tz
            AmbiguousTimeHandling::RAISE,       // ambiguous
            NonexistentTimeHandling::RAISE      // nonexistent
        };
        
        auto index = date_range(options);
        REQUIRE(index->size() == periods);
    }

    SECTION("US Calendar Integration") {
        // Define some holidays
        std::vector<DateTime> holidays = {DATE_2013_01_01, DATE_2013_01_21};
        
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.holidays = holidays;
        
        auto offset = efo::cbday(params);
        
        // January 1, 2013 is a holiday, so one business day after December 31, 2012 is January 2
        auto result = offset->add(DATE_2012_12_31.timestamp());
        REQUIRE(to_datetime(result) == DATE_2013_01_02);
    }
    
    SECTION("Saudi Calendar Integration") {
        // Create Saudi calendar with Eid as a holiday
        std::vector<DateTime> saudi_holidays = {DATE_2013_08_08};
        
        efo::CustomBusinessDayParams saudi_params;
        saudi_params.n = 1;
        saudi_params.weekmask = np::WeekMask{true, true, true, false, false, true, true};
        saudi_params.holidays = saudi_holidays;
        
        auto saudi_cal = efo::cbday(saudi_params);
        
        // August 8, 2013 is a holiday, and August 9-10 is weekend in Saudi
        // One business day after August 7 should be August 11
        auto result = saudi_cal->add(DATE_2013_08_07.timestamp());
        REQUIRE(to_datetime(result) == DATE_2013_08_11);
    }
    
    SECTION("Holidays and Custom Weekmask") {
        // Define some holidays
        std::vector<DateTime> holidays = {DATE_2008_01_02, DATE_2008_01_03};
        
        efo::CustomBusinessDayParams params;
        params.n = 1;
        params.weekmask = np::WeekMask{true, true, true, true, false, false, false}; // Mon-Thu work week
        params.holidays = holidays;
        
        auto offset = efo::cbday(params);
        
        // Jan 2 and 3 are holidays, Jan 4-6 is weekend in this weekmask
        // So Jan 1 + 1 business day = Jan 7
        auto result = offset->add(DATE_2008_01_01.timestamp());
        REQUIRE(to_datetime(result) == DATE_2008_01_07);
    }
} 