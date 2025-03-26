#include <catch.hpp>
#include "calendar/calendars/all.h"
#include "epoch_frame/series.h"

TEST_CASE("NYSE Calendar", "[calendar]") {
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;

    SECTION("test_custom_open_close")
    {
        NYSEExhangeCalendar cal{MarketTime{Time{9h}}, MarketTime{Time{10h}}};
        auto sched = cal.schedule(Date{2024y, August, 16d}, Date{2024y, August, 16d}, ScheduleOptions{});
        
        REQUIRE(sched.iloc(0, "MarketOpen").to_datetime() == "2024-08-16 13:00:00"__dt.replace_tz(UTC));
        REQUIRE(sched.iloc(0, "MarketClose").to_datetime() == "2024-08-16 14:00:00"__dt.replace_tz(UTC));
    }
}
