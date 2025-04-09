#include "calendar/calendars/all.h"
#include "date_time/business/np_busdaycal.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <catch.hpp>
#include <chrono>
#include <memory>
#include <variant>

using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("CME Globex Equities Calendar", "[calendar]")
{
    using namespace epoch_frame::calendar;
    using namespace epoch_frame;
    static CMEGlobexEquitiesExchangeCalendar cal;

    SECTION("test_time_zone")
    {
        REQUIRE(cal.tz() == "America/Chicago");
        REQUIRE(cal.name() == "CME Globex Equities");
    }

    SECTION("test_2009_through_2023_holidays")
    {
        struct TestCase
        {
            DateTime                                       date;
            std::variant<Time, epoch_core::MarketTimeType> market_time;
        };

        static std::vector<TestCase> test_cases = {
            // 2009
            // 2009 Martin Luther King Day (19th = Monday)
            {"2009-01-16"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2009-01-19"__date, Time{10h, 30min}},
            {"2009-01-20"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2009 Presidents Day (16th = Monday)
            {"2009-02-13"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2009-02-16"__date, Time{10h, 30min}},
            {"2009-02-17"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2009 Good Friday (10th = Friday)
            {"2009-04-09"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2009-04-10"__date, epoch_core::MarketTimeType::MarketClose},
            {"2009-04-13"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2009 Memorial Day (May 25 = Monday)
            {"2009-05-22"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2009-05-25"__date, Time{10h, 30min}},
            {"2009-05-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2009 Independence Day (4th = Saturday)
            {"2009-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2009-07-03"__date, Time{10h, 30min}},
            {"2009-07-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2009 Labor Day (7th = Monday)
            {"2009-09-04"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2009-09-07"__date, Time{10h, 30min}},
            {"2009-09-08"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2009 Thanksgiving (26th = Thursday)
            {"2009-11-25"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2009-11-26"__date, Time{10h, 30min}},
            {"2009-11-27"__date, Time{12h, 15min}},
            {"2009-11-30"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2009 Christmas (25th = Friday)
            {"2009-12-24"__date, Time{12h, 15min}},
            {"2009-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2009-12-28"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2009-12-29"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2009/10 New Year's (Dec 31 = Thur)
            {"2009-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2010-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2010-01-04"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2010
            // 2010 Martin Luther King Day (18th = Monday)
            {"2010-01-15"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2010-01-18"__date, Time{10h, 30min}},
            {"2010-01-19"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2010 Presidents Day (15th = Monday)
            {"2010-02-12"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2010-02-15"__date, Time{10h, 30min}},
            {"2010-02-16"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2010 Good Friday (2nd = Friday)
            {"2010-04-01"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2010-04-02"__date, Time{8h, 15min}},
            {"2010-04-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2010 Memorial Day (May 31 = Monday)
            {"2010-05-18"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2010-05-31"__date, Time{10h, 30min}},
            {"2010-06-01"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2010 Independence Day (4th = Sunday)
            {"2010-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2010-07-05"__date, Time{10h, 30min}},
            {"2010-07-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2010 Labor Day (6th = Monday)
            {"2010-09-03"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2010-09-06"__date, Time{10h, 30min}},
            {"2010-09-07"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2010 Thanksgiving (25th = Thursday)
            {"2010-11-24"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2010-11-25"__date, Time{10h, 30min}},
            {"2010-11-26"__date, Time{12h, 15min}},
            {"2010-11-29"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2010 Christmas (25th = Saturday)
            {"2010-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2010-12-24"__date, epoch_core::MarketTimeType::MarketClose},
            {"2010-12-27"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2010/11 New Year's (Dec 31 = Fri)
            {"2010-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2011-01-03"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2011
            // 2011 Martin Luther King Day (17th = Monday)
            {"2011-01-14"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2011-01-17"__date, Time{10h, 30min}},
            {"2011-01-18"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2011 Presidents Day (21st = Monday)
            {"2011-02-18"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2011-02-21"__date, Time{10h, 30min}},
            {"2011-02-22"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2011 Good Friday (22th = Friday)
            {"2011-04-21"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2011-04-22"__date, epoch_core::MarketTimeType::MarketClose},
            {"2011-04-25"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2011 Memorial Day (May 30 = Monday)
            {"2011-05-27"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2011-05-30"__date, Time{10h, 30min}},
            {"2011-05-31"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2011 Independence Day (4th = Monday)
            {"2011-07-01"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2011-07-04"__date, Time{10h, 30min}},
            {"2011-07-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2011 Labor Day (5th = Monday)
            {"2011-09-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2011-09-05"__date, Time{10h, 30min}},
            {"2011-09-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2011 Thanksgiving (24th = Thursday)
            {"2011-11-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2011-11-24"__date, Time{10h, 30min}},
            {"2011-11-25"__date, Time{12h, 15min}},
            {"2011-11-28"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2011 Christmas (25th = Sunday)
            {"2011-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2011-12-26"__date, epoch_core::MarketTimeType::MarketClose},
            {"2011-12-27"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2011/21 New Year's (Dec 31 = Saturday)
            {"2011-12-30"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2012-01-02"__date, epoch_core::MarketTimeType::MarketClose},
            {"2012-01-03"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2012
            // 2012 Martin Luther King Day (16th = Monday)
            {"2012-01-13"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2012-01-16"__date, Time{10h, 30min}},
            {"2012-01-17"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2012 Presidents Day (20th = Monday)
            {"2012-02-17"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2012-02-20"__date, Time{10h, 30min}},
            {"2012-02-21"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2012 Good Friday (06th = Friday)
            {"2012-04-05"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2012-04-06"__date, Time{8h, 15min}},
            {"2012-04-09"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2012 Memorial Day (May 28 = Monday)
            {"2012-05-25"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2012-05-28"__date, Time{10h, 30min}},
            {"2012-05-29"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2012 Independence Day (4th = Wednesday)
            {"2012-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2012-07-03"__date, Time{12h, 15min}},
            {"2012-07-04"__date, Time{10h, 30min}},
            {"2012-07-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2012 Labor Day (3rd = Monday)
            {"2012-08-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2012-09-03"__date, Time{10h, 30min}},
            {"2012-09-04"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2012 Thanksgiving (22 = Thursday)
            {"2012-11-21"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2012-11-22"__date, Time{10h, 30min}},
            {"2012-11-23"__date, Time{12h, 15min}},
            {"2012-11-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2012 Christmas (25th = Friday)
            {"2012-12-24"__date, Time{12h, 15min}},
            {"2012-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2012-12-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2012/21 New Year's (Dec 31 = Monday)
            {"2012-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2013-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2013-01-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2013
            // 2013 Martin Luther King Day (21st = Monday)
            {"2013-01-18"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2013-01-21"__date, Time{10h, 30min}},
            {"2013-01-22"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2013 Presidents Day (18th = Monday)
            {"2013-02-15"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2013-02-18"__date, Time{10h, 30min}},
            {"2013-02-19"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2013 Good Friday (3/29 = Friday)
            {"2013-03-28"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2013-03-29"__date, epoch_core::MarketTimeType::MarketClose},
            {"2013-04-01"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2013 Memorial Day (May 27 = Monday)
            {"2013-05-24"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2013-05-27"__date, Time{10h, 30min}},
            {"2013-05-28"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2013 Independence Day (4th = Thursday)
            {"2013-07-03"__date, Time{12h, 15min}},
            {"2013-07-04"__date, Time{10h, 30min}},
            {"2013-07-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2013 Labor Day (2nd = Monday)
            {"2013-08-30"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2013-09-02"__date, Time{10h, 30min}},
            {"2013-09-03"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2013 Thanksgiving (28th = Thursday)
            {"2013-11-27"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2013-11-28"__date, Time{10h, 30min}},
            {"2013-11-29"__date, Time{12h, 15min}},
            {"2013-04-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2013 Christmas (25th = Wednesday)
            {"2013-12-24"__date, Time{12h, 15min}},
            {"2013-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2013-12-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2013/21 New Year's (Dec 31 = Tue)
            {"2013-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2014-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2014-01-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2014
            // 2014 Martin Luther King Day (20th = Monday)
            {"2014-01-17"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2014-01-20"__date, Time{10h, 30min}},
            {"2014-01-21"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2014 Presidents Day (17th = Monday)
            {"2014-02-14"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2014-02-17"__date, Time{10h, 30min}},
            {"2014-02-18"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2014 Good Friday (18th = Friday)
            {"2014-04-17"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2014-04-18"__date, epoch_core::MarketTimeType::MarketClose},
            {"2014-04-21"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2014 Memorial Day (May 26 = Monday)
            {"2014-05-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2014-05-26"__date, Time{12h, 0min}},
            {"2014-05-27"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2014 Independence Day (4th = Friday)
            {"2014-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2014-07-03"__date, Time{12h, 15min}},
            {"2014-07-04"__date, Time{12h, 0min}},
            {"2014-07-07"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2014 Labor Day (1st = Monday)
            {"2014-08-29"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2014-09-01"__date, Time{12h, 0min}},
            {"2014-09-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2014 Thanksgiving (27th = Thursday)
            {"2014-11-26"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2014-11-27"__date, Time{12h, 0min}},
            {"2014-11-28"__date, Time{12h, 15min}},
            {"2014-12-01"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2014 Christmas (25th = Thursday)
            {"2014-12-24"__date, Time{12h, 15min}},
            {"2014-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2014-12-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2014/15 New Year's (Dec 31 = Wednesday)
            {"2014-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2015-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2015-01-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2015
            // 2015 Martin Luther King Day (19th = Monday)
            {"2015-01-16"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2015-01-19"__date, Time{12h, 0min}},
            {"2015-01-20"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2015 Presidents Day (16th = Monday)
            {"2015-02-13"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2015-02-16"__date, Time{12h, 0min}},
            {"2015-02-17"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2015 Good Friday (03th = Friday)
            {"2015-04-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2015-04-03"__date, Time{8h, 15min}},
            {"2015-04-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2015 Memorial Day (May 25 = Monday)
            {"2015-05-22"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2015-05-25"__date, Time{12h, 0min}},
            {"2015-05-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2015 Independence Day (4th = Saturday)
            {"2015-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2015-07-03"__date, Time{12h, 0min}},
            {"2015-07-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2015 Labor Day (7th = Monday)
            {"2015-09-04"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2015-09-07"__date, Time{12h, 0min}},
            {"2015-09-08"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2015 Thanksgiving (26th = Thursday)
            {"2015-11-25"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2015-11-26"__date, Time{12h, 0min}},
            {"2015-11-27"__date, Time{12h, 15min}},
            {"2015-11-30"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2015 Christmas (25th = Friday)
            {"2015-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2015-12-24"__date, Time{12h, 15min}},
            {"2015-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2015-12-28"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2015/16 New Year's (Dec 31 = Thur)
            {"2015-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2016-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2016-01-04"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2016
            // 2016 Martin Luther King Day (18th = Monday)
            {"2016-01-15"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2016-01-18"__date, Time{12h, 0min}},
            {"2016-01-19"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2016 Presidents Day (15th = Monday)
            {"2016-02-12"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2016-02-15"__date, Time{12h, 0min}},
            {"2016-02-16"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2016 Good Friday (3/25th = Friday)
            {"2016-03-24"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2016-03-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2016-03-28"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2016 Memorial Day (May 30 = Monday)
            {"2016-05-27"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2016-05-30"__date, Time{12h, 0min}},
            {"2016-05-31"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2016 Independence Day (4th = Monday)
            {"2016-07-01"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2016-07-04"__date, Time{12h, 0min}},
            {"2016-07-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2016 Labor Day (5th = Monday)
            {"2016-09-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2016-09-05"__date, Time{12h, 0min}},
            {"2016-09-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2016 Thanksgiving (24 = Thursday)
            {"2016-11-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2016-11-24"__date, Time{12h, 0min}},
            {"2016-11-25"__date, Time{12h, 15min}},
            {"2016-11-28"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2016 Christmas (25th = Sunday)
            {"2016-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2016-12-26"__date, epoch_core::MarketTimeType::MarketClose},
            {"2016-12-27"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2016/17 New Year's (Dec 31 = Saturday)
            {"2016-12-30"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2017-01-02"__date, epoch_core::MarketTimeType::MarketClose},
            {"2017-01-03"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2017
            // 2017 Martin Luther King Day (16th = Monday)
            {"2017-01-13"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2017-01-16"__date, Time{12h, 0min}},
            {"2017-01-17"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2017 Presidents Day (20th = Monday)
            {"2017-02-17"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2017-02-20"__date, Time{12h, 0min}},
            {"2017-02-21"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2017 Good Friday (14th = Friday)
            {"2017-04-13"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2017-04-14"__date, epoch_core::MarketTimeType::MarketClose},
            {"2017-04-17"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2017 Memorial Day (May 29 = Monday)
            {"2017-05-26"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2017-05-29"__date, Time{12h, 0min}},
            {"2017-05-30"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2017 Independence Day (4th = Tuesday)
            {"2017-06-30"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2017-07-03"__date, Time{12h, 15min}},
            {"2017-07-04"__date, Time{12h, 0min}},
            {"2017-07-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2017 Labor Day (4th = Monday)
            {"2017-09-01"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2017-09-04"__date, Time{12h, 0min}},
            {"2017-09-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2017 Thanksgiving (23 = Thursday)
            {"2017-11-22"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2017-11-23"__date, Time{12h, 0min}},
            {"2017-11-24"__date, Time{12h, 15min}},
            {"2017-11-27"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2017 Christmas (25th = Monday)
            {"2017-12-22"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2017-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2017-12-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2017/18 New Year's (Dec 31 = Sunday)
            {"2017-12-29"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2018-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2018-01-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2018
            // 2018 Martin Luther King Day (15th = Monday)
            {"2018-01-12"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2018-01-15"__date, Time{12h, 0min}},
            {"2018-01-16"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2018 Presidents Day (19th = Monday)
            {"2018-02-16"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2018-02-19"__date, Time{12h, 0min}},
            {"2018-02-20"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2018 Good Friday (3/30th = Friday)
            {"2018-03-29"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2018-03-30"__date, epoch_core::MarketTimeType::MarketClose},
            {"2018-04-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2018 Memorial Day (May 28 = Monday)
            {"2018-05-25"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2018-05-28"__date, Time{12h, 0min}},
            {"2018-05-29"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2018 Independence Day (4th = Wednesday)
            {"2018-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2018-07-03"__date, Time{12h, 15min}},
            {"2018-07-04"__date, Time{12h, 0min}},
            {"2018-07-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2018 Labor Day (3rd = Monday)
            {"2018-08-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2018-09-03"__date, Time{12h, 0min}},
            {"2018-09-04"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2018 Thanksgiving (22nd = Thursday)
            {"2018-11-21"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2018-11-22"__date, Time{12h, 0min}},
            {"2018-11-23"__date, Time{12h, 15min}},
            {"2018-11-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2018 Christmas (25th = Tuesday)
            {"2018-12-21"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2018-12-24"__date, Time{12h, 15min}},
            {"2018-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2018-12-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2018/19 New Year's (Jan 1 = Tuesday)
            {"2018-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2019-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2019-01-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2019
            // 2019 Martin Luther King Day (21st = Monday)
            {"2019-01-18"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2019-01-21"__date, Time{12h, 0min}},
            {"2019-01-22"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2019 Presidents Day (18th = Monday)
            {"2019-02-15"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2019-02-18"__date, Time{12h, 0min}},
            {"2019-02-19"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2019 Good Friday (19th = Friday)
            {"2019-04-18"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2019-04-19"__date, epoch_core::MarketTimeType::MarketClose},
            {"2019-04-22"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2019 Memorial Day (May 27 = Monday)
            {"2019-05-24"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2019-05-27"__date, Time{12h, 0min}},
            {"2019-05-28"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2019 Independence Day (4th = Thursday)
            {"2019-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2019-07-03"__date, Time{12h, 15min}},
            {"2019-07-04"__date, Time{12h, 0min}},
            {"2019-07-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2019 Labor Day (2nd = Monday)
            {"2019-08-30"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2019-09-02"__date, Time{12h, 0min}},
            {"2019-09-03"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2019 Thanksgiving (28 = Thursday)
            {"2019-11-27"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2019-11-28"__date, Time{12h, 0min}},
            {"2019-11-29"__date, Time{12h, 15min}},
            {"2019-12-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2019 Christmas (25th = Wednesday)
            {"2019-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2019-12-24"__date, Time{12h, 15min}},
            {"2019-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2019-12-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2019/20 New Year's (Jan 1 = Wednesday)
            {"2019-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2020-01-02"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020
            // 2020 Martin Luther King Day (20th = Monday)
            {"2020-01-17"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-01-20"__date, Time{12h, 0min}},
            {"2020-01-21"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Presidents Day (17th = Monday)
            {"2020-02-14"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-02-17"__date, Time{12h, 0min}},
            {"2020-02-18"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Good Friday (10th = Friday)
            {"2020-04-09"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-04-10"__date, epoch_core::MarketTimeType::MarketClose},
            {"2020-04-13"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Memorial Day (May 25 = Monday)
            {"2020-05-22"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-05-25"__date, Time{12h, 0min}},
            {"2020-05-26"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Independence Day (4th = Saturday)
            {"2020-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-07-03"__date, Time{12h, 0min}},
            {"2020-07-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Labor Day (7th = Monday)
            {"2020-09-04"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-09-07"__date, Time{12h, 0min}},
            {"2020-09-08"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Thanksgiving (26th = Thursday)
            {"2020-11-25"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-11-26"__date, Time{12h, 0min}},
            {"2020-11-27"__date, Time{12h, 15min}},
            {"2020-11-30"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020 Christmas (25th = Friday)
            {"2020-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2020-12-24"__date, Time{12h, 15min}},
            {"2020-12-25"__date, epoch_core::MarketTimeType::MarketClose},
            {"2020-12-28"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2020/21 New Year's (Jan 1 = Friday)
            {"2020-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-01-01"__date, epoch_core::MarketTimeType::MarketClose},
            {"2021-01-04"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021
            // 2021 Martin Luther King Day (18th = Monday)
            {"2021-01-15"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-01-18"__date, Time{12h, 0min}},
            {"2021-01-19"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Presidents Day (15th = Monday)
            {"2021-02-12"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-02-15"__date, Time{12h, 0min}},
            {"2021-02-16"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Good Friday (2nd = Friday)
            {"2021-04-01"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-04-02"__date, Time{8h, 15min}},
            {"2021-04-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Memorial Day (May 31 = Monday)
            {"2021-05-28"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-05-31"__date, Time{12h, 0min}},
            {"2021-06-01"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Independence Day (4th = Sunday)
            {"2021-07-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-07-05"__date, Time{12h, 0min}},
            {"2021-07-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Labor Day (6th = Monday)
            {"2021-09-03"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-09-06"__date, Time{12h, 0min}},
            {"2021-09-07"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Thanksgiving (25th = Thursday)
            {"2021-11-24"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-11-25"__date, Time{12h, 0min}},
            {"2021-11-26"__date, Time{12h, 15min}},
            {"2021-11-29"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021 Christmas (25th = Saturday)
            {"2021-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2021-12-24"__date, epoch_core::MarketTimeType::MarketClose},
            {"2021-12-27"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2021/22 New Year's (Jan 1 = Saturday)
            {"2021-12-31"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-01-03"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022
            // 2022 Martin Luther King Day (17th = Monday)
            {"2022-01-14"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-01-17"__date, Time{12h, 0min}},
            {"2022-01-18"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 President's Day (21st = Monday)
            {"2022-02-18"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-02-21"__date, Time{12h, 0min}},
            {"2022-02-22"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Good Friday (15 = Friday)
            {"2022-04-14"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-04-15"__date, epoch_core::MarketTimeType::MarketClose},
            {"2022-04-18"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Memorial Day (30th = Monday)
            {"2022-05-27"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-05-30"__date, Time{12h, 0min}},
            {"2022-05-31"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Juneteenth (20th = Monday)
            {"2022-06-17"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-06-20"__date, Time{12h, 0min}},
            {"2022-06-21"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Independence Day (4th = Monday)
            {"2022-07-01"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-07-04"__date, Time{12h, 0min}},
            {"2022-07-05"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Labor Day (5th = Monday)
            {"2022-09-02"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-09-05"__date, Time{12h, 0min}},
            {"2022-09-06"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Thanksgiving (24th = Thursday)
            {"2022-11-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-11-24"__date, Time{12h, 0min}},
            {"2022-11-25"__date, Time{12h, 15min}},
            {"2022-11-28"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022 Christmas (25 = Sunday)
            {"2022-12-23"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2022-12-26"__date, epoch_core::MarketTimeType::MarketClose},
            {"2022-12-27"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2022/23 New Years (Jan 1 = Sunday)
            {"2022-12-30"__date, epoch_core::MarketTimeType::MarketOpen},
            {"2023-01-02"__date, epoch_core::MarketTimeType::MarketClose},
            {"2023-01-03"__date, epoch_core::MarketTimeType::MarketOpen},
            // 2023 Good Friday (7 = Friday)
            {"2023-04-07"__date, Time{8h, 15min}}

        };

        for (const auto& [date_, market_time] : test_cases)
        {
            std::visit(
                [&]<typename T>(T const& market_time)
                {
                    auto date = date_.replace_tz(CST);
                    DYNAMIC_SECTION("date: " << date << " market_time: " << market_time)
                    {
                        auto   date_scalar = Scalar{date};
                        auto   offset      = date_scalar.dt().is_dst().as_bool() ? 5.0 : 6.0;
                        auto   delta       = TimeDelta{{.hours = offset}};
                        Scalar day_ts{date + delta};
                        auto   year = date.date.year;
                        auto   schedule =
                            cal.schedule(Date{year, January, 1d},
                                         Date{year + years(1), January, 1d}, {.tz = CST});

                        if constexpr (std::is_same_v<T, Time>)
                        {
                            auto market_open  = schedule.loc(date_scalar, "MarketOpen");
                            auto market_close = schedule.loc(date_scalar, "MarketClose");
                            auto expected_date =
                                offset::hours(17)->add(offset::days(-1)->add(day_ts.timestamp()));

                            Scalar expected_scalar{expected_date};

                            {
                                INFO("market_open_scalar: " << expected_scalar.repr());
                                REQUIRE(market_open == expected_scalar);
                            }

                            expected_date =
                                offset::minutes(market_time.minute.count())
                                    ->add(offset::hours(market_time.hour.count())
                                              ->add(offset::days(0)->add(day_ts.timestamp())));
                            expected_scalar = Scalar{expected_date};

                            {
                                INFO("market_open_scalar: " << expected_scalar.repr());
                                REQUIRE(market_close == expected_scalar);
                            }
                        }
                        else if constexpr (std::is_same_v<T, epoch_core::MarketTimeType>)
                        {
                            if (market_time == epoch_core::MarketTimeType::MarketOpen)
                            {
                                auto market_open  = schedule.loc(date_scalar, "MarketOpen");
                                auto market_close = schedule.loc(date_scalar, "MarketClose");

                                auto expected_open_date =
                                    offset::minutes(0)->add(offset::hours(17)->add(
                                        offset::days(-1)->add(day_ts.timestamp())));
                                Scalar expected_open_scalar{expected_open_date};

                                {
                                    INFO("market_open_scalar: " << expected_open_scalar.repr());
                                    REQUIRE(market_open == expected_open_scalar);
                                }

                                auto expected_close_date =
                                    offset::minutes(0)->add(offset::hours(16)->add(
                                        offset::days(0)->add(day_ts.timestamp())));
                                Scalar expected_close_scalar{expected_close_date};

                                {
                                    INFO("market_close_scalar: " << expected_close_scalar.repr());
                                    REQUIRE(market_close == expected_close_scalar);
                                }
                            }
                            else
                            {
                                REQUIRE_FALSE(
                                    schedule.index()->contains(day_ts.dt().tz_localize("")));
                            }
                        }
                    }
                },
                market_time);
        }
    }
}
