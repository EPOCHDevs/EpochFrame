#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/calendar_factory.h"
#include <iostream>

using namespace epoch_frame;
using namespace epoch_frame::factory;
using namespace epoch_frame::factory::offset;
using namespace epoch_frame::factory::index;
using namespace epoch_frame::calendar;

int main() {
    auto cal = CalendarFactory::instance().get_calendar("NYSE");
    auto schedule = cal->schedule("2025-01-03"__date.date(), "2025-01-10"__date.date(), {});
    
    auto d0_open = schedule["MarketOpen"].iloc(0);
    auto d0_close = schedule["MarketClose"].iloc(0);
    
    auto session = SessionRange{d0_open.to_datetime().time(), d0_close.to_datetime().time()};
    auto after_open_n1 = session_anchor(session, SessionAnchorWhich::AfterOpen,
                                       TimeDelta{TimeDelta::Components{.minutes = 2}}, 1);
    
    std::cout << "d0_open: " << d0_open.to_datetime().format("%Y-%m-%d %H:%M:%S %Z") << std::endl;
    
    auto ao0 = after_open_n1->add(d0_open.timestamp());
    std::cout << "ao0: " << Scalar{ao0}.to_datetime().format("%Y-%m-%d %H:%M:%S %Z") << std::endl;
    
    auto ao1 = after_open_n1->add(ao0);
    std::cout << "ao1: " << Scalar{ao1}.to_datetime().format("%Y-%m-%d %H:%M:%S %Z") << std::endl;
    
    // Now check date_range
    Array rng1(date_range({.start = d0_open.timestamp(), .periods = 3, .offset = after_open_n1})->array());
    std::cout << "rng1[0]: " << rng1[0].to_datetime().format("%Y-%m-%d %H:%M:%S %Z") << std::endl;
    std::cout << "rng1[1]: " << rng1[1].to_datetime().format("%Y-%m-%d %H:%M:%S %Z") << std::endl;
    std::cout << "rng1[2]: " << rng1[2].to_datetime().format("%Y-%m-%d %H:%M:%S %Z") << std::endl;
    
    return 0;
}
