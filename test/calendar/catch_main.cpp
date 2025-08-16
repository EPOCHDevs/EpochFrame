//
// Created by adesola on 8/16/25.
//
#include <catch2/catch_session.hpp>
#include "epoch_frame/factory/calendar_factory.h"

int main(int argc, char** argv) {
    // Prewarm anything your tests will use:
    epoch_frame::calendar::CalendarFactory::instance().Init(); // add more as needed
    return Catch::Session().run(argc, argv);
}
