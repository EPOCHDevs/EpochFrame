//
// Created by adesola on 3/21/25.
//
#include <catch2/catch_session.hpp>
#include "epoch_frame/serialization.h"
#include "epoch_frame/factory/calendar_factory.h"

int main( int argc, char* argv[] ) {
    // your setup ...
    epoch_frame::ScopedS3 scoped_s3;
    epoch_frame::calendar::CalendarFactory::instance().Init(); // add more as needed

    int result = Catch::Session().run( argc, argv );

    // your clean-up...

    return result;
}
