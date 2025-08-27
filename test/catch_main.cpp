//
// Created by adesola on 3/21/25.
//
#include <iostream>
#include <catch2/catch_session.hpp>
#include "epoch_frame/serialization.h"
#include "epoch_frame/factory/calendar_factory.h"

int main( int argc, char* argv[] ) {
    // your setup ...
    auto arrowComputeStatus = arrow::compute::Initialize();
    if (!arrowComputeStatus.ok()) {
        std::cout << "arrow compute initialized failed: " << arrowComputeStatus << std::endl;
        return 1;
    }
    epoch_frame::ScopedS3 scoped_s3;
    epoch_frame::calendar::CalendarFactory::instance().Init(); // add more as needed

    int result = Catch::Session().run( argc, argv );

    // your clean-up...

    return result;
}
