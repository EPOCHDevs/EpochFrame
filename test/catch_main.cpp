//
// Created by adesola on 3/21/25.
//
#include <catch2/catch_session.hpp>
#include "epochframe/serialization.h"

int main( int argc, char* argv[] ) {
    // your setup ...
    epochframe::ScopedS3 scoped_s3;
    int result = Catch::Session().run( argc, argv );

    // your clean-up...

    return result;
}
