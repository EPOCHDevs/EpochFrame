//
// Created by adesola on 1/20/25.
//
#include <catch2/catch_session.hpp>

int main( int argc, char* argv[] ) {
    // your setup ...

    int result = Catch::Session().run( argc, argv );

    // your clean-up...

    return result;
}