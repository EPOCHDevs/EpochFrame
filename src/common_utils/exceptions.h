//
// Created by adesola on 1/21/25.
//

#pragma once
#include <stdexcept>
#include "fmt/format.h"

#define CREATE_EXCEPTION_CLASS(Name) \
class Name : public std::runtime_error { \
public: \
    explicit Name(const std::string &format_msg) : std::runtime_error(std::string{#Name} + " : " + format_msg) {} \
}

namespace epochframe {
    CREATE_EXCEPTION_CLASS(ApplyTypeError);
    CREATE_EXCEPTION_CLASS(ValueError);
    CREATE_EXCEPTION_CLASS(NotImplementedError);
}
