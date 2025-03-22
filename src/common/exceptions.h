//
// Created by adesola on 1/21/25.
//

#pragma once
#include <stdexcept>
#include "fmt/format.h"
#include "arrow/api.h"
#include <sstream>


#define CREATE_EXCEPTION_CLASS(Name) \
class Name : public std::runtime_error { \
public: \
    explicit Name(const std::string &format_msg) : std::runtime_error(std::string{#Name} + " : " + format_msg) {} \
}

namespace epoch_frame {
    CREATE_EXCEPTION_CLASS(ApplyTypeError);

    CREATE_EXCEPTION_CLASS(ValueError);

    CREATE_EXCEPTION_CLASS(NotImplementedError);

    struct RawArrayCastException : std::exception {
        std::shared_ptr<arrow::DataType> requested_type, array_type;
        mutable std::string msg;

        RawArrayCastException(std::shared_ptr<arrow::DataType> requested_type,
                              std::shared_ptr<arrow::DataType> array_type)
                : requested_type(std::move(requested_type)), array_type(std::move(array_type)) {
        }

        const char *what() const noexcept override {
            std::stringstream ss;
            ss << "Calling values with wrong data type:\t"
               << "Requested DataType: " << requested_type->ToString() << "\n"
               << "Current DataType: " << array_type->ToString() << "\n";
            msg = ss.str();
            return msg.c_str();
        }
    };
}
