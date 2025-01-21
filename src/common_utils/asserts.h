//
// Created by adesola on 1/20/25.
//

#pragma once
#include <arrow/api.h>
#include <epoch_lab_shared/macros.h>
#include "epochframe/aliases.h"


namespace epochframe {
    template<typename U, typename T> requires (!std::is_same_v<U, arrow::Array>)
    std::shared_ptr<U> PtrCast(const std::shared_ptr<T>  &datum) {
        AssertWithTraceFromFormat(datum != nullptr, "Failed to cast pointer, got null");

        std::string typeAsString{U::TypeClass::type_name()};
        std::string datumTypeAsString;

        if constexpr (std::is_same_v<T, arrow::Scalar>) {
            datumTypeAsString = datum->type->ToString();
        } else {
            datumTypeAsString = datum->type()->ToString();
        }

        AssertWithTraceFromFormat( typeAsString == "utf8" ? datumTypeAsString == "string" : datumTypeAsString == typeAsString,
                                  fmt::format("Failed to cast pointer, Expected type {}, got {}",
                                              typeAsString, datumTypeAsString));

        auto ptr = std::dynamic_pointer_cast<U>(std::move(datum));
        AssertWithTraceFromFormat(ptr != nullptr, fmt::format("Failed to cast pointer to type {}. got null",
                                                              typeAsString));
        return ptr;
    }

    void AssertStatusIsOk(const arrow::Status &status);

    template<typename U, typename T>
    std::shared_ptr<U> AssertCastResultIsOk(arrow::Result<std::shared_ptr<T>> &&status) {
        if (status.ok()) {
            return PtrCast<U>(status.MoveValueUnsafe());
        }
        throw std::runtime_error(status.status().ToString());
    }

    template<typename T>
    std::shared_ptr<T> AssertResultIsOk(const arrow::Result<std::shared_ptr<T>> &result) {
        if (result.ok()) {
            return result.ValueUnsafe();
        }
        throw std::runtime_error(result.status().ToString());
    }


    inline arrow::ArrayPtr AssertArrayResultIsOk(const arrow::Result<arrow::Datum> &result) {
        if (result.ok()) {
            return result->make_array();
        }
        throw std::runtime_error(result.status().ToString());
    }

    template<typename T>
    inline std::shared_ptr<T> AssertCastArrayResultIsOk(const arrow::Result<arrow::Datum> &result) {
        if (result.ok()) {
            try {
                return result->array_as<T>();
            }catch (const std::exception&e) {
                throw std::runtime_error(fmt::format("Failed to cast array to type {}: {}", std::string{T::TypeClass::type_name()}, e.what()));
            }
        }
        throw std::runtime_error(result.status().ToString());
    }

    template<typename ArrowScalarType>
    inline ArrowScalarType AssertCastScalarResultIsOk(const arrow::Result<arrow::Datum> &result) {
        if (result.ok()) {
            try {
                return result->scalar_as<ArrowScalarType>();
            }catch (const std::exception&e) {
                throw std::runtime_error(fmt::format("Failed to cast scalar to type {}: {}", std::string{ArrowScalarType::TypeClass::type_name()}, e.what()));
            }
        }
        throw std::runtime_error(result.status().ToString());
    }
}
