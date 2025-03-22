//
// Created by adesola on 1/20/25.
//

#pragma once
#include <arrow/api.h>
#include <epoch_core/macros.h>
#include "epochframe/aliases.h"


namespace epochframe {
    template<typename U, typename T>
    requires (!std::is_same_v<U, arrow::Array>)
    std::shared_ptr<U> PtrCast(const std::shared_ptr<T> &datum) {
        AssertFromFormat(datum != nullptr, "Failed to cast pointer, got null");

        std::string typeAsString{U::TypeClass::type_name()};
        std::string datumTypeAsString;

        if constexpr (std::is_same_v<T, arrow::Scalar>) {
            datumTypeAsString = datum->type->ToString();
        } else {
            datumTypeAsString = datum->type()->ToString();
        }

        AssertFromFormat(
                typeAsString == "utf8" ? datumTypeAsString == "string" : datumTypeAsString == typeAsString,
                "Failed to cast pointer, Expected type {}, got {}",
                            typeAsString, datumTypeAsString);

        auto ptr = std::dynamic_pointer_cast<U>(std::move(datum));
        AssertFromFormat(ptr != nullptr, "Failed to cast pointer to type {}. got null",
                                                              typeAsString);
        return ptr;
    }

    void AssertStatusIsOk(const arrow::Status &status);

    template<typename U, typename T>
    std::shared_ptr<U> AssertCastResultIsOk(arrow::Result<std::shared_ptr<T>>
                                            &&status) {
        if (status.ok()) {
            return PtrCast<U>(status.MoveValueUnsafe());
        }
        throw std::runtime_error(status.status().ToString());
    }

    template<typename T>
    std::shared_ptr<T> AssertResultIsOk(arrow::Result<std::shared_ptr<T>> result) {
        if (result.ok()) {
            return result.MoveValueUnsafe();
        }
        throw std::runtime_error(result.status().ToString());
    }

    inline arrow::Datum AssertResultIsOk(const arrow::Result<arrow::Datum> &result) {
        if (result.ok()) {
            return result.ValueUnsafe();
        }
        throw std::runtime_error(result.status().ToString());
    }

    inline arrow::ScalarPtr AssertScalarResultIsOk(const arrow::Result<arrow::Datum> &result) {
        if (result.ok()) {
            return result->scalar();
        }
        throw std::runtime_error(result.status().ToString());
    }

    template<typename T>
    std::unique_ptr<T>  AssertUniqueResultIsOk(arrow::Result< std::unique_ptr<T>> && result) {
        if (result.ok()) {
            return result.MoveValueUnsafe();
        }
        throw std::runtime_error(result.status().ToString());
    }

    arrow::ChunkedArrayPtr AssertArrayResultIsOk(const arrow::Result<arrow::Datum> &result);

    arrow::TablePtr AssertTableResultIsOk(const arrow::Result<arrow::Datum> &result);

    arrow::ArrayPtr AssertContiguousArrayResultIsOk(const arrow::Result<arrow::Datum> &result);

    template<typename ArrowScalarType>
    ArrowScalarType AssertCastScalarResultIsOk(const arrow::Result<arrow::Datum> &result) {
        if (result.ok()) {
            try {
                return result->scalar_as<ArrowScalarType>();
            } catch (const std::exception &e) {
                throw std::runtime_error(std::format("Failed to cast scalar to type {}: {}",
                                                     std::string{ArrowScalarType::TypeClass::type_name()}, e.what()));
            }
        }
        throw std::runtime_error(result.status().ToString());
    }
}
