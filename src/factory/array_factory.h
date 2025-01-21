//
// Created by adesola on 1/20/25.
//

#pragma once
#include <arrow/api.h>
#include <span>
#include <ranges>
#include "common_utils/asserts.h"

namespace epochframe::factory::array {

    template<class T>
    std::shared_ptr<arrow::Array> MakeArray(const auto& begin_, const auto& end_){

        typename arrow::CTypeTraits<T>::BuilderType builder;
        AssertStatusIsOk(builder.Reserve(std::distance(begin_, end_)));

        if constexpr ( std::is_floating_point_v<T>) {
            auto flagsView = std::span<const T>(begin_, end_) | std::views::transform([](auto && x) { return !std::isnan(x); });
            AssertStatusIsOk(builder.AppendValues(begin_, end_, flagsView.begin()));
        }
        else if constexpr (std::is_same_v<T, std::string>) {
            for(auto it = begin_; it != end_; ++it) {
                AssertStatusIsOk(builder.Append(*it));
            }
        }
        else {
            AssertStatusIsOk(builder.AppendValues(begin_, end_));
        };
        return AssertResultIsOk(builder.Finish());
    }

    template<class CType>
    std::shared_ptr<arrow::Array> MakeArray(const std::vector<CType> &values) {
        return MakeArray<CType>(values.begin(), values.end());
    }

    template<class CType>
    std::shared_ptr<arrow::Array> MakeArray(const std::span<const CType> &values) {
        return MakeArray<CType>(values.begin(), values.end());
    }
}
