//
// Created by adesola on 2/17/25.
//

#pragma once
#include "epochframe/dataframe.h"
#include "epochframe/series.h"
#include <variant>
#include "index/index.h"
#include "common/table_or_array.h"


namespace epochframe {
    class FrameOrSeries {
    public:
        FrameOrSeries() = default;
        explicit FrameOrSeries(DataFrame frame) : m_impl(std::move(frame)) {}

        explicit FrameOrSeries(Series series) : m_impl(std::move(series)) {}

        IndexPtr index() const {
            return std::visit([](auto const &item) {
                return item.index();
            }, m_impl);
        }

        DataFrame frame() const {
            return std::get<DataFrame>(m_impl);
        }

        Series series() const {
            return std::get<Series>(m_impl);
        }

        bool is_frame() const {
            return std::holds_alternative<DataFrame>(m_impl);
        }

        auto visit(auto && fn) const {
            return std::visit(fn, m_impl);
        }

        auto table() const {
            return visit([](auto &&variant_) {
                using T = std::decay_t<decltype(variant_)>;
                if constexpr (std::same_as<T, DataFrame>) {
                    return variant_.table();
                } else {
                    return variant_.to_frame().table();
                }
            });
        }

        TableOrArray table_or_array() const {
            return is_frame() ? TableOrArray{frame().table()} : TableOrArray{series().array()};
        }

    private:
        std::variant<DataFrame, Series> m_impl;
    };
}
