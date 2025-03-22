//
// Created by adesola on 2/17/25.
//

#pragma once
#include "epoch_frame/dataframe.h"
#include "epoch_frame/series.h"
#include <variant>
#include "index/index.h"
#include "common/table_or_array.h"


namespace epoch_frame {
    class FrameOrSeries {
    public:
        FrameOrSeries() = default;
        FrameOrSeries(IndexPtr const& index, TableOrArray const& tableOrArray) {
            if (tableOrArray.is_table()) {
                m_impl = DataFrame(index, tableOrArray.table());
            } else {
                m_impl = Series(index, tableOrArray.chunked_array());
            }
        }

        FrameOrSeries(DataFrame frame) : m_impl(std::move(frame)) {}

        FrameOrSeries(Series series) : m_impl(std::move(series)) {}

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
            return visit([]<typename T>(T const& variant_) {
                if constexpr (std::same_as<T, DataFrame>) {
                    return variant_.table();
                } else {
                    return variant_.to_frame().table();
                }
            });
        }

        auto to_frame() const {
            return holds_alternative<DataFrame>(m_impl) ? frame() : DataFrame{index(), table()};
        }

        [[nodiscard]] TableOrArray table_or_array() const {
            return is_frame() ? TableOrArray{frame().table()} : TableOrArray{series().array()};
        }

        [[nodiscard]] uint64_t size() const {
            return std::visit([]<typename T0>(T0 const &item) -> uint64_t {
                  return item.size();
            }, m_impl);
        }

        bool operator==(FrameOrSeries const& other) const {
            return std::visit([&]<typename T0, typename T1>(const T0& lhs, const T1 & rhs) {
                if constexpr (std::same_as<T0, T1>) {
                    return lhs.equals(rhs);
                } else {
                    return false;
                }
            }, m_impl, other.m_impl);
        }

        friend std::ostream& operator<<(std::ostream& os, FrameOrSeries const& frameOrSeries) {
            if (frameOrSeries.is_frame()) {
                return os << frameOrSeries.frame();
            }
            return os << frameOrSeries.series();
        }

        auto visit(auto && fn, auto && ...args) const {
            return std::visit(fn, m_impl, (args).m_impl ... );
        }

        template<typename T>
        T as() const {
            return std::get<T>(m_impl);
        }

    private:
        std::variant<DataFrame, Series> m_impl;
    };
}
