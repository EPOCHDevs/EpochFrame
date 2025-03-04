#pragma once
#include "epochframe/aliases.h"

#include "arrow/api.h"

#include <epoch_lab_shared/macros.h>


namespace epochframe {
class TableOrArray {
    public:
        explicit TableOrArray(arrow::Datum const& datum) : m_impl(datum) {
            AssertWithTraceFromStream(datum.kind() == arrow::Datum::TABLE || datum.kind() == arrow::Datum::CHUNKED_ARRAY, "Datum is not a table or chunked array");
        }

        explicit TableOrArray(arrow::TablePtr const& table) : m_impl(arrow::Datum{table}) {
            AssertWithTraceFromStream(table != nullptr, "Table is nullptr");
        }

        explicit TableOrArray(arrow::ChunkedArrayPtr const& chunked_array) : m_impl(arrow::Datum{chunked_array}) {
            AssertWithTraceFromStream(chunked_array != nullptr, "ChunkedArray is nullptr");
        }

        arrow::TablePtr table() const {
            return m_impl.table();
        }

        int64_t size() const {
            return m_impl.length();
        }

        template<typename T>
        std::shared_ptr<T> get() const {
            if constexpr (std::is_same_v<T, arrow::ChunkedArray>) {
                return m_impl.chunked_array();
            }
            else if constexpr (std::is_same_v<T, arrow::Table>) {
                return m_impl.table();
            }
            else {
                static_assert(std::is_same_v<T, arrow::Table> || std::is_same_v<T, arrow::ChunkedArray>, "Unsupported type");
            }
        }

        arrow::TablePtr get_table(std::string const& default_array="") const {
            return is_table() ? table() : arrow::Table::Make(arrow::schema({std::pair{default_array, chunked_array()->type()}}), {chunked_array()});
        }

        arrow::ChunkedArrayPtr chunked_array() const {
            return m_impl.chunked_array();
        }

        bool is_table() const {
            return m_impl.kind() == arrow::Datum::TABLE;
        }

        bool is_chunked_array() const {
            return m_impl.kind() == arrow::Datum::CHUNKED_ARRAY;
        }

        auto visit(auto && fn) const {
            return std::visit(fn, m_impl);
        }

        arrow::Datum datum() const {
            return m_impl;
        }

    private:
        arrow::Datum m_impl;
    };
}
