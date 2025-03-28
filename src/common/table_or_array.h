#pragma once
#include "epoch_frame/aliases.h"

#include "arrow/api.h"

#include <epoch_core/macros.h>


namespace epoch_frame {
class TableOrArray {
    public:
        explicit TableOrArray(arrow::Datum const& datum, std::optional<std::string> const& name = std::nullopt) : m_impl(datum) {
            AssertFromStream(datum.kind() == arrow::Datum::TABLE || datum.kind() == arrow::Datum::CHUNKED_ARRAY, "Datum is not a table or chunked array" << datum.kind());
            if (datum.kind() == arrow::Datum::TABLE) {
                const arrow::TablePtr table = this->table();
                if (table->num_columns() == 1 && name.has_value() && table->field(0)->name() == name.value()) {
                    m_impl = table->column(0);
                }
            }
        }

        explicit TableOrArray(arrow::TablePtr const& table) : m_impl(arrow::Datum{table}) {
            AssertFromStream(table != nullptr, "Table is nullptr");
        }

        explicit TableOrArray(arrow::ChunkedArrayPtr const& chunked_array) : m_impl(arrow::Datum{chunked_array}) {
            AssertFromStream(chunked_array != nullptr, "ChunkedArray is nullptr");
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
