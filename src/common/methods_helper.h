//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <vector>
#include <arrow/api.h>
#include <epoch_lab_shared/macros.h>
#include "common/exceptions.h"
#include <optional>
#include "epochframe/dataframe.h"

namespace epochframe {
    arrow::TablePtr add_column(const arrow::TablePtr &table, const std::string &name,
                               const arrow::ChunkedArrayPtr &array);

    arrow::TablePtr add_column(const arrow::TablePtr &table, const std::string &name,
                               const arrow::ArrayPtr &array);

    std::pair<TableComponent, TableComponent>
    make_table(arrow::ArrayVector const &chunks, uint64_t num_rows,
               std::shared_ptr<arrow::Schema> const &);

    std::tuple<IndexPtr, TableOrArray, TableOrArray>
    align_by_index_and_columns(const TableComponent &left_table_,
                               const TableComponent &right_table_);

    TableOrArray
    align_by_index(const TableComponent &left_table_,
                   const IndexPtr &,
                   const Scalar & scalar = Scalar{});

    // Perform the actual column-wise binary op once two RecordBatches
    // are aligned by row.
    arrow::TablePtr
    unsafe_binary_op(const TableOrArray &left_rb,
                     const TableOrArray &right_rb,
                     const std::string &op);

    bool has_unique_type(const arrow::SchemaPtr &schema);

    struct DictionaryEncodeResult {
        std::shared_ptr<arrow::Int32Array> indices;
        arrow::ArrayPtr array;
    };

    DictionaryEncodeResult dictionary_encode(const arrow::ArrayPtr &array);

    struct ValueCountResult {
        std::shared_ptr<arrow::Int64Array> counts;
        arrow::ArrayPtr values;
    };

    ValueCountResult value_counts(const arrow::ArrayPtr &array);

    arrow::ChunkedArrayPtr get_column_by_name(arrow::Table const &, std::string const &name);

    arrow::FieldPtr get_field_by_name(arrow::Schema const &, std::string const &name);

    arrow::SchemaPtr slice_schema(arrow::Schema const &, std::vector<std::string> const &column_names);

    template<typename T>
    static bool can_cast_to_int64_from_timestamp(arrow::ArrayPtr const &array) {
        return (arrow::CTypeTraits<T>::type_singleton()->id() == arrow::Type::INT64 &&
                array->type()->id() == arrow::Type::TIMESTAMP);
    }

    template<typename T>
    std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType> get_view(arrow::ArrayPtr const &array) {
        AssertWithTraceFromFormat(array != nullptr, "array is null");

        if (arrow::CTypeTraits<T>::type_singleton()->id() != array->type()->id()) {
            if (!can_cast_to_int64_from_timestamp<T>(array)) {
                throw std::runtime_error(fmt::format("Type mismatch: expected {}, got {}", array->type()->ToString(),
                                                     arrow::CTypeTraits<T>::type_singleton()->ToString()));
            }
        }
        return std::static_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(array);
    }

    template<typename T>
    std::vector<T> get_values(arrow::ArrayPtr const &array) {
        AssertWithTraceFromFormat(array != nullptr, "array is null");

        auto requested_type = arrow::CTypeTraits<T>::type_singleton();
        if (requested_type != array->type()) {
            if (!can_cast_to_int64_from_timestamp<T>(array)) {
                throw RawArrayCastException(requested_type, array->type());
            }
        }

        if (array->null_count() != 0 && (array->type_id() != arrow::Type::DOUBLE)) {
            throw std::runtime_error("values() called on null array");
        }

        std::vector<T> result;
        if constexpr (!arrow::is_fixed_width_type<typename arrow::CTypeTraits<T>::ArrowType>::value) {
            result.resize(array->length());
            auto viewArray = get_view<T>(array);
            std::ranges::transform(*viewArray, result.begin(), [](auto const &s) {
                return (*s);
            });
        } else if constexpr (std::same_as<T, bool>) {
            result.reserve(array->length());
            std::shared_ptr<arrow::BooleanArray> viewArray = get_view<bool>(array);
            std::transform(viewArray->begin(), viewArray->end(), std::back_inserter(result), [](auto const &s) {
                return *s;
            });
        } else {
            result.resize(array->length());
            auto realArray = std::static_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(array);

            auto N = realArray->length();
            auto ptr = realArray->raw_values();

            memcpy(result.data(), ptr, sizeof(T) * N);
        }
        return result;
    }

    DataFrame get_variant_column(DataFrame const &, const LocColArgumentVariant &);

    DataFrame get_variant_row(DataFrame const &, const LocRowArgumentVariant &);

    Series get_variant_row(Series const &, const LocRowArgumentVariant &);

    std::pair<bool, IndexPtr> combine_index(std::vector<FrameOrSeries> const& objs, bool intersect);

    arrow::TablePtr make_table_from_array_schema(arrow::Table const& table, arrow::ChunkedArrayPtr const &array);

    arrow::ChunkedArrayPtr get_array(arrow::Table const &table, std::string const &name, arrow::Scalar const &default_value);
}
