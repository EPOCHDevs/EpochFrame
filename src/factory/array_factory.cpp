//
// Created by adesola on 1/20/25.
//

#include "array_factory.h"
#include "epochframe/scalar.h"


namespace epochframe::factory::array {
    arrow::ArrayPtr make_array(const arrow::ScalarVector &scalarVector,
                              std::shared_ptr<arrow::DataType> const &type) {
        auto result = arrow::MakeBuilder(type);
        AssertWithTraceFromFormat(result.ok(), "Failed to create builder");
        auto builder = result.MoveValueUnsafe();
        AssertStatusIsOk(builder->AppendScalars(std::move(scalarVector)));

        return AssertContiguousArrayResultIsOk(builder->Finish());
    }

    arrow::ChunkedArrayPtr make_chunked_array(const arrow::ChunkedArrayVector &arrowPtrList) {
        arrow::ArrayVector arrays;
        for (const auto &array: arrowPtrList) {
            auto chunks = array->chunks();
            arrays.insert(arrays.end(), chunks.begin(), chunks.end());
        }
        return AssertArrayResultIsOk(arrow::ChunkedArray::Make(std::move(arrays)));
    }

    arrow::ChunkedArrayPtr make_chunked_array(const arrow::Result<arrow::Datum> &datum) {
        return AssertArrayResultIsOk(datum);
    }

    arrow::ArrayPtr make_contiguous_array(const arrow::Result<arrow::Datum> &datum) {
        return AssertContiguousArrayResultIsOk(datum);
    }

    arrow::ChunkedArrayPtr make_chunked_array(const arrow::Datum &datum) {
        if (datum.is_array()) {
            return make_array(datum.make_array());
        }
        AssertWithTraceFromStream(datum.is_chunked_array(), "datum is not chunked array or array");
        return datum.chunked_array();
    }

    arrow::ArrayPtr make_contiguous_array(const arrow::ChunkedArrayPtr &chunked_array) {
        auto chunks = AssertArrayResultIsOk(arrow::Concatenate(chunked_array->chunks()))->chunks();
        AssertWithTraceFromStream(chunks.size() == 1, "datum is not contiguous array");
        return chunks[0];
    }

    arrow::ArrayPtr make_contiguous_array(const arrow::Datum &datum) {
        if (datum.is_array()) {
            return datum.make_array();
        }
        AssertWithTraceFromStream(datum.is_chunked_array(), "datum is not chunked array or array");
        return make_contiguous_array(datum.chunked_array());
    }

    arrow::ArrayPtr make_contiguous_array(const std::vector<Scalar> &set_array,
                                        std::shared_ptr<arrow::DataType> const &type) {
        arrow::ScalarVector scalars(set_array.size());
        std::ranges::transform(set_array, scalars.begin(), [](auto const &scalar) {
            return scalar.value();
        });

        return make_array(std::move(scalars), type);
    }

    arrow::Result<std::shared_ptr<arrow::Array>>
    array_to_struct_single_chunk(const std::vector<std::shared_ptr<arrow::Array>>& columns,
                                    const std::vector<std::shared_ptr<arrow::Field>>& fields) {
        // Safety checks (all columns must have same total length, same number of fields, etc)
        if (columns.size() != fields.size()) {
            return arrow::Status::Invalid(
                "Number of columns does not match number of fields");
        }
        if (columns.empty()) {
            return arrow::Status::Invalid("No columns supplied");
        }
        // Check total length consistency
        int64_t expected_length = columns[0]->length();
        for (int i = 1; i < static_cast<int>(columns.size()); i++) {
            if (columns[i]->length() != expected_length) {
                return arrow::Status::Invalid("All columns must have the same total length");
            }
        }

        // Make a StructArray
        auto struct_type = std::make_shared<arrow::StructType>(fields);
        return arrow::StructArray::Make(columns, fields);
    }
}
