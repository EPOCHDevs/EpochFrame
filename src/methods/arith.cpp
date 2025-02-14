//
// Created by adesola on 2/13/25.
//

#include "arith.h"
#include <arrow/compute/api.h>
#include "methods_helper.h"
#include <epoch_lab_shared/macros.h>
#include <utility>


namespace epochframe {
    Arithmetric::Arithmetric(TableComponent data) : m_data(std::move(data)) {}

    arrow::RecordBatchPtr Arithmetric::apply(std::string const &op) const {
        auto result = arrow::compute::CallFunction(op, {make_chunked_array(m_data.second)});
        AssertWithTraceFromStream(result.ok(), std::string("Error in applying arithmetic operation:\n")
                << result.status().ToString());
        return make_record_batch(result->chunks(), m_data.second->num_rows(), m_data.second->schema());
    }

    arrow::RecordBatchPtr Arithmetric::apply(const std::string &op, const arrow::compute::FunctionOptions &) const {
        return arrow::RecordBatchPtr();
    }

    arrow::RecordBatchPtr Arithmetric::apply(const std::string &op, const TableComponent &otherData) const {
        return arrow::RecordBatchPtr();
    }
}
