//
// Created by adesola on 1/20/25.
//

#pragma once
#include "epochframe/aliases.h"
#include "arrow/compute/api.h"
#include "arrow/chunked_array.h"
#include "common/asserts.h"


namespace epochframe::vector {
    inline arrow::ArrayPtr unique(arrow::ArrayPtr const & array) {
        return AssertContiguousArrayResultIsOk(arrow::compute::CallFunction("unique", {array}));
    }

    inline arrow::ArrayPtr indices_nonzero(arrow::ArrayPtr const & array) {
        return AssertContiguousArrayResultIsOk(arrow::compute::CallFunction("indices_nonzero", {array}));
    }

    inline arrow::ArrayPtr partition_nth_indices(arrow::ArrayPtr const & array, arrow::compute::PartitionNthOptions const &options) {
        return AssertContiguousArrayResultIsOk(arrow::compute::CallFunction("partition_nth_indices", {array}, &options));
    }

    // TODO:
    // inline arrow::ArrayPtr rank(arrow::ArrayPtr const & array, arrow::compute::RankOptions const &options) {
    //     return AssertContiguousArrayResultIsOk(arrow::compute::CallFunction("rank", {array}, &options));
    // }
    //
    // inline arrow::ArrayPtr select_k_unstable(arrow::ArrayPtr const & array, arrow::compute::SelectKOptions const &options) const {
    //     return AssertContiguousArrayResultIsOk(arrow::compute::CallFunction("select_k_unstable", {array}, &options));
    // }
}
