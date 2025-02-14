//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <vector>
#include <arrow/api.h>


namespace epochframe {
    std::shared_ptr<arrow::ChunkedArray> make_chunked_array(arrow::RecordBatchPtr const &);

    arrow::RecordBatchPtr make_record_batch(arrow::ArrayVector const &chunks, uint64_t num_rows,
                                            std::shared_ptr<arrow::Schema> const &);
}
