//
// Created by adesola on 2/13/25.
//

#include "dataframe_factory.h"
#include <arrow/api.h>


namespace epochframe {
    NDFrame make_dataframe(arrow::RecordBatchPtr const &data) {
        return NDFrame(data);
    }

    NDFrame make_dataframe(IndexPtr const &index, arrow::RecordBatchPtr const &data) {
        return NDFrame(index, data);
    }
}
