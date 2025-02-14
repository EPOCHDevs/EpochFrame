//
// Created by adesola on 2/13/25.
//

#include "series_factory.h"
#include <arrow/api.h>


namespace epochframe {
    NDFrame make_series(arrow::ArrayPtr const &data, std::string const &name) {
        return NDFrame(arrow::RecordBatch::Make(arrow::schema({std::make_shared<arrow::Field>(name, data->type())}),
                                                data->length(), {data}));
    }

    NDFrame make_series(IndexPtr const &index, arrow::ArrayPtr const &data, std::string const &name) {
        return NDFrame(index,
                       arrow::RecordBatch::Make(arrow::schema({std::make_shared<arrow::Field>(name, data->type())}),
                                                data->length(), {data}));
    }
}
