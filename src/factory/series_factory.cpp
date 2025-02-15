//
// Created by adesola on 2/13/25.
//

#include "series_factory.h"
#include <arrow/api.h>


namespace epochframe {
    NDFrame make_series(arrow::ChunkedArrayPtr const &data, std::string const &name) {
        return NDFrame(arrow::Table::Make(arrow::schema({std::make_shared<arrow::Field>(name, data->type())}), {data},
                                          data->length()));
    }

    NDFrame make_series(IndexPtr const &index, arrow::ChunkedArrayPtr const &data, std::string const &name) {
        return NDFrame(index,
                       arrow::Table::Make(arrow::schema({std::make_shared<arrow::Field>(name, data->type())}), {data},
                                          data->length()));
    }
}
