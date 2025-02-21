//
// Created by adesola on 2/13/25.
//

#include "dataframe_factory.h"
#include <arrow/api.h>
#include "common/table_or_array.h"


namespace epochframe {
    DataFrame make_dataframe(arrow::TablePtr const &data) {
        return DataFrame(data);
    }

    DataFrame make_dataframe(IndexPtr const &index, arrow::TablePtr const &data) {
        return DataFrame(index, data);
    }
}
