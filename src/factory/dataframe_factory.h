//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/ndframe.h"


namespace epochframe {
    NDFrame make_dataframe(arrow::RecordBatchPtr const &data);

    NDFrame make_dataframe(IndexPtr const &index, arrow::RecordBatchPtr const &data);
}
