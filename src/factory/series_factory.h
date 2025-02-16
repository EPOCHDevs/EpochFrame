//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/ndframe.h"


namespace epochframe {
    NDFrame make_series(arrow::ChunkedArrayPtr const &data, std::string const &name = "");

    NDFrame make_series(IndexPtr const &index, arrow::ChunkedArrayPtr const &data, std::string const &name = "");
}
