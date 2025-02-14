//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/ndframe.h"


namespace epochframe {
    NDFrame make_series(arrow::ArrayPtr const &data, std::string const &name = "");

    NDFrame make_series(IndexPtr const &index, arrow::ArrayPtr const &data, std::string const &name = "");
}
