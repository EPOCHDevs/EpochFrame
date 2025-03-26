//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/series.h"


namespace epoch_frame {
    template<typename ColumnT>
inline Series make_series(IndexPtr const &index, std::vector<ColumnT> const &data,
                       std::optional<std::string> const &name={}) {
        return Series(index, factory::array::make_array(data), name);
    }

    inline Series make_series(IndexPtr const &index, arrow::ChunkedArrayPtr const &data,
                       std::optional<std::string> const &name={}) {
        return Series(index, data, name);
    }
}
