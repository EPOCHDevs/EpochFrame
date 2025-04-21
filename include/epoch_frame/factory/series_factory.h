//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epoch_core/ranges_to.h"
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include <arrow/type_traits.h>

namespace epoch_frame
{
    template <typename ColumnT>
    inline Series make_series(IndexPtr const& index, std::vector<ColumnT> const& data,
                              std::optional<std::string> const& name = {})
    {
        return Series(index, factory::array::make_array(data), name);
    }

    inline Series make_series_from_view(IndexPtr const& index, auto const& data,
                                        std::optional<std::string> const& name = {})
    {
        auto vec = data | epoch_core::ranges::to_vector_v;
        return Series(index, factory::array::make_array(vec), name);
    }

    template <typename ColumnT>
    inline Series make_series_from_scalar(IndexPtr const& index, std::vector<Scalar> const& data,
                                          std::optional<std::string> const& name = {})
    {
        return Series(
            index,
            factory::array::make_chunked_array(data, arrow::CTypeTraits<ColumnT>::type_singleton()),
            name);
    }

    inline Series make_series(IndexPtr const& index, arrow::ChunkedArrayPtr const& data,
                              std::optional<std::string> const& name = {})
    {
        return Series(index, data, name);
    }
} // namespace epoch_frame
