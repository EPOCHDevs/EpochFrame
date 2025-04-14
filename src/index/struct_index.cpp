//
// Created by adesola on 1/20/25.
//

#include "struct_index.h"
#include "common/asserts.h"
#include "epoch_frame/factory/array_factory.h"

namespace epoch_frame
{
    StructIndex::StructIndex(std::shared_ptr<arrow::StructArray> array, std::string const& name)
        : ArrowIndex(factory::array::make_array(std::move(array)), name)
    {
        m_monotonic_direction = MonotonicDirection::NotMonotonic;
    }

    StructIndex::StructIndex(std::shared_ptr<arrow::Array> array, std::string const& name)
        : StructIndex(std::dynamic_pointer_cast<arrow::StructArray>(std::move(array)), name)
    {
    }

    IndexPtr StructIndex::Make(std::shared_ptr<arrow::Array> array) const
    {
        return std::make_shared<StructIndex>(std::move(array), name());
    }
} // namespace epoch_frame
