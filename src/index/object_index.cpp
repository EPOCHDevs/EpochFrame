//
// Created by adesola on 1/20/25.
//

#include "object_index.h"
#include "common/asserts.h"
#include "epoch_frame/factory/array_factory.h"

namespace epoch_frame
{
    ObjectIndex::ObjectIndex(std::shared_ptr<arrow::StringArray> array, std::string const& name)
        : ArrowIndex(factory::array::make_array(std::move(array)), name)
    {
        m_monotonic_direction = MonotonicDirection::NotMonotonic;
    }

    ObjectIndex::ObjectIndex(std::shared_ptr<arrow::Array> array, std::string const& name)
        : ObjectIndex(PtrCast<arrow::StringArray>(array), name)
    {
    }

    IndexPtr ObjectIndex::Make(std::shared_ptr<arrow::Array> array) const
    {
        return std::make_shared<ObjectIndex>(std::move(array), name());
    }
} // namespace epoch_frame
