//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>

namespace epochframe {
    class StructOperation {
    public:
        StructOperation(arrow::ArrayPtr const&);

        // structural transforms
        arrow::TablePtr list_value_length() const;
        arrow::ArrayPtr make_struct() const;

        arrow::ArrayPtr list_element(arrow::ArrayPtr const&) const;

        arrow::ArrayPtr list_flatten() const;
        arrow::ArrayPtr list_parent_indices() const;

        arrow::ArrayPtr list_slice(arrow::compute::ListSliceOptions const&) const;
        arrow::ArrayPtr map_lookup(arrow::compute::MapLookupOptions const&) const;
        arrow::ArrayPtr struct_field(arrow::compute::StructFieldOptions const&) const;

    private:
        arrow::ArrayPtr m_array;
    };
}
