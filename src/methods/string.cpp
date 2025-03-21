//
// Created by adesola on 2/13/25.
//

#include "string.h"
#include "common/arrow_compute_utils.h"

namespace epochframe {
    template<bool is_array>
    StringOperation<is_array>::StringOperation(const Type& data) : m_data(data) {
        AssertFromStream(m_data.type()->id() == arrow::Type::STRING, "data is not a string");
    }

    template<bool is_array>
    typename StringOperation<is_array>::Type StringOperation<is_array>::call_function(const std::string& name, const arrow::compute::FunctionOptions* options) const {
        auto result = arrow_utils::call_unary_compute(m_data.value(), name, options);
        if constexpr (is_array) {
            return Type(result.make_array());
        } else {
            return Type(result.scalar());
        }
    }

    template class StringOperation<true>;
    template class StringOperation<false>;
}
