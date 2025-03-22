//
// Created by adesola on 1/20/25.
//

#include "datetime_index.h"
#include "factory/array_factory.h"


namespace epoch_frame {
    DateTimeIndex::DateTimeIndex(std::shared_ptr<arrow::TimestampArray> array,  std::string const& name) : ArrowIndex<true>(
            factory::array::make_array(std::move(array)), name, MonotonicDirection::Increasing) {}

    DateTimeIndex::DateTimeIndex(std::shared_ptr<arrow::Array> array, std::string const& name) : ArrowIndex<true>(std::move(array), name, MonotonicDirection::Increasing) {}

    IndexPtr DateTimeIndex::Make(std::shared_ptr<arrow::Array> array) const {
        return std::make_shared<DateTimeIndex>(std::move(array), name());
    }

    std::string DateTimeIndex::tz() const {
        return arrow_utils::get_tz(dtype());
    }
} // namespace epoch_frame
