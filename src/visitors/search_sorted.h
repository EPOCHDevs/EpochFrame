#pragma once

#include "epoch_frame/aliases.h"
#include "epoch_frame/enums.h"
#include <arrow/api.h>
#include <algorithm>        // for std::lower_bound, std::upper_bound
#include <stdexcept>
#include <string>


namespace epoch_frame {
    class SearchSortedVisitor : public arrow::ArrayVisitor {
    public:
        SearchSortedVisitor(const arrow::ScalarPtr &value,
                            SearchSortedSide side)
                : value_(value), side_(side), result_(0) {}

        // The "result" of the search
        uint64_t result() const { return result_; }

        // 1) Handle UInt64Array
        arrow::Status Visit(const arrow::UInt64Array &arr) override;

        // 2) Handle StringArray
        arrow::Status Visit(const arrow::StringArray &arr) override;

        // 3) Handle TimestampArray
        arrow::Status Visit(const arrow::TimestampArray &arr) override;

    private:
        arrow::ScalarPtr value_;
        SearchSortedSide side_;
        uint64_t result_;
    };
}
