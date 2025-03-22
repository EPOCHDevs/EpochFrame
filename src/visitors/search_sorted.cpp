//
// Created by adesola on 2/14/25.
//

#include "search_sorted.h"


namespace epoch_frame {
    arrow::Status SearchSortedVisitor::Visit(const arrow::UInt64Array &arr) {
        // Extract the scalar's raw value
        auto val_scalar = std::static_pointer_cast<arrow::UInt64Scalar>(value_);
        uint64_t needle = val_scalar->value;

        // We'll build a small "access" function for comparing array[i] with 'needle'
        // ignoring nulls by treating them as "less than everything"? Or skipping them.
        // The user code in your snippet checks "if (!l) return true;" to handle null.
        // We'll define a simple vector of the non-null values and do lower_bound on that.
        std::vector<uint64_t> valid_vals;
        valid_vals.reserve(arr.length());  // max possible

        for (int64_t i = 0; i < arr.length(); i++) {
            if (!arr.IsNull(i)) {
                valid_vals.push_back(arr.Value(i));
            }
        }

        auto comp = [&](uint64_t lhs, uint64_t rhs) { return lhs < rhs; };

        if (side_ == SearchSortedSide::Left) {
            auto iter = std::lower_bound(valid_vals.begin(), valid_vals.end(), needle, comp);
            result_ = static_cast<uint64_t>(std::distance(valid_vals.begin(), iter));
        } else {
// side_ == Right
            auto iter = std::upper_bound(valid_vals.begin(), valid_vals.end(), needle, comp);
            result_ = static_cast<uint64_t>(std::distance(valid_vals.begin(), iter));
        }

        return arrow::Status::OK();
    }

    arrow::Status SearchSortedVisitor::Visit(const arrow::StringArray &arr) {
        // Convert the scalar to a string
        std::string needle = value_->ToString(); // or cast to StringScalar, etc.
        std::vector<std::string> valid_vals;
        valid_vals.reserve(arr.length());

        for (int64_t i = 0; i < arr.length(); i++) {
            if (!arr.IsNull(i)) {
                // We can get the i-th string
                auto view = arr.GetView(i);
                valid_vals.emplace_back(view);
            }
        }

        if (side_ == SearchSortedSide::Left) {
            auto iter = std::lower_bound(valid_vals.begin(), valid_vals.end(), needle);
            result_ = static_cast<uint64_t>(std::distance(valid_vals.begin(), iter));
        } else {
            auto iter = std::upper_bound(valid_vals.begin(), valid_vals.end(), needle);
            result_ = static_cast<uint64_t>(std::distance(valid_vals.begin(), iter));
        }

        return arrow::Status::OK();
    }

    arrow::Status SearchSortedVisitor::Visit(const arrow::TimestampArray &arr) {
        auto val_scalar = std::static_pointer_cast<arrow::TimestampScalar>(value_);
        int64_t needle = val_scalar->value; // underlying integer representation
        std::vector<int64_t> valid_vals;
        valid_vals.reserve(arr.length());

        for (int64_t i = 0; i < arr.length(); i++) {
            if (!arr.IsNull(i)) {
                valid_vals.push_back(arr.Value(i));
            }
        }

        if (side_ == SearchSortedSide::Left) {
            auto iter = std::lower_bound(valid_vals.begin(), valid_vals.end(), needle);
            result_ = std::distance(valid_vals.begin(), iter);
        } else {
            auto iter = std::upper_bound(valid_vals.begin(), valid_vals.end(), needle);
            result_ = std::distance(valid_vals.begin(), iter);
        }

        return arrow::Status::OK();
    }
}
