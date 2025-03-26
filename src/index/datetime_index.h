//
// Created by adesola on 1/20/25.
//

#pragma once
#include "arrow_index.h"


namespace epoch_frame {
    class DateTimeIndex : public ArrowIndex<true> {

    public:
        explicit DateTimeIndex(std::shared_ptr<arrow::TimestampArray> const& array, std::string const& name="");
        explicit DateTimeIndex(std::shared_ptr<arrow::Array> const& array, std::string const& name="");

        IndexPtr Make(std::shared_ptr<arrow::Array> array) const final;

        std::string tz() const;
    };
} // namespace epoch_frame
