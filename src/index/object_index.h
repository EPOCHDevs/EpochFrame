//
// Created by adesola on 1/20/25.
//

#pragma once
#include "arrow_index.h"


namespace epochframe {

    class ObjectIndex : public ArrowIndex<false> {

    public:
        using value_type = std::string;
        using array_type = arrow::StringArray;
        explicit ObjectIndex(std::shared_ptr<arrow::StringArray> array, std::string const& name="");
        explicit ObjectIndex(std::shared_ptr<arrow::Array> array, std::string const& name="");

        IndexPtr Make(std::shared_ptr<arrow::Array> array) const final;
    };
} // namespace epochframe
