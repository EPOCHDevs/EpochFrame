//
// Created by adesola on 1/20/25.
//

#pragma once
#include "arrow_index.h"


namespace epochframe {

    class StructIndex : public ArrowIndex<false> {

    public:
        using value_type = void;
        using array_type = arrow::StructArray;
        explicit StructIndex(std::shared_ptr<arrow::StructArray> array, std::string const& name="");
        explicit StructIndex(std::shared_ptr<arrow::Array> array, std::string const& name="");

        IndexPtr Make(std::shared_ptr<arrow::Array> array) const final;
    };
} // namespace epochframe
