//
// Created by adesola on 2/15/25.
//

#pragma once
#include "common/table_or_array.h"
#include "epoch_frame/aliases.h"
#include <arrow/compute/api.h>
#include <utility>


namespace epoch_frame {
    class MethodBase {

    public:
        explicit MethodBase(const TableComponent&  data);

    protected:
        [[nodiscard]] TableOrArray apply(std::string const &op, const arrow::compute::FunctionOptions *options = nullptr) const;

        [[nodiscard]] TableComponent apply(std::string const &op, const TableComponent &otherData) const;

        [[nodiscard]] TableOrArray apply(std::string const &op, const arrow::Datum &other, bool lhs = true) const;

        [[nodiscard]] TableOrArray rapply(std::string const &op, const arrow::Datum &other) const;

        arrow::TablePtr merge_index() const;

        TableComponent unzip_index(arrow::TablePtr const&) const;

    protected:
        const TableComponent& m_data;

        static constexpr const char * RESERVED_INDEX_NAME = "__RESERVED_INDEX__";
    };
}
