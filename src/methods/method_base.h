//
// Created by adesola on 2/15/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>


namespace epochframe {
    class MethodBase {

    public:
        MethodBase(TableComponent const &data): m_data(data) {}

    protected:
        arrow::TablePtr apply(std::string const &op, const arrow::compute::FunctionOptions *options = nullptr) const;

        TableComponent apply(std::string const &op, const TableComponent &otherData) const;

        arrow::TablePtr apply(std::string const &op, const Scalar &other, bool lhs = true) const;

        arrow::TablePtr rapply(std::string const &op, const Scalar &other) const {
            return apply(op, other, false);
        }

    private:
        TableComponent m_data;
    };
}
