//
// Created by adesola on 2/13/25.
//
#pragma once
#include "method_base.h"
#include "common/table_or_array.h"


namespace epochframe {
    class WindowOperation : public MethodBase {
    public:
        WindowOperation(const TableComponent& data) : MethodBase(data) {}

        TableComponent shift(int64_t periods) const;

        TableComponent pct_change(int64_t periods) const;

        TableComponent diff(int64_t periods) const;

        private:
        IndexPtr resolve_index(int64_t periods) const;
    };
}