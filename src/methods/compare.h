//
// Created by adesola on 2/13/25.
//

#pragma once
#include "method_base.h"


namespace epochframe {
    class Comparison : public MethodBase {

    public:
        explicit Comparison(TableComponent const& data): MethodBase(data) {}

        //==================
        // Comparison ops
        //==================
        TableComponent equal(const TableComponent &other) const {
            return apply("equal", other);
        }

        TableOrArray equal(const arrow::Datum &other) const {
            // NDFrame op scalar
            return apply("equal", other, true);
        }

        TableOrArray requal(const arrow::Datum &other) const {
            // scalar op NDFrame
            return rapply("equal", other);
        }

        TableComponent not_equal(const TableComponent &other) const {
            return apply("not_equal", other);
        }

        TableOrArray not_equal(const arrow::Datum &other) const {
            return apply("not_equal", other, true);
        }

        TableOrArray rnot_equal(const arrow::Datum &other) const {
            return rapply("not_equal", other);
        }

        TableComponent less(const TableComponent &other) const {
            return apply("less", other);
        }

        TableOrArray less(const arrow::Datum &other) const {
            return apply("less", other, true);
        }

        TableOrArray rless(const arrow::Datum &other) const {
            return rapply("less", other);
        }

        TableComponent less_equal(const TableComponent &other) const {
            return apply("less_equal", other);
        }

        TableOrArray less_equal(const arrow::Datum &other) const {
            return apply("less_equal", other, true);
        }

        TableOrArray rless_equal(const arrow::Datum &other) const {
            return rapply("less_equal", other);
        }

        TableComponent greater(const TableComponent &other) const {
            return apply("greater", other);
        }

        TableOrArray greater(const arrow::Datum &other) const {
            return apply("greater", other, true);
        }

        TableOrArray rgreater(const arrow::Datum &other) const {
            return rapply("greater", other);
        }

        TableComponent greater_equal(const TableComponent &other) const {
            return apply("greater_equal", other);
        }

        TableOrArray greater_equal(const arrow::Datum &other) const {
            return apply("greater_equal", other, true);
        }

        TableOrArray rgreater_equal(const arrow::Datum &other) const {
            return rapply("greater_equal", other);
        }

        //========================
        // Logical ops (and/or/xor)
        //========================
        TableComponent and_(const TableComponent &other) const {
            return apply("and", other);
        }

        TableOrArray and_(const arrow::Datum &other) const {
            return apply("and", other, true);
        }

        TableOrArray rand_(const arrow::Datum &other) const {
            return rapply("and", other);
        }

        TableComponent and_kleene(const TableComponent &other) const {
            return apply("and_kleene", other);
        }

        TableComponent and_not(const TableComponent &other) const {
            return apply("and_not", other);
        }

        TableComponent and_not_kleene(const TableComponent &other) const {
            return apply("and_not_kleene", other);
        }

        TableComponent or_(const TableComponent &other) const {
            return apply("or", other);
        }

        TableOrArray or_(const arrow::Datum &other) const {
            return apply("or", other, true);
        }

        TableOrArray ror_(const arrow::Datum &other) const {
            return rapply("or", other);
        }

        TableComponent or_kleene(const TableComponent &other) const {
            return apply("or_kleene", other);
        }

        TableComponent xor_(const TableComponent &other) const {
            return apply("xor", other);
        }

        TableOrArray xor_(const arrow::Datum &other) const {
            return apply("xor", other, true);
        }

        TableOrArray rxor_(const arrow::Datum &other) const {
            return rapply("xor", other);
        }

        TableOrArray invert() const {
            return apply("invert");
        }
    };
}
