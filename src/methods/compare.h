//
// Created by adesola on 2/13/25.
//

#pragma once
#include "method_base.h"


namespace epochframe {
    class Comparison : public MethodBase {

    public:
        explicit Comparison(TableComponent data): MethodBase(data) {}

        //==================
        // Comparison ops
        //==================
        TableComponent equal(const TableComponent &other) const {
            return apply("equal", other);
        }

        arrow::TablePtr equal(const Scalar &other) const {
            // NDFrame op scalar
            return apply("equal", other, true);
        }

        arrow::TablePtr requal(const Scalar &other) const {
            // scalar op NDFrame
            return rapply("equal", other);
        }

        TableComponent not_equal(const TableComponent &other) const {
            return apply("not_equal", other);
        }

        arrow::TablePtr not_equal(const Scalar &other) const {
            return apply("not_equal", other, true);
        }

        arrow::TablePtr rnot_equal(const Scalar &other) const {
            return rapply("not_equal", other);
        }

        TableComponent less(const TableComponent &other) const {
            return apply("less", other);
        }

        arrow::TablePtr less(const Scalar &other) const {
            return apply("less", other, true);
        }

        arrow::TablePtr rless(const Scalar &other) const {
            return rapply("less", other);
        }

        TableComponent less_equal(const TableComponent &other) const {
            return apply("less_equal", other);
        }

        arrow::TablePtr less_equal(const Scalar &other) const {
            return apply("less_equal", other, true);
        }

        arrow::TablePtr rless_equal(const Scalar &other) const {
            return rapply("less_equal", other);
        }

        TableComponent greater(const TableComponent &other) const {
            return apply("greater", other);
        }

        arrow::TablePtr greater(const Scalar &other) const {
            return apply("greater", other, true);
        }

        arrow::TablePtr rgreater(const Scalar &other) const {
            return rapply("greater", other);
        }

        TableComponent greater_equal(const TableComponent &other) const {
            return apply("greater_equal", other);
        }

        arrow::TablePtr greater_equal(const Scalar &other) const {
            return apply("greater_equal", other, true);
        }

        arrow::TablePtr rgreater_equal(const Scalar &other) const {
            return rapply("greater_equal", other);
        }

        //========================
        // Logical ops (and/or/xor)
        //========================
        TableComponent and_(const TableComponent &other) const {
            return apply("and", other);
        }

        arrow::TablePtr and_(const Scalar &other) const {
            return apply("and", other, true);
        }

        arrow::TablePtr rand_(const Scalar &other) const {
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

        arrow::TablePtr or_(const Scalar &other) const {
            return apply("or", other, true);
        }

        arrow::TablePtr ror_(const Scalar &other) const {
            return rapply("or", other);
        }

        TableComponent or_kleene(const TableComponent &other) const {
            return apply("or_kleene", other);
        }

        TableComponent xor_(const TableComponent &other) const {
            return apply("xor", other);
        }

        arrow::TablePtr xor_(const Scalar &other) const {
            return apply("xor", other, true);
        }

        arrow::TablePtr rxor_(const Scalar &other) const {
            return rapply("xor", other);
        }

        arrow::TablePtr invert() const {
            return apply("invert"); // or a simpler apply("invert") if you like
        }

    private:
        TableComponent m_data;
    };
}
