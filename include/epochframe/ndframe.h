//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/scalar.h"
#include "methods/arith.h"


namespace epochframe {
    class NDFrame {

    public:
        NDFrame();

        explicit NDFrame(arrow::TablePtr const &data);

        NDFrame(IndexPtr const &index, arrow::TablePtr const &data);

        NDFrame(TableComponent const & tableComponent):NDFrame(tableComponent.first, tableComponent.second) {}

        NDFrame(NDFrame const &other) = default;

        NDFrame(NDFrame &&other) = default;

        NDFrame &operator=(NDFrame const &other) = default;

        NDFrame &operator=(NDFrame &&other) = default;

        virtual ~NDFrame() = default;

        // General Attributes
        void add_prefix(const std::string &prefix);

        void add_suffix(const std::string &suffix);

        // Indexing Operation
        NDFrame head(uint64_t n = 10) const;

        NDFrame tail(uint64_t n = 10) const;

        NDFrame loc(uint64_t index_label) const;

        NDFrame loc(const Scalar &index_label) const;

        NDFrame loc(const std::vector<bool> &filter) const;

        NDFrame loc(const std::vector<uint64_t> &labels) const;

        NDFrame loc(const arrow::Array &labels) const;

        NDFrame loc(const IntgerSliceType &labels) const;

        NDFrame loc(const SliceType &labels) const;

        Scalar at(uint64_t index_label) const;

        Scalar at(const Scalar &index_label, const std::string &column) const;

        Scalar at(const Scalar &index_label) const;

        NDFrame at(const std::string &column) const;

        IndexPtr index() const;

        void set_index(IndexPtr const &index);

        // General Attributes
        [[nodiscard]] Shape2D shape() const;

        [[nodiscard]] bool empty() const;

        [[nodiscard]] uint64_t size() const;

        // arithmetric
        NDFrame operator+(NDFrame const &other) const {
            return NDFrame{m_arithOp->add(other.m_data)};
        }

        // Serialization
        friend std::ostream & operator<<(std::ostream &os, NDFrame const&);
    private:
        TableComponent m_data;
        std::shared_ptr<Arithmetric> m_arithOp;

    protected:
        Scalar iat(int64_t row, std::string const &col);

    };
} // namespace epochframe
