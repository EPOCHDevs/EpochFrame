//
// Created by adesola on 1/20/25.
//

#pragma once
#include <epochframe/enums.h>
#include "epochframe/aliases.h"
#include <string>
#include <unordered_set>
#include <arrow/compute/api.h>
#include <common/asserts.h>
#include <epochframe/scalar.h>
#include <epochframe/series.h>
#include <memory>
#include <functional>

namespace epochframe {
    using Groups = std::vector<std::pair<Scalar, std::shared_ptr<arrow::UInt64Array>>>;

    // Forward declarations
    class Grouper;
    class GroupOperations;
    class AggOperations;
    class ApplyOperations;

    //-------------------------------------------------------------------------
    // Grouper classes
    //-------------------------------------------------------------------------

    // Base class for all groupers
    class Grouper {
    public:
        explicit Grouper(arrow::TablePtr table);
        virtual ~Grouper() = default;

        // Common interface
        Groups groups();
        arrow::TablePtr table() const { return m_table; }
        const std::vector<arrow::FieldRef>& keys() const { return m_keys; }
        const std::vector<arrow::FieldRef>& fields() const { return m_fields; }
        virtual void makeGroups() = 0;

    protected:
        arrow::TablePtr m_table;
        std::vector<arrow::FieldRef> m_keys;
        std::vector<arrow::FieldRef> m_fields;
        Groups m_groups;

        void makeGroups(arrow::ChunkedArrayVector const& keys);
    };

    // Specialization for grouping by key names
    class KeyGrouper : public Grouper {
    public:
        KeyGrouper(arrow::TablePtr table, std::vector<std::string> const &by);

    private:
        void makeGroups() override;
    };

    // Specialization for grouping by arrays
    class ArrayGrouper : public Grouper {
    public:
        ArrayGrouper(arrow::TablePtr table, arrow::ChunkedArrayVector const &by);

    private:
        arrow::ChunkedArrayVector m_arr;
        void makeGroups() override;
    };

    //-------------------------------------------------------------------------
    // Operation classes
    //-------------------------------------------------------------------------

    // Base class for group operations
    class GroupOperations {
    public:
        explicit GroupOperations(std::shared_ptr<Grouper> grouper);
        virtual ~GroupOperations() = default;

        // Common utilities
        DataFrame agg_table_to_dataFrame(arrow::TablePtr const& result) const;
        std::unordered_map<std::string, DataFrame> to_dataFrame_map(
            std::vector<std::string> const& agg_names,
            DataFrame const& result) const;

    protected:
        std::shared_ptr<Grouper> m_grouper;

        std::pair<arrow::ChunkedArrayPtr, arrow::TablePtr> filter_key(std::string const& name, arrow::TablePtr const& result) const;
    };

    // Specialization for aggregation operations
    class AggOperations : public GroupOperations {
    public:
        explicit AggOperations(std::shared_ptr<Grouper> grouper);

        [[nodiscard]] arrow::TablePtr apply_agg(std::string const& agg_name,
            const std::shared_ptr<arrow::compute::FunctionOptions>& option = nullptr) const;

        [[nodiscard]] arrow::TablePtr apply_agg(
            std::vector<std::string> const& agg_names,
            const std::vector<std::shared_ptr<arrow::compute::FunctionOptions>>& options) const;

        [[nodiscard]] arrow::TablePtr apply_agg(
            std::vector<arrow::compute::Aggregate> const& aggregates) const;

        [[nodiscard]] DataFrame agg(std::string const& agg_name,
            const std::shared_ptr<arrow::compute::FunctionOptions>& option = nullptr) const;

        [[nodiscard]] std::unordered_map<std::string, DataFrame> agg(
            std::vector<std::string> const& agg_names,
            const std::vector<std::shared_ptr<arrow::compute::FunctionOptions>>& options) const;
    };

    // Specialization for apply operations
    // TODO: Extend to Async
    class ApplyOperations : public GroupOperations {
    public:
        explicit ApplyOperations(DataFrame const& data, std::shared_ptr<Grouper> grouper, bool groupKeys);

        Series apply(std::function<Scalar(DataFrame const &)> const& fn) const;
        Series apply(std::function<Series(DataFrame const &)> const& fn) const;
        DataFrame apply(std::function<DataFrame(DataFrame const &)> const& fn) const;

    private:
        DataFrame const& m_data;
        bool m_groupKeys;
        IndexPtr make_apply_index(IndexPtr const& newIndex, arrow::ScalarPtr const& groupKey) const;
    };

    //-------------------------------------------------------------------------
    // Main GroupBy classes (completely separate interfaces)
    //-------------------------------------------------------------------------

    // Class for aggregation operations - only has agg methods
    class GroupByAgg {
    public:
        explicit GroupByAgg(std::shared_ptr<Grouper> grouper, std::shared_ptr<AggOperations> operations);
        virtual ~GroupByAgg() = default;

        // Aggregation methods only
        [[nodiscard]] DataFrame agg(std::string const& agg_name,
            const std::shared_ptr<arrow::compute::FunctionOptions>& option = nullptr) const;

        [[nodiscard]] std::unordered_map<std::string, DataFrame> agg(
            std::vector<std::string> const& agg_names,
            const std::vector<std::shared_ptr<arrow::compute::FunctionOptions>>& options) const;

    private:
        std::shared_ptr<Grouper> m_grouper;
        std::shared_ptr<AggOperations> m_operations;
    };

    // Class for apply operations - only has apply methods and access to groups
    class GroupByApply {
    public:
        explicit GroupByApply(std::shared_ptr<Grouper> grouper, std::shared_ptr<ApplyOperations> operations);
        virtual ~GroupByApply() = default;

        // Apply methods only
        Series apply(std::function<Scalar(DataFrame const &)> const& fn) const;
        Series apply(std::function<Series(DataFrame const &)> const& fn) const;
        DataFrame apply(std::function<DataFrame(DataFrame const &)> const& fn) const;

        // Access to underlying data - only needed for apply operations
        [[nodiscard]] Groups groups() const;
    private:
        std::shared_ptr<Grouper> m_grouper;
        std::shared_ptr<ApplyOperations> m_operations;
    };

    //-------------------------------------------------------------------------
    // Factory functions
    //-------------------------------------------------------------------------
    namespace factory::group_by {
        // For aggregation operations only
        GroupByAgg make_agg_by_key(arrow::TablePtr table, std::vector<std::string> const &by);
        GroupByAgg make_agg_by_array(arrow::TablePtr table, arrow::ChunkedArrayVector const &by);

        // For apply operations only
        GroupByApply make_apply_by_key(DataFrame const& table, std::vector<std::string> const &by, bool groupKeys);
        GroupByApply make_apply_by_array(DataFrame const&  table, arrow::ChunkedArrayVector const &by, bool groupKeys);
    }
}
