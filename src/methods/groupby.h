//
// Created by adesola on 1/20/25.
//

#pragma once
#include "epoch_frame/aliases.h"
#include "time_grouper.h"
#include <arrow/compute/api.h>
#include <common/asserts.h>
#include <epoch_frame/enums.h>
#include <epoch_frame/scalar.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>

namespace epoch_frame
{
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
    class Grouper
    {
      public:
        explicit Grouper(arrow::TablePtr                   table,
                         std::optional<TimeGrouperOptions> options = std::nullopt);
        virtual ~Grouper() = default;

        // Common interface
        Groups          groups();
        arrow::TablePtr table() const
        {
            return m_table;
        }
        const std::vector<arrow::FieldRef>& keys() const
        {
            return m_keys;
        }
        const std::vector<arrow::FieldRef>& fields() const
        {
            return m_fields;
        }
        virtual void makeGroups() = 0;

      protected:
        arrow::TablePtr              m_table;
        std::vector<arrow::FieldRef> m_keys;
        std::vector<arrow::FieldRef> m_fields;
        Groups                       m_groups;
        std::optional<TimeGrouper>   m_time_grouper;
        void                         makeGroups(arrow::ChunkedArrayVector const& keys);
    };

    // Specialization for grouping by key names
    class KeyGrouper : public Grouper
    {
      public:
        KeyGrouper(arrow::TablePtr table, std::vector<std::string> const& by,
                   std::optional<TimeGrouperOptions> options = std::nullopt);

      private:
        void makeGroups() override;
    };

    // Specialization for grouping by arrays
    class ArrayGrouper : public Grouper
    {
      public:
        ArrayGrouper(arrow::TablePtr table, arrow::ChunkedArrayVector const& by,
                     std::optional<TimeGrouperOptions> options = std::nullopt);

      private:
        arrow::ChunkedArrayVector m_arr;
        void                      makeGroups() override;
    };

    //-------------------------------------------------------------------------
    // Operation classes
    //-------------------------------------------------------------------------

    // Base class for group operations
    class GroupOperations
    {
      public:
        explicit GroupOperations(std::shared_ptr<Grouper> grouper);
        virtual ~GroupOperations() = default;

        // Common utilities
        DataFrame agg_table_to_dataFrame(arrow::TablePtr const& result) const;
        std::unordered_map<std::string, DataFrame>
        to_dataFrame_map(std::vector<std::string> const& agg_names, DataFrame const& result) const;

      protected:
        std::shared_ptr<Grouper> m_grouper;

        std::pair<arrow::ChunkedArrayPtr, arrow::TablePtr>
        filter_key(std::string const& name, arrow::TablePtr const& result) const;
    };

    // Specialization for aggregation operations
    class AggOperations : public GroupOperations
    {
      public:
        explicit AggOperations(std::shared_ptr<Grouper> grouper);

        [[nodiscard]] arrow::TablePtr
        apply_agg(std::string const&                                      agg_name,
                  const std::shared_ptr<arrow::compute::FunctionOptions>& option = nullptr) const;

        [[nodiscard]] arrow::TablePtr apply_agg(
            std::vector<std::string> const&                                      agg_names,
            const std::vector<std::shared_ptr<arrow::compute::FunctionOptions>>& options) const;

        [[nodiscard]] DataFrame
        agg(std::string const&                                      agg_name,
            const std::shared_ptr<arrow::compute::FunctionOptions>& option = nullptr) const;

        [[nodiscard]] std::unordered_map<std::string, DataFrame>
        agg(std::vector<std::string> const&                                      agg_names,
            const std::vector<std::shared_ptr<arrow::compute::FunctionOptions>>& options) const;
    };

    // Specialization for apply operations
    // TODO: Extend to Async
    class ApplyOperations : public GroupOperations
    {
      public:
        explicit ApplyOperations(DataFrame const& data, std::shared_ptr<Grouper> grouper,
                                 bool groupKeys);

        Series    apply(std::function<Scalar(DataFrame const&)> const& fn) const;
        Series    apply(std::function<Series(DataFrame const&)> const& fn) const;
        DataFrame apply(std::function<DataFrame(DataFrame const&)> const& fn) const;
        DataFrame apply(std::function<arrow::TablePtr(DataFrame const&)> const& fn) const;

      private:
        DataFrame const& m_data;
        bool             m_groupKeys;
        IndexPtr make_apply_index(IndexPtr const& newIndex, arrow::ScalarPtr const& groupKey) const;
    };

    //-------------------------------------------------------------------------
    // Main GroupBy classes (completely separate interfaces)
    //-------------------------------------------------------------------------

    // Class for aggregation operations - only has agg methods
#define MAKE_SCALAR_AGG_FUNCTION(name)                                                             \
    OutputType name(bool skip_nulls = true, uint32_t min_count = 0) const                          \
    {                                                                                              \
        return agg(#name, std::make_shared<arrow::compute::ScalarAggregateOptions>(skip_nulls,     \
                                                                                   min_count));    \
    }

    template <typename OutputType> class GroupByAgg
    {
      public:
        explicit GroupByAgg(std::shared_ptr<Grouper>       grouper,
                            std::shared_ptr<AggOperations> operations);
        virtual ~GroupByAgg() = default;

        // Aggregation methods only
        [[nodiscard]] OutputType
        agg(std::string const&                                      agg_name,
            const std::shared_ptr<arrow::compute::FunctionOptions>& option = nullptr) const;

        [[nodiscard]] std::unordered_map<std::string, OutputType>
        agg(std::vector<std::string> const&                                      agg_names,
            const std::vector<std::shared_ptr<arrow::compute::FunctionOptions>>& options) const;

        // Aggs
        MAKE_SCALAR_AGG_FUNCTION(all)
        MAKE_SCALAR_AGG_FUNCTION(any)
        MAKE_SCALAR_AGG_FUNCTION(approximate_median)

        OutputType count(arrow::compute::CountOptions::CountMode mode =
                             arrow::compute::CountOptions::CountMode::ONLY_VALID) const
        {
            return agg("count", std::make_shared<arrow::compute::CountOptions>(mode));
        }

        OutputType count_all() const
        {
            return agg("count", nullptr);
        }

        OutputType count_distinct(arrow::compute::CountOptions::CountMode mode =
                                      arrow::compute::CountOptions::CountMode::ONLY_VALID) const
        {
            return agg("count_distinct", std::make_shared<arrow::compute::CountOptions>(mode));
        }

        MAKE_SCALAR_AGG_FUNCTION(first)

        OutputType index(Scalar const& value) const
        {
            return agg("index", std::make_shared<arrow::compute::IndexOptions>(value.value()));
        }

        MAKE_SCALAR_AGG_FUNCTION(last)
        MAKE_SCALAR_AGG_FUNCTION(min)
        MAKE_SCALAR_AGG_FUNCTION(max)
        MAKE_SCALAR_AGG_FUNCTION(mean)
        MAKE_SCALAR_AGG_FUNCTION(product)

        OutputType quantile(double                                         q,
                            arrow::compute::QuantileOptions::Interpolation interpolation =
                                arrow::compute::QuantileOptions::Interpolation::LINEAR) const
        {
            return agg("quantile",
                       std::make_shared<arrow::compute::QuantileOptions>(q, interpolation));
        }

        OutputType stddev(int ddof = 0) const
        {
            return agg("stddev", std::make_shared<arrow::compute::VarianceOptions>(ddof));
        }

        MAKE_SCALAR_AGG_FUNCTION(sum)

        OutputType tdigest(double q, uint32_t delta = 100) const
        {
            return agg("tdigest", std::make_shared<arrow::compute::TDigestOptions>(q, delta));
        }

        OutputType variance(int ddof = 0, bool skip_nulls = true, uint64_t min_count = 0) const
        {
            return agg("variance", std::make_shared<arrow::compute::VarianceOptions>(
                                       ddof, skip_nulls, min_count));
        }

      private:
        std::shared_ptr<Grouper>       m_grouper;
        std::shared_ptr<AggOperations> m_operations;
    };

    // Class for apply operations - only has apply methods and access to groups
    class GroupByApply
    {
      public:
        explicit GroupByApply(std::shared_ptr<Grouper>         grouper,
                              std::shared_ptr<ApplyOperations> operations);
        virtual ~GroupByApply() = default;

        // Apply methods only
        Series    apply(std::function<Scalar(DataFrame const&)> const& fn) const;
        Series    apply(std::function<Series(DataFrame const&)> const& fn) const;
        DataFrame apply(std::function<DataFrame(DataFrame const&)> const& fn) const;
        DataFrame apply(std::function<arrow::TablePtr(DataFrame const&)> const& fn) const;

        // Access to underlying data - only needed for apply operations
        [[nodiscard]] Groups groups() const;

      private:
        std::shared_ptr<Grouper>         m_grouper;
        std::shared_ptr<ApplyOperations> m_operations;
    };

    extern template class GroupByAgg<DataFrame>;
    extern template class GroupByAgg<Series>;
    //-------------------------------------------------------------------------
    // Factory functions
    //-------------------------------------------------------------------------
    namespace factory::group_by
    {
        // For aggregation operations only
        template <typename OutputType>
        GroupByAgg<OutputType>
        make_agg_by_key(arrow::TablePtr table, std::vector<std::string> const& by,
                        std::optional<TimeGrouperOptions> options = std::nullopt);

        template <typename OutputType>
        GroupByAgg<OutputType>
        make_agg_by_array(arrow::TablePtr table, arrow::ChunkedArrayVector const& by,
                          std::optional<TimeGrouperOptions> options = std::nullopt);

        template <typename OutputType>
        GroupByAgg<OutputType> make_agg_by_index(DataFrame const&          table,
                                                 const TimeGrouperOptions& options);

        // For apply operations only
        GroupByApply make_apply_by_key(DataFrame const& table, std::vector<std::string> const& by,
                                       bool                              groupKeys,
                                       std::optional<TimeGrouperOptions> options = std::nullopt);
        GroupByApply make_apply_by_array(DataFrame const&                 table,
                                         arrow::ChunkedArrayVector const& by, bool groupKeys,
                                         std::optional<TimeGrouperOptions> options = std::nullopt);
        GroupByApply make_apply_by_index(DataFrame const& table, bool groupKeys,
                                         const TimeGrouperOptions& options);

        extern template GroupByAgg<DataFrame>
        make_agg_by_key<DataFrame>(arrow::TablePtr table, std::vector<std::string> const& by,
                                   std::optional<TimeGrouperOptions> options);
        extern template GroupByAgg<DataFrame>
        make_agg_by_array<DataFrame>(arrow::TablePtr table, arrow::ChunkedArrayVector const& by,
                                     std::optional<TimeGrouperOptions> options);
        extern template GroupByAgg<DataFrame>
        make_agg_by_index<DataFrame>(DataFrame const& table, const TimeGrouperOptions& options);

        extern template GroupByAgg<Series>
        make_agg_by_key<Series>(arrow::TablePtr table, std::vector<std::string> const& by,
                                std::optional<TimeGrouperOptions> options);
        extern template GroupByAgg<Series>
        make_agg_by_array<Series>(arrow::TablePtr table, arrow::ChunkedArrayVector const& by,
                                  std::optional<TimeGrouperOptions> options);
        extern template GroupByAgg<Series>
        make_agg_by_index<Series>(DataFrame const& table, const TimeGrouperOptions& options);
    } // namespace factory::group_by
} // namespace epoch_frame
