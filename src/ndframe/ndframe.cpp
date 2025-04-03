//
// NDFrame.cpp
// Created by adesola on 1/20/25.
//

#include "ndframe.h"
#include "common/arrow_compute_utils.h"
#include "common/asserts.h"
#include "common/series_or_scalar.h"
#include "common/table_or_array.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/table_factory.h"
#include "epoch_frame/frame_or_series.h"
#include "epoch_frame/index.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/series.h"
#include "index/arrow_index.h"
#include "methods/agg.h"
#include "methods/arith.h"
#include "methods/common_op.h"
#include "methods/compare.h"
#include "methods/select.h"
#include <arrow/api.h>
#include <common/methods_helper.h>
#include <epoch_core/macros.h>
#include <tabulate/table.hpp>
#include <tbb/parallel_for_each.h>

namespace epoch_frame
{

    //////////////////////////////////////////////////////////////////////////
    /// CONSTRUCTORS, DESTRUCTOR, ASSIGNMENT
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::NDFrame(IndexPtr const&                   index,
                                           std::shared_ptr<ArrowType> const& data)
    {
        AssertFromStream(index != nullptr, "IIndex cannot be null");
        if constexpr (std::is_same_v<ArrowType, arrow::Table>)
        {
            AssertFromStream(data != nullptr, "Table cannot be null");
            AssertFromStream(data->num_rows() == 0 ||
                                 data->num_rows() == static_cast<int64_t>(index->size()),
                             "Number of rows in RecordBatch must match index size."
                                 << data->num_rows() << " != " << index->size());
        }
        else if constexpr (std::is_same_v<ArrowType, arrow::ChunkedArray>)
        {
            AssertFromStream(data != nullptr, "Array cannot be null");
            AssertFromStream(
                data->length() == 0 || data->length() == static_cast<int64_t>(index->size()),
                "Array length must match index size." << data->length() << " != " << index->size());
        }

        this->m_index = index;
        this->m_table = data;

        if constexpr (std::is_same_v<ArrowType, arrow::Table>)
        {
            if (m_table->num_rows() == 0 && index->size() > 0)
            {
                m_table = factory::table::make_null_table(m_table->schema(), index->size());
            }
        }
        else if constexpr (std::is_same_v<ArrowType, arrow::ChunkedArray>)
        {
            if (m_table->length() == 0 && index->size() > 0)
            {
                m_table = factory::table::make_null_chunked_array(m_table->type(), index->size());
            }
        }

        this->m_tableComponent = std::make_shared<TableComponent>(TableComponent{index, data});
        this->m_arithOp        = std::make_shared<Arithmetic>(*m_tableComponent);
        this->m_compareOp      = std::make_shared<Comparison>(*m_tableComponent);
        this->m_commonOp       = std::make_shared<CommonOperations>(*m_tableComponent);
        this->m_select         = std::make_shared<Selections>(*m_tableComponent);
        this->m_agg            = std::make_shared<Aggregator>(*m_tableComponent);
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::NDFrame(ArrowPtrType const& data)
        : NDFrame(factory::index::from_range(factory::table::get_size(data)), data)
    {
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::NDFrame()
        : NDFrame(factory::index::from_range(0),
                  factory::table::make_empty_table_or_array<ArrowType>())
    {
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::NDFrame(TableComponent const& tableComponent)
        : NDFrame(tableComponent.first, tableComponent.second.get<ArrowType>())
    {
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::NDFrame(NDFrame const& other) = default;

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::NDFrame(NDFrame&& other) noexcept = default;

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>&
    NDFrame<ChildType, ArrowType>::operator=(NDFrame const& other) = default;

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>&
    NDFrame<ChildType, ArrowType>::operator=(NDFrame&& other) noexcept = default;

    template <class ChildType, class ArrowType> NDFrame<ChildType, ArrowType>::~NDFrame() = default;

    //////////////////////////////////////////////////////////////////////////
    /// GENERAL ATTRIBUTES
    //////////////////////////////////////////////////////////////////////////
    template <class ChildType, class ArrowType> Shape2D NDFrame<ChildType, ArrowType>::shape() const
    {
        if constexpr (std::is_same_v<ArrowType, arrow::Table>)
        {
            return {static_cast<unsigned long>(m_table->num_rows()),
                    static_cast<unsigned long>(m_table->num_columns())};
        }
        else if constexpr (std::is_same_v<ArrowType, arrow::ChunkedArray>)
        {
            return {static_cast<unsigned long>(m_table->length()), static_cast<unsigned long>(1)};
        }
    }

    template <class ChildType, class ArrowType> bool NDFrame<ChildType, ArrowType>::empty() const
    {
        return size() == 0;
    }

    template <class ChildType, class ArrowType> uint64_t NDFrame<ChildType, ArrowType>::size() const
    {
        return m_index->size();
    }

    //////////////////////////////////////////////////////////////////////////
    /// 1) BASIC UNARY OPS
    //////////////////////////////////////////////////////////////////////////
    template <class ChildType, class ArrowType> ChildType NDFrame<ChildType, ArrowType>::abs() const
    {
        return from_base(m_arithOp->abs());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator-() const
    {
        return from_base(m_arithOp->negate());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::sign() const
    {
        return from_base(m_arithOp->sign());
    }

    //////////////////////////////////////////////////////////////////////////
    /// 2) BASIC ARITHMETIC OPS
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator+(ChildType const& other) const
    {
        return from_base(m_arithOp->add(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator+(Scalar const& val) const
    {
        return from_base(m_arithOp->add(val.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::radd(Scalar const& val) const
    {
        return from_base(m_arithOp->radd(val.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator-(ChildType const& other) const
    {
        return from_base(m_arithOp->subtract(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator-(Scalar const& val) const
    {
        return from_base(m_arithOp->subtract(val.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rsubtract(Scalar const& val) const
    {
        return from_base(m_arithOp->rsubtract(val.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator*(ChildType const& other) const
    {
        return from_base(m_arithOp->multiply(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator*(Scalar const& val) const
    {
        return from_base(m_arithOp->multiply(val.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rmultiply(Scalar const& val) const
    {
        return from_base(m_arithOp->rmultiply(val.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator/(ChildType const& other) const
    {
        return from_base(m_arithOp->divide(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator/(Scalar const& val) const
    {
        return from_base(m_arithOp->divide(val.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rdivide(Scalar const& val) const
    {
        return from_base(m_arithOp->rdivide(val.value()));
    }

    //////////////////////////////////////////////////////////////////////////
    /// 3) EXPONENTIAL, POWER, SQRT, LOGS, ETC.
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType> ChildType NDFrame<ChildType, ArrowType>::exp() const
    {
        return from_base(m_arithOp->exp());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::expm1() const
    {
        return from_base(m_arithOp->expm1());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::power(ChildType const& other) const
    {
        return from_base(m_arithOp->power(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::power(Scalar const& val) const
    {
        return from_base(m_arithOp->power(val.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rpower(Scalar const& lhs) const
    {
        return from_base(m_arithOp->rpower(lhs.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::sqrt() const
    {
        return from_base(m_arithOp->sqrt());
    }

    template <class ChildType, class ArrowType> ChildType NDFrame<ChildType, ArrowType>::ln() const
    {
        return from_base(m_arithOp->ln());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::log10() const
    {
        return from_base(m_arithOp->log10());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::log1p() const
    {
        return from_base(m_arithOp->log1p());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::log2() const
    {
        return from_base(m_arithOp->log2());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::logb(ChildType const& other) const
    {
        return from_base(m_arithOp->logb(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rlogb(Scalar const& other) const
    {
        return from_base(m_arithOp->rlogb(other.value()));
    }

    //////////////////////////////////////////////////////////////////////////
    /// 4) BITWISE OPS
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::bitwise_and(ChildType const& other) const
    {
        return from_base(m_arithOp->bit_wise_and(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::bitwise_and(Scalar const& other) const
    {
        return from_base(m_arithOp->bit_wise_and(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rbitwise_and(Scalar const& other) const
    {
        return from_base(m_arithOp->rbit_wise_and(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::bitwise_not() const
    {
        return from_base(m_arithOp->bit_wise_not());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::bitwise_or(ChildType const& other) const
    {
        return from_base(m_arithOp->bit_wise_or(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::bitwise_or(Scalar const& other) const
    {
        return from_base(m_arithOp->bit_wise_or(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rbitwise_or(Scalar const& other) const
    {
        return from_base(m_arithOp->rbit_wise_or(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::bitwise_xor(ChildType const& other) const
    {
        return from_base(m_arithOp->bit_wise_xor(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::bitwise_xor(Scalar const& other) const
    {
        return from_base(m_arithOp->bit_wise_xor(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rbitwise_xor(Scalar const& other) const
    {
        return from_base(m_arithOp->rbit_wise_xor(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::shift_left(ChildType const& other) const
    {
        return from_base(m_arithOp->shift_left(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::shift_left(Scalar const& other) const
    {
        return from_base(m_arithOp->shift_left(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::shift_right(ChildType const& other) const
    {
        return from_base(m_arithOp->shift_right(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::shift_right(Scalar const& other) const
    {
        return from_base(m_arithOp->shift_right(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rshift_right(Scalar const& other) const
    {
        return from_base(m_arithOp->rshift_right(other.value()));
    }

    //////////////////////////////////////////////////////////////////////////
    /// 5) ROUNDING (Primitive arguments exposed in the API)
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::ceil() const
    {
        return from_base(m_arithOp->ceil());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::floor() const
    {
        return from_base(m_arithOp->floor());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::trunc() const
    {
        return from_base(m_arithOp->trunc());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::round(int                       ndigits,
                                                   arrow::compute::RoundMode round_mode) const
    {
        return from_base(m_arithOp->round(arrow::compute::RoundOptions{
            ndigits, static_cast<arrow::compute::RoundMode>(round_mode)}));
    }

    template <class ChildType, class ArrowType>
    ChildType
    NDFrame<ChildType, ArrowType>::round_to_multiple(double                    multiple,
                                                     arrow::compute::RoundMode round_mode) const
    {
        return from_base(m_arithOp->round_to_multiple(arrow::compute::RoundToMultipleOptions{
            multiple, static_cast<arrow::compute::RoundMode>(round_mode)}));
    }

    template <class ChildType, class ArrowType>
    ChildType
    NDFrame<ChildType, ArrowType>::round_binary(arrow::compute::RoundMode round_mode) const
    {
        return from_base(m_arithOp->round_binary(arrow::compute::RoundBinaryOptions{
            static_cast<arrow::compute::RoundMode>(round_mode)}));
    }

    //////////////////////////////////////////////////////////////////////////
    /// 6) TRIGONOMETRIC OPS
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType> ChildType NDFrame<ChildType, ArrowType>::cos() const
    {
        return from_base(m_arithOp->cos());
    }

    template <class ChildType, class ArrowType> ChildType NDFrame<ChildType, ArrowType>::sin() const
    {
        return from_base(m_arithOp->sin());
    }

    template <class ChildType, class ArrowType> ChildType NDFrame<ChildType, ArrowType>::tan() const
    {
        return from_base(m_arithOp->tan());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::acos() const
    {
        return from_base(m_arithOp->acos());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::asin() const
    {
        return from_base(m_arithOp->asin());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::atan() const
    {
        return from_base(m_arithOp->atan());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::atan2(ChildType const& other) const
    {
        return from_base(m_arithOp->atan2(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::atan2(Scalar const& other) const
    {
        return from_base(m_arithOp->atan2(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::ratan2(Scalar const& other) const
    {
        return from_base(m_arithOp->ratan2(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::sinh() const
    {
        return from_base(m_arithOp->sinh());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::cosh() const
    {
        return from_base(m_arithOp->cosh());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::tanh() const
    {
        return from_base(m_arithOp->tanh());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::acosh() const
    {
        return from_base(m_arithOp->acosh());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::asinh() const
    {
        return from_base(m_arithOp->asinh());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::atanh() const
    {
        return from_base(m_arithOp->atanh());
    }

    //////////////////////////////////////////////////////////////////////////
    /// 7) CUMULATIVE OPS (Using default options)
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType>
    arrow::compute::CumulativeOptions MakeCumulativeOptions(bool                         skip_nulls,
                                                            std::optional<double> const& start)
    {
        return start ? arrow::compute::CumulativeOptions{*start, skip_nulls}
                     : arrow::compute::CumulativeOptions{skip_nulls};
    }

    template <class ChildType, class ArrowType>
    ChildType
    NDFrame<ChildType, ArrowType>::cumulative_sum(bool                         skip_nulls,
                                                  std::optional<double> const& start) const
    {
        return from_base(m_arithOp->cumulative_sum(
            MakeCumulativeOptions<ChildType, ArrowType>(skip_nulls, start)));
    }

    template <class ChildType, class ArrowType>
    ChildType
    NDFrame<ChildType, ArrowType>::cumulative_prod(bool                         skip_nulls,
                                                   std::optional<double> const& start) const
    {
        return from_base(m_arithOp->cumulative_prod(
            MakeCumulativeOptions<ChildType, ArrowType>(skip_nulls, start)));
    }

    template <class ChildType, class ArrowType>
    ChildType
    NDFrame<ChildType, ArrowType>::cumulative_max(bool                         skip_nulls,
                                                  std::optional<double> const& start) const
    {
        return from_base(m_arithOp->cumulative_max(
            MakeCumulativeOptions<ChildType, ArrowType>(skip_nulls, start)));
    }

    template <class ChildType, class ArrowType>
    ChildType
    NDFrame<ChildType, ArrowType>::cumulative_min(bool                         skip_nulls,
                                                  std::optional<double> const& start) const
    {
        return from_base(m_arithOp->cumulative_min(
            MakeCumulativeOptions<ChildType, ArrowType>(skip_nulls, start)));
    }

    template <class ChildType, class ArrowType>
    ChildType
    NDFrame<ChildType, ArrowType>::cumulative_mean(bool                         skip_nulls,
                                                   std::optional<double> const& start) const
    {
        return from_base(m_arithOp->cumulative_mean(
            MakeCumulativeOptions<ChildType, ArrowType>(skip_nulls, start)));
    }

    //////////////////////////////////////////////////////////////////////////
    /// 8) INDEXING OPS
    //////////////////////////////////////////////////////////////////////////
    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::head(uint64_t n) const
    {
        return iloc(UnResolvedIntegerSliceBound{.stop = std::min(n, size())});
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::tail(uint64_t n) const
    {
        return iloc(UnResolvedIntegerSliceBound{
            .start = std::max(static_cast<int64_t>(size()) - static_cast<int64_t>(n), 0L)});
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::iloc(const Array& indexes) const
    {
        return from_base(m_select->itake(indexes.value(), arrow::compute::TakeOptions()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::iloc(const UnResolvedIntegerSliceBound& bound) const
    {
        auto [start, length, step] = resolve_integer_slice(bound, size());
        
        // Handle empty slices
        if (length == 0) {
            // Return an empty frame with the same index type and schema
            if constexpr (std::is_same_v<ArrowType, arrow::Table>) {
                return from_base(factory::index::from_range(0), 
                                 factory::table::make_empty_table(m_table->schema()));
            } else {
                return from_base(factory::index::from_range(0),
                                 factory::table::make_empty_chunked_array(m_table->type()));
            }
        }
        
        // The same slice_array function should handle both step=1 and other step values now
        return from_base(arrow_utils::integer_slice_index(*m_index, start, length, step),
                         arrow_utils::slice_array(m_table, start, length, step));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::loc(const Array& filter_or_labels) const
    {
        if (filter_or_labels.type()->id() == arrow::Type::BOOL)
        {
            AssertFromFormat(filter_or_labels.length() == static_cast<int64_t>(size()),
                             "Length of labels must match length of index");
            const auto chunked = std::make_shared<arrow::ChunkedArray>(filter_or_labels.value());
            return from_base(m_select->filter(chunked, arrow::compute::FilterOptions{}));
        }
        return from_base(m_select->take(filter_or_labels.value(), arrow::compute::TakeOptions{}));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::loc(const Series& filter_or_labels) const
    {
        AssertFromStream(filter_or_labels.index()->equals(m_index), "IIndex mismatch");
        if (filter_or_labels.array()->type()->id() == arrow::Type::BOOL)
        {
            return from_base(
                m_select->filter(filter_or_labels.array(), arrow::compute::FilterOptions{}));
        }
        return from_base(
            m_select->take(factory::array::make_contiguous_array(filter_or_labels.array()),
                           arrow::compute::TakeOptions{}));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::loc(const SliceType& label_slice) const
    {
        auto [start, end, _] = m_index->slice_locs(label_slice.first, label_slice.second);
        AssertFromStream(start <= end, "Start index must be less than end index");
        auto table_slice = arrow_utils::slice_array(m_table, start, end-start);
        auto index_slice = arrow_utils::integer_slice_index(*m_index, start, end-start);
        return from_base(std::move(index_slice), table_slice);
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::loc(const IndexPtr& newIndex) const
    {
        return from_base(m_select->take(newIndex, arrow::compute::TakeOptions{}));
    }

    template <class ChildType, class ArrowType>
    IndexPtr NDFrame<ChildType, ArrowType>::index() const
    {
        return m_index;
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::reindex(IndexPtr const& index,
                                                     const Scalar&   fillValue) const
    {
        return from_base(
            index,
            align_by_index(TableComponent{m_index, m_table}, index, fillValue).get<ArrowType>());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::set_index(IndexPtr const& index) const
    {
        return from_base(index, m_table);
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::where(const WhereConditionVariant& cond,
                                                   WhereOtherVariant const&     other) const
    {
        return from_base(m_select->where(cond, other));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::isin(const Array& values) const
    {
        return from_base(m_select->is_in(values.value()));
    }

    //////////////////////////////////////////////////////////////////////////
    /// 11) COMPARISON OPS
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator==(ChildType const& other) const
    {
        return from_base(m_compareOp->equal(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator==(Scalar const& other) const
    {
        return from_base(m_compareOp->equal(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::requal(Scalar const& other) const
    {
        return from_base(m_compareOp->requal(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType operator==(Scalar const& lhs, NDFrame<ChildType, ArrowType> const& rhs)
    {
        return rhs.from_base(rhs.m_compareOp->requal(lhs.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator!=(ChildType const& other) const
    {
        return from_base(m_compareOp->not_equal(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator!=(Scalar const& other) const
    {
        return from_base(m_compareOp->not_equal(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rnot_equal(Scalar const& other) const
    {
        return from_base(m_compareOp->rnot_equal(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator<(ChildType const& other) const
    {
        return from_base(m_compareOp->less(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator<(Scalar const& other) const
    {
        return from_base(m_compareOp->less(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rless(Scalar const& other) const
    {
        return from_base(m_compareOp->rless(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator<=(ChildType const& other) const
    {
        return from_base(m_compareOp->less_equal(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator<=(Scalar const& other) const
    {
        return from_base(m_compareOp->less_equal(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rless_equal(Scalar const& other) const
    {
        return from_base(m_compareOp->rless_equal(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator>(ChildType const& other) const
    {
        return from_base(m_compareOp->greater(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator>(Scalar const& other) const
    {
        return from_base(m_compareOp->greater(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rgreater(Scalar const& other) const
    {
        return from_base(m_compareOp->rgreater(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator>=(ChildType const& other) const
    {
        return from_base(m_compareOp->greater_equal(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator>=(Scalar const& other) const
    {
        return from_base(m_compareOp->greater_equal(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rgreater_equal(Scalar const& other) const
    {
        return from_base(m_compareOp->rgreater_equal(other.value()));
    }

    //////////////////////////////////////////////////////////////////////////
    /// 12) LOGICAL OPS
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator&&(ChildType const& other) const
    {
        return from_base(m_compareOp->and_(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator&&(Scalar const& other) const
    {
        return from_base(m_compareOp->and_(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rand(Scalar const& other) const
    {
        return from_base(m_compareOp->rand_(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator||(ChildType const& other) const
    {
        return from_base(m_compareOp->or_(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator||(Scalar const& other) const
    {
        return from_base(m_compareOp->or_(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::ror(Scalar const& other) const
    {
        return from_base(m_compareOp->ror_(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator^(ChildType const& other) const
    {
        return from_base(m_compareOp->xor_(*other.m_tableComponent));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator^(Scalar const& other) const
    {
        return from_base(m_compareOp->xor_(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::rxor(Scalar const& other) const
    {
        return from_base(m_compareOp->rxor_(other.value()));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::operator!() const
    {
        return from_base(m_compareOp->invert());
    }

    //////////////////////////////////////////////////////////////////////////
    /// 13) COMMON OPERATIONS
    //////////////////////////////////////////////////////////////////////////

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::is_finite() const
    {
        return from_base(m_commonOp->is_finite());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::is_inf() const
    {
        return from_base(m_commonOp->is_inf());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::is_null(bool nan_is_null) const
    {
        arrow::compute::NullOptions options{nan_is_null};
        return from_base(m_commonOp->is_null(options));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::is_valid() const
    {
        return from_base(m_commonOp->is_valid());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::true_unless_null() const
    {
        return from_base(m_commonOp->true_unless_null());
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::cast(arrow::DataTypePtr const& to_type,
                                                  bool                      safe) const
    {
        arrow::compute::CastOptions options{safe};
        options.to_type = to_type;
        return from_base(m_commonOp->cast(options));
    }

    //--------------------------------------------------------------------------
    // 13) Selection & Transform
    //--------------------------------------------------------------------------
    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::drop_null(DropMethod how, AxisType axis,
                                                       std::vector<std::string> const& subset,
                                                       bool ignore_index) const
    {
        return from_base(m_select->drop_null(how, axis, subset, ignore_index));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::drop(IndexPtr const& index) const
    {
        auto diff = m_index->difference(index);
        return loc(diff);
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::fillnull(Scalar const& value) const
    {
        return where(is_valid(), value);
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::bfill(AxisType axis) const
    {
        return from_base(TableComponent{m_index, m_select->fill_null_backward(axis)});
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::ffill(AxisType axis) const
    {
        return from_base(TableComponent{m_index, m_select->fill_null_forward(axis)});
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::sort_index(bool place_na_last, bool ascending) const
    {
        return from_base(m_select->sort_index(place_na_last, ascending));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::sort_values(std::vector<std::string> const& by,
                                                         bool place_na_last, bool ascending) const
    {
        return from_base(m_select->sort_values(by, place_na_last, ascending));
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::map(const std::function<Scalar(const Scalar&)>& func,
                                                 bool ignore_nulls) const
    {
        return from_base(TableOrArray(arrow_utils::map(m_table, func, ignore_nulls)));
    }

    //--------------------------------------------------------------------------
    // 14) Aggregation
    //--------------------------------------------------------------------------

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::agg(AxisType axis, std::string const& agg, bool skip_null) const
    {
        return m_agg->agg(axis, agg, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::agg(AxisType axis, std::string const& agg, bool skip_null,
                                       const arrow::compute::FunctionOptions& options) const
    {
        return m_agg->agg(axis, agg, skip_null, options).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    typename NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::all(AxisType axis, bool skip_null) const
    {
        return m_agg->all(axis, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType NDFrame<ChildType, ArrowType>::any(AxisType axis,
                                                                              bool skip_null) const
    {
        return m_agg->any(axis, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::approximate_median(AxisType axis, bool skip_null) const
    {
        return m_agg->approximate_median(axis, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::count_all(AxisType axis) const
    {
        arrow::compute::CountOptions options;
        options.mode = arrow::compute::CountOptions::ALL;
        return m_agg->count(options, axis).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::count_valid(AxisType axis) const
    {
        arrow::compute::CountOptions options;
        options.mode = arrow::compute::CountOptions::ONLY_VALID;
        return m_agg->count(options, axis).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::count_null(AxisType axis) const
    {
        arrow::compute::CountOptions options;
        options.mode = arrow::compute::CountOptions::ONLY_NULL;
        return m_agg->count(options, axis).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::first(AxisType axis, bool skip_null) const
    {
        return m_agg->first(axis, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType NDFrame<ChildType, ArrowType>::last(AxisType axis,
                                                                               bool skip_null) const
    {
        return m_agg->last(axis, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::index(Scalar const& scalar, AxisType axis) const
    {
        auto result = m_agg->index(scalar, axis).as<AggType>();

        if constexpr (std::is_same_v<AggType, Series>)
        {
            return result.where(result != Scalar(-1), Scalar{});
        }
        else
        {
            return (result == -(1_scalar)) ? Scalar{} : result;
        }
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType NDFrame<ChildType, ArrowType>::max(AxisType axis,
                                                                              bool skip_null) const
    {
        return m_agg->max(axis, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType NDFrame<ChildType, ArrowType>::min(AxisType axis,
                                                                              bool skip_null) const
    {
        return m_agg->min(axis, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType NDFrame<ChildType, ArrowType>::mean(AxisType axis,
                                                                               bool skip_null) const
    {
        return m_agg->mean(axis, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::mode(AxisType axis, bool skip_null, int64_t n) const
    {
        return m_agg->mode(axis, skip_null, n).as<ChildType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::product(AxisType axis, bool skip_null, uint32_t min_count) const
    {
        arrow::compute::ScalarAggregateOptions options{skip_null, min_count};
        return m_agg->product(options, axis).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::quantile(arrow::compute::QuantileOptions const& options,
                                            AxisType                               axis) const
    {
        return m_agg->quantile(options, axis).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::stddev(arrow::compute::VarianceOptions const& options,
                                          AxisType                               axis) const
    {
        return m_agg->stddev(options, axis).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType NDFrame<ChildType, ArrowType>::sum(AxisType axis,
                                                                              bool skip_null) const
    {
        return m_agg->sum(axis, skip_null).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::tdigest(arrow::compute::TDigestOptions const& options,
                                           AxisType                              axis) const
    {
        return m_agg->tdigest(options, axis).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    NDFrame<ChildType, ArrowType>::AggType
    NDFrame<ChildType, ArrowType>::variance(arrow::compute::VarianceOptions const& options,
                                            AxisType                               axis) const
    {
        return m_agg->variance(options, axis).as<AggType>();
    }

    template <class ChildType, class ArrowType>
    bool NDFrame<ChildType, ArrowType>::equals(ChildType const& other) const
    {
        return m_index->equals(other.m_index) && m_table->Equals(*other.m_table);
    }

    template <class ChildType, class ArrowType>
    const TableComponent& NDFrame<ChildType, ArrowType>::tableComponent() const
    {
        return *m_tableComponent;
    }

    template class NDFrame<Series, arrow::ChunkedArray>;
    template class NDFrame<DataFrame, arrow::Table>;

    template <class ChildType, class ArrowType>
    ChildType NDFrame<ChildType, ArrowType>::from_base(TableOrArray const& tableOrArray) const
    {
        if constexpr (std::is_same_v<arrow::Table, ArrowType>)
        {
            return from_base(m_index, tableOrArray.table());
        }
        else
        {
            return from_base(m_index, tableOrArray.chunked_array());
        }
    }

} // namespace epoch_frame
