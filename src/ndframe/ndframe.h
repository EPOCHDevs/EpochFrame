#pragma once
#include "epoch_frame/aliases.h"
#include "epoch_frame/enums.h"
#include "epoch_frame/integer_slice.h"
#include "epoch_frame/scalar.h"
#include <functional>
#include <string>
#include <vector>
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "methods/window.h"

// Forward declarations and type aliases
namespace epoch_frame {
    using IndexPtr = std::shared_ptr<class IIndex>;

    template<class ChildType, class ArrowType>
    class NDFrame {
    public:
        using ArrowPtrType = std::shared_ptr<ArrowType>;
        using AggType = std::conditional_t<std::is_same_v<ArrowType, arrow::Table>, class Series, class Scalar>;
        static constexpr bool is_table = std::is_same_v<ArrowType, arrow::Table>;
        // ------------------------------------------------------------------------
        // Constructors / Destructor / Assignment
        // ------------------------------------------------------------------------
        NDFrame();

        explicit NDFrame(ArrowPtrType const &data);

        NDFrame(IndexPtr const &index, ArrowPtrType const &data);

        NDFrame(TableComponent const &tableComponent);

        NDFrame(NDFrame const &other);

        NDFrame(NDFrame &&other) noexcept;

        NDFrame &operator=(NDFrame const &other);

        NDFrame &operator=(NDFrame &&other) noexcept;

        virtual ~NDFrame();

        // ------------------------------------------------------------------------
        // General Attributes
        // ------------------------------------------------------------------------
        IndexPtr index() const;

        virtual ChildType add_prefix(const std::string &prefix) const = 0;

        virtual ChildType add_suffix(const std::string &suffix) const = 0;

        [[nodiscard]] Shape2D shape() const;

        [[nodiscard]] bool empty() const;

        [[nodiscard]] uint64_t size() const;

        //--------------------------------------------------------------------------
        // 1) Basic unary ops
        //--------------------------------------------------------------------------
        ChildType abs() const;

        ChildType operator-() const;

        ChildType sign() const;

        // ------------------------------------------------------------------------
        // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
        // ------------------------------------------------------------------------
        ChildType operator+(ChildType const &other) const;

        ChildType operator+(Scalar const &val) const;

        ChildType radd(Scalar const &other) const;

        ChildType operator-(ChildType const &other) const;

        ChildType operator-(Scalar const &val) const;

        ChildType rsubtract(Scalar const &other) const;

        ChildType operator*(ChildType const &other) const;

        ChildType operator*(Scalar const &val) const;

        ChildType rmultiply(Scalar const &other) const;

        ChildType operator/(ChildType const &other) const;

        ChildType operator/(Scalar const &val) const;

        ChildType rdivide(Scalar const &other) const;

        //--------------------------------------------------------------------------
        // 3) Exponential, Power, sqrt, logs, trig
        //--------------------------------------------------------------------------
        ChildType exp() const;

        ChildType expm1() const;

        ChildType power(ChildType const &other) const;

        ChildType power(Scalar const &val) const;

        ChildType rpower(Scalar const &other) const;

        ChildType sqrt() const;

        ChildType ln() const;

        ChildType log10() const;

        ChildType log1p() const;

        ChildType log2() const;

        ChildType logb(ChildType const &other) const;

        ChildType rlogb(Scalar const &other) const;

        //--------------------------------------------------------------------------
        // 4) Bitwise ops
        //--------------------------------------------------------------------------
        ChildType bitwise_and(ChildType const &other) const;

        ChildType bitwise_and(Scalar const &other) const;

        ChildType rbitwise_and(Scalar const &other) const;

        ChildType bitwise_not() const;

        ChildType bitwise_or(ChildType const &other) const;

        ChildType bitwise_or(Scalar const &other) const;

        ChildType rbitwise_or(Scalar const &other) const;

        ChildType bitwise_xor(ChildType const &other) const;

        ChildType bitwise_xor(Scalar const &other) const;

        ChildType rbitwise_xor(Scalar const &other) const;

        ChildType shift_left(ChildType const &other) const;

        ChildType shift_left(Scalar const &other) const;

        ChildType rshift_left(Scalar const &other) const;

        ChildType shift_right(ChildType const &other) const;

        ChildType shift_right(Scalar const &other) const;

        ChildType rshift_right(Scalar const &other) const;

        //--------------------------------------------------------------------------
        // 5) Rounding
        // Instead of accepting full compute options, we expose primitive arguments.
        //--------------------------------------------------------------------------
        ChildType ceil() const;

        ChildType floor() const;

        ChildType trunc() const;

        ChildType round(int ndigits = 0, arrow::compute::RoundMode round_mode = arrow::compute::RoundMode::HALF_TO_EVEN) const;

        ChildType round_to_multiple(double multiple = 1,
                                  arrow::compute::RoundMode round_mode = arrow::compute::RoundMode::HALF_TO_EVEN) const;

        ChildType round_binary(arrow::compute::RoundMode round_mode = arrow::compute::RoundMode::HALF_TO_EVEN) const;

        //--------------------------------------------------------------------------
        // 6) Trigonometric ops
        //--------------------------------------------------------------------------
        ChildType cos() const;

        ChildType sin() const;

        ChildType tan() const;

        ChildType acos() const;

        ChildType asin() const;

        ChildType atan() const;

        ChildType atan2(ChildType const &other) const;

        ChildType atan2(Scalar const &other) const;

        ChildType ratan2(Scalar const &other) const;

        ChildType sinh() const;

        ChildType cosh() const;

        ChildType tanh() const;

        ChildType acosh() const;

        ChildType asinh() const;

        ChildType atanh() const;

        //--------------------------------------------------------------------------
        // 7) Cumulative operations (using default options internally)
        //--------------------------------------------------------------------------
        ChildType cumulative_sum(bool skip_nulls = true, std::optional<double> const &start = std::nullopt) const;

        ChildType cumulative_prod(bool skip_nulls = true, std::optional<double> const &start = std::nullopt) const;

        ChildType cumulative_mean(bool skip_nulls = true, std::optional<double> const &start = std::nullopt) const;

        ChildType cumulative_max(bool skip_nulls = true, std::optional<double> const &start = std::nullopt) const;

        ChildType cumulative_min(bool skip_nulls = true, std::optional<double> const &start = std::nullopt) const;

        //--------------------------------------------------------------------------
        // 8) Indexing ops
        //--------------------------------------------------------------------------
        ChildType head(uint64_t n = 10) const;

        ChildType tail(uint64_t n = 10) const;

        ChildType iloc(const Array &indexes) const;

        // ChildType iloc(const std::vector<int64_t> &indexes) const;

        ChildType iloc(const UnResolvedIntegerSliceBound &indexes) const;

        ChildType loc(const Array &labels) const;

        ChildType loc(const Series &filter_or_labels) const;

        ChildType reindex(IndexPtr const &index,
                        const Scalar &fillValue = Scalar{}) const;

        ChildType set_index(IndexPtr const &index) const;

        ChildType loc(const SliceType &label_slice) const;

        ChildType loc(const IndexPtr &newIndex) const;

        ChildType where(const WhereConditionVariant &cond, WhereOtherVariant const &other) const;

        ChildType isin(const Array &values) const;

        //--------------------------------------------------------------------------
        // 10) Comparison ops
        //--------------------------------------------------------------------------
        ChildType operator==(ChildType const &other) const;

        ChildType operator==(Scalar const &other) const;

        ChildType requal(Scalar const &other) const;

        ChildType operator!=(ChildType const &other) const;

        ChildType operator!=(Scalar const &other) const;

        ChildType rnot_equal(Scalar const &other) const;

        ChildType operator<(ChildType const &other) const;

        ChildType operator<(Scalar const &other) const;

        ChildType rless(Scalar const &other) const;

        ChildType operator<=(ChildType const &other) const;

        ChildType operator<=(Scalar const &other) const;

        ChildType rless_equal(Scalar const &other) const;

        ChildType operator>(ChildType const &other) const;

        ChildType operator>(Scalar const &other) const;

        ChildType rgreater(Scalar const &other) const;

        ChildType operator>=(ChildType const &other) const;

        ChildType operator>=(Scalar const &other) const;

        ChildType rgreater_equal(Scalar const &other) const;

        //--------------------------------------------------------------------------
        // 11) Logical ops (and/or/xor)
        //--------------------------------------------------------------------------
        ChildType operator&&(ChildType const &other) const;

        ChildType operator&&(Scalar const &other) const;

        ChildType rand(Scalar const &other) const;

        ChildType operator||(ChildType const &other) const;

        ChildType operator||(Scalar const &other) const;

        ChildType ror(Scalar const &other) const;

        ChildType operator^(ChildType const &other) const;

        ChildType operator^(Scalar const &other) const;

        ChildType rxor(Scalar const &other) const;

        ChildType operator!() const;

        //--------------------------------------------------------------------------
        // 12) Common Operations
        //--------------------------------------------------------------------------
        ChildType is_finite() const;

        ChildType is_inf() const;

        ChildType is_null(bool nan_is_null = true) const;  // expose as bool instead of full options
        ChildType is_valid() const;

        ChildType true_unless_null() const;

        ChildType cast(arrow::DataTypePtr const &to_type, bool safe = true) const; // expose target type as a string

        //--------------------------------------------------------------------------
        // 13) Selection & Transform
        //--------------------------------------------------------------------------
        ChildType drop_null(DropMethod how = DropMethod::Any,
                        AxisType axis = AxisType::Row,
                        std::vector<std::string> const &subset = {},
                        bool ignore_index = false) const;

        ChildType drop(IndexPtr const& index) const;

        ChildType fillnull(Scalar const& value) const;

        ChildType bfill(AxisType axis = AxisType::Row) const;

        ChildType ffill(AxisType axis = AxisType::Row) const;

        ChildType sort_index(bool place_na_last=true, bool ascending=true) const;

        ChildType sort_values(std::vector<std::string> const& by, bool place_na_last=true, bool ascending=true) const;

        /**
         * @brief Apply a function to each element in the NDFrame
         *
         * This applies a function to each scalar value in the NDFrame and returns
         * a new NDFrame with the results.
         *
         * @param func A function that takes a Scalar and returns a Scalar
         * @param ignore_nulls Optional. If true, nulls are not passed to the function
         * @return A new NDFrame with the results
         */
        ChildType map(const std::function<Scalar(const Scalar&)>& func, bool ignore_nulls = false) const;

        //--------------------------------------------------------------------------
        // 14) Aggregation
        //--------------------------------------------------------------------------
        AggType agg(AxisType axis, std::string const& agg, bool skip_null = true) const;
        AggType agg(AxisType axis, std::string const& agg, bool skip_null, const arrow::compute::FunctionOptions& options) const;

        AggType all(AxisType axis=AxisType::Row, bool skip_null=true) const;
        AggType any(AxisType axis=AxisType::Row, bool skip_null=true) const;
        AggType approximate_median(AxisType axis=AxisType::Row, bool skip_null=true) const;
        AggType count_all(AxisType axis=AxisType::Row) const;
        AggType count_valid(AxisType axis=AxisType::Row) const;
        AggType count_null(AxisType axis=AxisType::Row) const;
        AggType first(AxisType axis=AxisType::Row, bool skip_null=true) const;
        AggType last(AxisType axis=AxisType::Row, bool skip_null=true) const;
        AggType index(Scalar const &scalar, AxisType axis=AxisType::Row) const;
        AggType max(AxisType axis=AxisType::Row, bool skip_null=true) const;
        AggType min(AxisType axis=AxisType::Row, bool skip_null=true) const;
        AggType mean(AxisType axis=AxisType::Row, bool skip_null=true) const;
        ChildType mode(AxisType axis=AxisType::Row, bool skip_null=true, int64_t n=1) const;
        AggType product(AxisType axis=AxisType::Row, bool skip_null=true, uint32_t min_count=0) const;
        AggType quantile(arrow::compute::QuantileOptions const &options, AxisType axis=AxisType::Row) const;
        AggType stddev(arrow::compute::VarianceOptions const &options, AxisType axis=AxisType::Row) const;
        AggType sum(AxisType axis=AxisType::Row, bool skip_null=true) const;
        AggType tdigest(arrow::compute::TDigestOptions const &options, AxisType axis=AxisType::Row) const;
        AggType variance(arrow::compute::VarianceOptions const &options, AxisType axis=AxisType::Row) const;

        bool equals(ChildType const&, arrow::EqualOptions const& options=arrow::EqualOptions::Defaults()) const;

        const TableComponent& tableComponent() const;

        ChildType from_base(TableOrArray const &tableOrArray) const;

        virtual ChildType from_base(TableComponent const &tableComponent) const = 0;

        EWMWindowOperations<is_table> ewm_agg(const class EWMWindowOptions& options) const {
            return EWMWindowOperations<is_table>(options, dynamic_cast<const ChildType&>(*this));
        }

        // Assignments

    protected:
        IndexPtr m_index;
        ArrowPtrType m_table;
        std::shared_ptr<TableComponent> m_tableComponent;
        std::shared_ptr<class Arithmetic> m_arithOp;
        std::shared_ptr<class Comparison> m_compareOp;
        std::shared_ptr<class CommonOperations> m_commonOp;
        std::shared_ptr<class Selections> m_select;
        std::shared_ptr<class Aggregator> m_agg;
        std::shared_ptr<class WindowOperation> m_windowOp;
        virtual ChildType from_base(IndexPtr const &index, ArrowPtrType const &table) const = 0;
    };

    extern template class NDFrame<Series, arrow::ChunkedArray>;
    extern template class NDFrame<DataFrame, arrow::Table>;

} // namespace epoch_frame
