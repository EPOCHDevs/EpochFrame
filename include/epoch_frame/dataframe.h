#pragma once
#include "epoch_frame/aliases.h"
#include "epoch_frame/scalar.h"
#include "methods/groupby.h"
#include "methods/window.h"
#include "ndframe/ndframe.h"
#include <vector>
#include <unordered_map>

// Forward declarations and type aliases
namespace epoch_frame
{
    class DataFrame : public NDFrame<DataFrame, arrow::Table>
    {
      public:
        // ------------------------------------------------------------------------
        // Constructors / Destructor / Assignment
        // ------------------------------------------------------------------------
        DataFrame() = default;

        explicit DataFrame(arrow::TablePtr const& data);

        DataFrame(IndexPtr const& index, arrow::TablePtr const& data);

        // ------------------------------------------------------------------------
        // General Attributes
        // ------------------------------------------------------------------------
        DataFrame add_prefix(const std::string& prefix) const override
        {
            return add_prefix_or_suffix(prefix, true);
        }

        DataFrame add_suffix(const std::string& suffix) const override
        {
            return add_prefix_or_suffix(suffix, false);
        }

        DataFrame rename(std::unordered_map<std::string, std::string> const& by) const;

        using NDFrame<DataFrame, arrow::Table>::set_index;
        DataFrame set_index(std::string const&) const;

        /**
         * @brief Configuration options for SQL operations
         *
         * Controls how temporary .arrows files are managed during SQL operations.
         * These options are used by the managed SQL interface.
         */
        struct SQLOptions {
            std::string arrow_file_dir = "/tmp/epochframe_sql/";  ///< Directory for temporary .arrows files
            bool cleanup = true;                                  ///< Auto-delete files after query completion
            bool debug = false;                                   ///< Print file paths and SQL transformations for debugging
            std::string file_prefix = "table_";                  ///< Prefix for generated temporary filenames
        };

        /**
         * @brief Execute SQL query on this DataFrame (managed approach)
         *
         * Automatically creates temporary .arrows files and registers this DataFrame as "df" in SQL.
         * Uses DuckDB's nanoarrow extension with proper filtering support.
         *
         * @param sql SQL query string. Reference this DataFrame as "df"
         * @param index_name Optional name for index column (default: "")
         * @return DataFrame containing query results
         *
         * @example
         * ```cpp
         * DataFrame df = load_data();
         * auto result = df.query("SELECT * FROM df WHERE value > 100");
         * auto top5 = df.query("SELECT name, value FROM df ORDER BY value DESC LIMIT 5");
         * ```
         */
        DataFrame query(const std::string& sql, const std::string& index_name = "") const;

        /**
         * @brief Execute SQL query with multiple DataFrames (managed approach)
         *
         * Automatically creates temporary .arrows files for all DataFrames and registers them
         * with their specified names in SQL. This DataFrame is available as "df".
         *
         * @param sql SQL query string. Reference DataFrames by their map keys
         * @param tables Map of table names to DataFrames
         * @param index_name Optional name for index column (default: "")
         * @return DataFrame containing query results
         *
         * @example
         * ```cpp
         * DataFrame sales = load_sales();
         * DataFrame products = load_products();
         * auto result = sales.query(
         *     "SELECT s.customer, p.name, s.amount FROM df s JOIN products p ON s.product_id = p.id",
         *     {{"products", products}}
         * );
         * ```
         */
        DataFrame query(const std::string& sql,
                       const std::unordered_map<std::string, DataFrame>& tables,
                       const std::string& index_name = "") const;

        /**
         * @brief Execute SQL query without DataFrame context (static method)
         *
         * Executes SQL directly on DuckDB. Useful for queries that don't reference DataFrames
         * or when working with existing database tables/views.
         *
         * @param sql SQL query string
         * @return DataFrame containing query results
         *
         * @example
         * ```cpp
         * auto result = DataFrame::sql("SELECT 1 as id, 'hello' as message");
         * auto info = DataFrame::sql("SHOW TABLES");
         * ```
         */
        static DataFrame sql(const std::string& sql);

        /**
         * @brief Execute SQL query with multiple DataFrames (static method)
         *
         * Automatically creates temporary .arrows files for all DataFrames and registers them
         * with their specified names in SQL.
         *
         * @param sql SQL query string. Reference DataFrames by their map keys
         * @param tables Map of table names to DataFrames
         * @param index_name Optional name for index column (default: "index")
         * @return DataFrame containing query results
         *
         * @example
         * ```cpp
         * DataFrame sales = load_sales();
         * DataFrame products = load_products();
         * auto result = DataFrame::sql(
         *     "SELECT s.customer, p.name FROM sales s JOIN products p ON s.product_id = p.id",
         *     {{"sales", sales}, {"products", products}}
         * );
         * ```
         */
        static DataFrame sql(const std::string& sql,
                           const std::unordered_map<std::string, DataFrame>& tables,
                           const std::string& index_name = "index");

        /**
         * @brief Execute SQL query directly on .arrows files (simple approach)
         *
         * User manages .arrows files manually. Use read_arrow('file.arrows') in SQL to reference files.
         * This approach gives full control over file lifecycle and is ideal for persistent workflows.
         *
         * @param sql SQL query string using read_arrow() function calls
         * @return DataFrame containing query results
         *
         * @example
         * ```cpp
         * // First save DataFrames to .arrows files
         * sales_df.write_arrows("sales.arrows");
         * products_df.write_arrows("products.arrows");
         *
         * // Then query directly
         * auto result = DataFrame::sql_simple(
         *     "SELECT s.customer, p.name "
         *     "FROM read_arrow('sales.arrows') s "
         *     "JOIN read_arrow('products.arrows') p ON s.product_id = p.id"
         * );
         *
         * // Clean up when done
         * std::filesystem::remove("sales.arrows");
         * std::filesystem::remove("products.arrows");
         * ```
         */
        static DataFrame sql_simple(const std::string& sql);

        /**
         * @brief Write DataFrame to .arrows file for use with sql_simple()
         *
         * Serializes DataFrame to Arrow IPC stream format (.arrows file) that can be read
         * by DuckDB's read_arrow() function. Uses existing serialization infrastructure.
         *
         * @param file_path Path where .arrows file will be created
         * @param include_index Whether to include the index as a column (default: true)
         *
         * @throws std::runtime_error if file cannot be written
         *
         * @example
         * ```cpp
         * DataFrame df = load_data();
         * df.write_arrows("my_data.arrows");  // Include index
         * df.write_arrows("data_no_index.arrows", false);  // Exclude index
         *
         * // Later use with sql_simple
         * auto result = DataFrame::sql_simple("SELECT * FROM read_arrow('my_data.arrows')");
         * ```
         */
        void write_arrows(const std::string& file_path, bool include_index = true) const;

        //--------------------------------------------------------------------------
        // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
        //--------------------------------------------------------------------------
        using NDFrame::operator+;
        using NDFrame::operator-;
        using NDFrame::operator*;
        using NDFrame::operator/;

        DataFrame operator+(Series const& other) const;

        DataFrame operator-(Series const& other) const;

        DataFrame operator*(Series const& other) const;

        DataFrame operator/(Series const& other) const;

        //--------------------------------------------------------------------------
        // 3) Exponential, Power, sqrt, logs, trig
        //--------------------------------------------------------------------------
        using NDFrame::logb;
        using NDFrame::power;

        DataFrame power(Series const& other) const;
        DataFrame logb(Series const& other) const;

        //--------------------------------------------------------------------------
        // 4) Bitwise ops
        //--------------------------------------------------------------------------
        using NDFrame::bitwise_and;
        using NDFrame::bitwise_or;
        using NDFrame::bitwise_xor;
        using NDFrame::shift_left;
        using NDFrame::shift_right;

        DataFrame bitwise_and(Series const& other) const;
        DataFrame bitwise_or(Series const& other) const;
        DataFrame bitwise_xor(Series const& other) const;
        DataFrame shift_left(Series const& other) const;
        DataFrame shift_right(Series const& other) const;

        //--------------------------------------------------------------------------
        // 8) Indexing ops
        //--------------------------------------------------------------------------
        using NDFrame::drop;
        using NDFrame::drop_null;
        using NDFrame::iloc; // for the IntegerSlice version
        using NDFrame::loc;  // for the array/Series/callable/slice versions

        // Keep the DataFrame-specific overloads
        Series          iloc(int64_t row) const;
        Scalar          iloc(int64_t row, std::string const& col) const;
        Series          operator[](const std::string& column) const;
        DataFrame       operator[](const StringVector& columns) const;
        DataFrame       operator[](const Array& array) const;
        DataFrame       operator[](const StringVectorCallable& callable) const;
        Series          loc(const Scalar& index_label) const;
        DataFrame       safe_loc(const Scalar& index_label) const;
        Scalar          loc(const Scalar& index_label, const std::string& column) const;
        Series          safe_loc(const Scalar& index_label, const std::string& column) const;
        DataFrame       loc(const DataFrameToSeriesCallable&) const;
        Series          loc(const Scalar&, const LocColArgumentVariant&) const;
        DataFrame       loc(const LocRowArgumentVariant&, const LocColArgumentVariant&) const;
        Series          loc(const LocRowArgumentVariant&, const std::string&) const;
        DataFrame       sort_columns(bool ascending = true) const;
        arrow::ArrayPtr flatten() const;

        //--------------------------------------------------------------------------
        // 10) Comparison ops
        //--------------------------------------------------------------------------
        using NDFrame::operator==;
        using NDFrame::operator!=;
        using NDFrame::operator<;
        using NDFrame::operator<=;
        using NDFrame::operator>;
        using NDFrame::operator>=;

        //--------------------------------------------------------------------------
        // 11) Logical ops
        //--------------------------------------------------------------------------
        using NDFrame::operator&&;
        using NDFrame::operator||;
        using NDFrame::operator^;
        using NDFrame::operator!;

        //--------------------------------------------------------------------------
        // 9) Serialization
        //--------------------------------------------------------------------------
        friend std::ostream& operator<<(std::ostream& os, DataFrame const&);
        std::string          repr() const;

        //--------------------------------------------------------------------------
        // 12) Common Operations
        //--------------------------------------------------------------------------
        size_t num_rows() const;

        size_t num_cols() const;

        std::vector<std::string> column_names() const;

        arrow::TablePtr table() const
        {
            return m_table;
        }

        Series to_series() const;

        using NDFrame::from_base;

        DataFrame from_base(IndexPtr const& index, arrow::TablePtr const& table) const override;

        DataFrame from_base(TableComponent const& tableComponent) const override;

        DataFrame reset_index(std::optional<std::string> const& name = std::nullopt) const;

        GroupByAgg<DataFrame> group_by_agg(std::vector<std::string> const& by) const;
        GroupByAgg<DataFrame> group_by_agg(arrow::ChunkedArrayVector const& by) const;
        GroupByApply          group_by_apply(std::vector<std::string> const& by,
                                             bool                            groupKeys = true) const;
        GroupByApply          group_by_apply(arrow::ChunkedArrayVector const& by,
                                             bool                             groupKeys = true) const;

        GroupByAgg<DataFrame> resample_by_agg(const TimeGrouperOptions& options) const;
        GroupByApply          resample_by_apply(const TimeGrouperOptions& options,
                                                bool                      groupKeys = true) const;
        DataFrame
                              resample_by_ohlcv(const TimeGrouperOptions&                           options,
                                                std::unordered_map<std::string, std::string> const& columns) const;

        std::string diff(DataFrame const& other) const;

        GroupByAgg<DataFrame> group_by_agg(std::string const& by) const
        {
            return group_by_agg(std::vector<std::string>{by});
        }
        GroupByAgg<DataFrame> group_by_agg(arrow::ChunkedArrayPtr const& by) const
        {
            return group_by_agg(arrow::ChunkedArrayVector{by});
        }
        GroupByApply group_by_apply(std::string const& by, bool groupKeys = true) const
        {
            return group_by_apply(std::vector<std::string>{by}, groupKeys);
        }
        GroupByApply group_by_apply(arrow::ChunkedArrayPtr const& by, bool groupKeys = true) const
        {
            return group_by_apply(arrow::ChunkedArrayVector{by}, groupKeys);
        }

        /**
         * @brief Apply a function to each row/column of the DataFrame
         *
         * @param func A function that takes a row/column as a Series and returns a Series
         * @param axis The axis to apply the function to. If AxisType::Row, the function is applied
         * to each row. If AxisType::Column, the function is applied to each column.
         * @return A new DataFrame with the results
         */
        DataFrame apply(const std::function<Series(const Series&)>& func,
                        AxisType                                    axis = AxisType::Row) const;
        DataFrame apply(const std::function<Array(const Array&)>& func,
                        AxisType                                  axis = AxisType::Row) const;
        AggRollingWindowOperations<true>
        rolling_agg(window::RollingWindowOptions const& options) const;
        ApplyDataFrameRollingWindowOperations
        rolling_apply(window::RollingWindowOptions const& options) const;

        AggRollingWindowOperations<true>
        expanding_agg(window::ExpandingWindowOptions const& options) const;
        ApplyDataFrameRollingWindowOperations
        expanding_apply(window::ExpandingWindowOptions const& options) const;

        // Assignments
        DataFrame assign(std::string const&, Series const&) const;

        DataFrame assign(IndexPtr const&, arrow::TablePtr const&) const;
        DataFrame assign(DataFrame const& df) const
        {
            return assign(df.m_index, df.m_table);
        }

        DataFrame drop(std::string const& column) const;
        DataFrame drop(std::vector<std::string> const& columns) const;
        bool      contains(std::string const& column) const
        {
            return m_table->schema()->GetFieldIndex(column) != -1;
        }

        // Inherit map method from NDFrame
        using NDFrame::map;

      private:
        DataFrame add_prefix_or_suffix(const std::string& prefix_or_suffix, bool is_prefix) const;
    };
} // namespace epoch_frame
