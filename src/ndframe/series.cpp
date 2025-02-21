//
// Created by adesola on 2/16/25.
//
#include "epochframe/series.h"
#include "factory/index_factory.h"
#include "common/asserts.h"
#include "index/index.h"
#include <arrow/api.h>
#include <tabulate/table.hpp>
#include "epochframe/dataframe.h"
#include "methods/arith.h"


namespace epochframe {
    Series::Series() : NDFrame<Series, arrow::ChunkedArray>() {}

    Series::Series(epochframe::IndexPtr index, arrow::ChunkedArrayPtr array, const std::optional<std::string> &name):NDFrame<Series, arrow::ChunkedArray>(index, array), m_name(name) {}

    Series::Series(IndexPtr index, arrow::ArrayPtr array, const std::optional<std::string> &name) :
            Series(index, AssertArrayResultIsOk(arrow::ChunkedArray::Make({array})), name) {}

    Series::Series(arrow::ArrayPtr const &data, std::optional<std::string> const &name):Series(factory::index::from_range(0, data->length()), data, name) {}

    Series::Series(arrow::ChunkedArrayPtr const &data, std::optional<std::string> const &name):Series(factory::index::from_range(0, data->length()), data, name) {}


    DataFrame Series::to_frame(std::optional<std::string> const &name) const {
        return DataFrame(m_index,
                       arrow::Table::Make(
                               arrow::schema({arrow::field(name.value_or(m_name.value_or("")), m_table->type())}),
                               {m_table}));
    }

//--------------------------------------------------------------------------
    // 2) Basic arithmetic: +, -, *, / with NDFrame and Scalar
    //--------------------------------------------------------------------------

    DataFrame Series::operator+(DataFrame const &other) const {
        // return from_base(other.arithmetic().radd(*m_tableComponent));
        return DataFrame();
    }

    DataFrame Series::operator-(DataFrame const &other) const {
        // return from_base(other.arithmetic().rsubtract(*m_tableComponent));
        return DataFrame();
    }

    DataFrame Series::operator*(DataFrame const &other) const {
        // return from_base(other.arithmetic().rmultiply(*m_tableComponent));
        return DataFrame();
    }

    DataFrame Series::operator/(DataFrame const &other) const {
        // return from_base(other.arithmetic().rdivide(*m_tableComponent));
        return DataFrame();
    }

    std::ostream &operator<<(std::ostream &os, Series const &df) {
        tabulate::Table table;
        tabulate::Table::Row_t header{fmt::format("index({})", df.m_index->dtype()->ToString())};
        header.push_back(fmt::format("{}({})", df.m_name.value_or(""), df.m_table->type()->ToString()));
        table.add_row(header);
        for (int64_t i = 0; i < df.m_index->size(); ++i) {
            auto index = df.m_index->array()->GetScalar(i).MoveValueUnsafe()->ToString();
            tabulate::Table::Row_t row{index};
            row.push_back(df.m_table->GetScalar(i).MoveValueUnsafe()->ToString());
            table.add_row(row);
        }
        os << table;
        return os;
    }

///** duplicated() */
//    std::shared_ptr<arrow::BooleanArray>
//    ArrowIndex::duplicated(DropDuplicatesKeepPolicy keep) const {
//        // Implementation idea:
//        // 1) dictionary_encode => we get 'codes'
//        // 2) track the first or last occurrence
//        // 3) mark all others as duplicates
//
//        using namespace arrow::compute;
//        auto [codes, _] = dictionary_encode(m_array);
//        auto length = codes->length();
//
//        // build boolean array
//        arrow::BooleanBuilder builder;
//        AssertStatusIsOk(builder.Reserve(length));
//
//        // We store code->first/last occurrence index
//        std::unordered_map<int32_t, int64_t> seen;
//
//        if (keep == DropDuplicatesKeepPolicy::First) {
//            // forward pass
//            for (int64_t i = 0; i < length; ++i) {
//                auto c = codes->Value(i);
//                if (seen.find(c) == seen.end()) {
//                    // first time => not duplicate
//                    builder.UnsafeAppend(false);
//                    seen[c] = i;
//                } else {
//                    // subsequent => true
//                    builder.UnsafeAppend(true);
//                }
//            }
//        } else if (keep == DropDuplicatesKeepPolicy::Last) {
//            // We'll do a reverse pass to mark duplicates from the end
//            // Then we invert the logic after
//            std::vector<bool> temp(length, false);
//            for (int64_t i = length - 1; i >= 0; --i) {
//                auto c = codes->Value(i);
//                if (seen.find(c) == seen.end()) {
//                    // first time from the end => not duplicate
//                    temp[i] = false;
//                    seen[c] = i;
//                } else {
//                    // subsequent => duplicate
//                    temp[i] = true;
//                }
//            }
//            // Now append in forward order
//            for (int64_t i = 0; i < length; ++i) {
//                builder.UnsafeAppend(temp[i]);
//            }
//        } else {
//            // keep == DropDuplicatesKeepPolicy::False => everything that's repeated is duplicate
//            // So *all* occurrences of repeated items are true except possibly the first, or maybe all occurrences?
//            // "False" in Pandas means "mark all duplicates"
//            // So let's do two passes or a map of counts
//            std::unordered_map<int32_t, int64_t> counts;
//            for (int64_t i = 0; i < length; ++i) {
//                counts[codes->Value(i)]++;
//            }
//            for (int64_t i = 0; i < length; ++i) {
//                auto c = codes->Value(i);
//                if (counts[c] > 1) {
//                    // repeated
//                    builder.UnsafeAppend(true);
//                } else {
//                    builder.UnsafeAppend(false);
//                }
//            }
//        }
//
//        std::shared_ptr<arrow::BooleanArray> result;
//        auto st = builder.Finish(&result);
//        if (!st.ok()) {
//            throw std::runtime_error(st.ToString());
//        }
//        return result;
//    }
//
///** drop_duplicates() */
//    std::shared_ptr<Index>
//    ArrowIndex::drop_duplicates(DropDuplicatesKeepPolicy keep) const {
//        auto mask = AssertContiguousArrayResultIsOk(arrow::compute::Invert(duplicated(keep)));
//
//        // Now filter the original array
//        arrow::compute::FilterOptions filter_opts{};
//        auto filtered_arr = AssertContiguousArrayResultIsOk(arrow::compute::Filter(m_array, mask, filter_opts));
//
//        return std::make_shared<ArrowIndex>(filtered_arr, m_name);
//    }
//
//    std::shared_ptr<Index> putmask(arrow::ArrayPtr const &mask,
//                                   arrow::ArrayPtr const &replacements) const final {
//    return std::make_shared<ArrowIndex>(
//            AssertContiguousArrayResultIsOk(arrow::compute::ReplaceWithMask(m_array, mask, replacements)),
//            m_name);
//}
}
