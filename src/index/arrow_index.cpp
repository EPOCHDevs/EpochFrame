//
// Created by adesola on 1/20/25.
//

#include "arrow_index.h"
#include "common_utils/asserts.h"  // for your AssertStatusIsOk, etc.

namespace epochframe {

/** nbytes() - sum buffer sizes */
    template<typename T>
    size_t ArrowIndex<T>::nbytes() const {
        if (!m_array) return 0;
        auto data = m_array->data();
        if (!data) return 0;
        size_t total = 0;
        for (const auto &buf: data->buffers) {
            if (buf) {
                total += buf->size();
            }
        }
        return total;
    }

/** identical() */
    template<typename T>
    bool ArrowIndex<T>::identical(std::shared_ptr<Index> const &other) const {
        if (!other) return false;
        if (this->name() != other->name()) return false;
        if (!this->dtype() || !other->dtype()) return false;
        if (!this->dtype()->Equals(*other->dtype())) return false;
        return this->equals(other);
    }

/** duplicated() */
    template<typename T>
    std::shared_ptr<arrow::BooleanArray>
    ArrowIndex<T>::duplicated(DropDuplicatesKeepPolicy keep) const {
        // Implementation idea:
        // 1) dictionary_encode => we get 'codes'
        // 2) track the first or last occurrence
        // 3) mark all others as duplicates

        using namespace arrow::compute;
        auto dict_res = CallFunction("dictionary_encode", {m_array});
        if (!dict_res.ok()) {
            throw std::runtime_error(dict_res.status().ToString());
        }
        auto dict_array = std::dynamic_pointer_cast<arrow::DictionaryArray>(dict_res->make_array());
        if (!dict_array) {
            throw std::runtime_error("duplicated(): dictionary_encode didn't return DictionaryArray");
        }

        auto codes = std::dynamic_pointer_cast<arrow::Int32Array>(dict_array->indices());
        auto length = codes->length();

        // build boolean array
        arrow::BooleanBuilder builder;
        AssertStatusIsOk(builder.Reserve(length));

        // We store code->first/last occurrence index
        std::unordered_map<int32_t, int64_t> seen;

        if (keep == DropDuplicatesKeepPolicy::First) {
            // forward pass
            for (int64_t i = 0; i < length; ++i) {
                auto c = codes->Value(i);
                if (seen.find(c) == seen.end()) {
                    // first time => not duplicate
                    builder.UnsafeAppend(false);
                    seen[c] = i;
                } else {
                    // subsequent => true
                    builder.UnsafeAppend(true);
                }
            }
        } else if (keep == DropDuplicatesKeepPolicy::Last) {
            // We'll do a reverse pass to mark duplicates from the end
            // Then we invert the logic after
            std::vector<bool> temp(length, false);
            for (int64_t i = length - 1; i >= 0; --i) {
                auto c = codes->Value(i);
                if (seen.find(c) == seen.end()) {
                    // first time from the end => not duplicate
                    temp[i] = false;
                    seen[c] = i;
                } else {
                    // subsequent => duplicate
                    temp[i] = true;
                }
            }
            // Now append in forward order
            for (int64_t i = 0; i < length; ++i) {
                builder.UnsafeAppend(temp[i]);
            }
        } else {
            // keep == DropDuplicatesKeepPolicy::False => everything that's repeated is duplicate
            // So *all* occurrences of repeated items are true except possibly the first, or maybe all occurrences?
            // "False" in Pandas means "mark all duplicates"
            // So let's do two passes or a map of counts
            std::unordered_map<int32_t, int64_t> counts;
            for (int64_t i = 0; i < length; ++i) {
                counts[codes->Value(i)]++;
            }
            for (int64_t i = 0; i < length; ++i) {
                auto c = codes->Value(i);
                if (counts[c] > 1) {
                    // repeated
                    builder.UnsafeAppend(true);
                } else {
                    builder.UnsafeAppend(false);
                }
            }
        }

        std::shared_ptr<arrow::BooleanArray> result;
        auto st = builder.Finish(&result);
        if (!st.ok()) {
            throw std::runtime_error(st.ToString());
        }
        return result;
    }

/** drop_duplicates() */
    template<typename T>
    std::shared_ptr<Index>
    ArrowIndex<T>::drop_duplicates(DropDuplicatesKeepPolicy keep) const {
        auto mask = AssertArrayResultIsOk(arrow::compute::Invert(duplicated(keep)));

        // Now filter the original array
        arrow::compute::FilterOptions filter_opts{};
        auto filtered_arr = AssertCastArrayResultIsOk<T>(arrow::compute::Filter(m_array, mask, filter_opts));

        return std::make_shared<ArrowIndex<T>>(filtered_arr, m_name);
    }

/** delete_(loc) */
    template<typename T>
    std::shared_ptr<Index>
    ArrowIndex<T>::delete_(int64_t loc) const {
        // Remove the element at 'loc'
        // 0 <= loc < length
        auto length = static_cast<int64_t>(m_array->length());
        if (loc < 0) {
            loc += length; // handle negative indexing
        }
        if (loc < 0 || loc >= length) {
            throw std::runtime_error(
                    fmt::format("delete_() out-of-bounds loc={}", loc)
            );
        }
        // slice 0..loc, slice loc+1..end => arrow::Concatenate
        auto slice1 = m_array->Slice(0, loc);
        auto slice2 = m_array->Slice(loc + 1, length - (loc + 1));

        std::vector<arrow::ArrayPtr> to_concat{slice1, slice2};
        arrow::ArrayPtr result = AssertArrayResultIsOk(arrow::Concatenate(to_concat));
        return std::make_shared<ArrowIndex<T>>(PtrCast<T>(result), m_name);
    }

/** insert(loc, value) */
    template<typename T>
    std::shared_ptr<Index>
    ArrowIndex<T>::insert(int64_t loc, arrow::ScalarPtr const &val) const {
        auto length = static_cast<int64_t>(m_array->length());
        if (loc < 0) {
            loc += length;
        }
        if (loc < 0 || loc > length) {
            throw std::runtime_error(
                    fmt::format("insert() out-of-bounds loc={}", loc)
            );
        }
        // Build a 1-element array from 'val'
        auto arr_res = arrow::MakeArrayFromScalar(*val, 1);
        if (!arr_res.ok()) {
            throw std::runtime_error(arr_res.status().ToString());
        }
        auto single_val = *arr_res;

        // slice(0..loc), single_val, slice(loc..end)
        auto slice1 = m_array->Slice(0, loc);
        auto slice2 = m_array->Slice(loc, length - loc);

        std::vector<arrow::ArrayPtr> to_concat{slice1, single_val, slice2};
        return std::make_shared<ArrowIndex<T>>(AssertCastResultIsOk<T>(arrow::Concatenate(to_concat)), m_name);
    }

/** get_loc(label) */
    template<typename T>
    int64_t ArrowIndex<T>::get_loc(arrow::ScalarPtr const &label) const {
        // We'll build a 1-element array from the scalar, then do a "match" or "index_in"
        // Then see the result
        using namespace arrow::compute;

        auto arr_res = arrow::MakeArrayFromScalar(*label, 1);
        if (!arr_res.ok()) {
            throw std::runtime_error(arr_res.status().ToString());
        }
        auto label_arr = *arr_res;

        // "match" returns the index of each value in the dictionary, or -1 if not found
        auto match_res = CallFunction("match", {label_arr, m_array});
        if (!match_res.ok()) {
            throw std::runtime_error(match_res.status().ToString());
        }
        auto match_arr = match_res->make_array();
        // match_arr is an Int32Array typically
        auto int32_match = std::dynamic_pointer_cast<arrow::Int32Array>(match_arr);
        if (!int32_match || int32_match->length() == 0) {
            return -1;
        }
        return static_cast<int64_t>(int32_match->Value(0));
    }

/** slice_locs(start, end) */
    template<typename T>
    std::pair<int64_t, int64_t>
    ArrowIndex<T>::slice_locs(arrow::ScalarPtr const &start, arrow::ScalarPtr const &end) const {
        // We'll just do get_loc for each
        auto start_pos = get_loc(start);
        auto end_pos = get_loc(end);
        // If either is -1 => not found => we clamp them or handle however Pandas does
        if (start_pos < 0) { start_pos = 0; }
        if (end_pos < 0) { end_pos = static_cast<int64_t>(size()); }
        // ensure start_pos <= end_pos
        if (start_pos > end_pos) {
            std::swap(start_pos, end_pos);
        }
        return {start_pos, end_pos};
    }


/** unique() => calls "unique" kernel */
    template<typename T>
    std::shared_ptr<Index> ArrowIndex<T>::unique() const {
        // "unique" returns array of distinct elements in the order of first occurrence
        using namespace arrow::compute;
        return std::make_shared<ArrowIndex<T>>(AssertCastArrayResultIsOk<T>(CallFunction("unique", {m_array})),
                                               m_name + "_unique");
    }

/** sort_values(ascending) => uses sort_indices + take */
    template<typename T>
    std::shared_ptr<Index> ArrowIndex<T>::sort_values(bool ascending) const {
        using namespace arrow::compute;
        // "sort_indices"
        SortOptions sort_opts({arrow::compute::SortKey("", ascending ? SortOrder::Ascending : SortOrder::Descending)});
        auto sort_idx_arr = AssertCastResultIsOk<arrow::UInt64Array>(SortIndices(m_array, sort_opts));
        return take(sort_idx_arr, false);
    }

/** union_ => concatenate + unique */
    template<typename T>
    std::shared_ptr<Index> ArrowIndex<T>::union_(std::shared_ptr<Index> const &other) const {
        std::vector<arrow::ArrayPtr> arrs{m_array, other->array()};
        auto combined = AssertCastArrayResultIsOk<T>(arrow::Concatenate(arrs));
        auto combined_idx = std::make_shared<ArrowIndex<T>>(combined, m_name);
        return combined_idx->unique();
    }

/** intersection => elements in both (like a set intersection) */
    template<typename T>
    std::shared_ptr<Index> ArrowIndex<T>::intersection(std::shared_ptr<Index> const &other) const {
        auto me_unique = this->unique();
        return me_unique->where(me_unique->isin(other->array()),
                                arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
    }

/** difference => elements in this but not in other */
    template<typename T>
    std::shared_ptr<Index> ArrowIndex<T>::difference(std::shared_ptr<Index> const &other) const {
        using namespace arrow::compute;
        return where(AssertCastArrayResultIsOk<arrow::BooleanArray>(Invert(isin(other->array()))),
                     arrow::compute::FilterOptions::NullSelectionBehavior::DROP);
    }

/** symmetric_difference => union_ - intersection */
    template<typename T>
    std::shared_ptr<Index> ArrowIndex<T>::symmetric_difference(std::shared_ptr<Index> const &other) const {
        // simple approach: union - intersection
        auto u = this->union_(other);
        auto inter = this->intersection(other);
        return u->difference(inter);
    }

    template<typename T>
    std::pair<std::shared_ptr<Index>, std::shared_ptr<arrow::UInt64Array>>
    ArrowIndex<T>::value_counts() const {
        // call the Arrow "value_counts" kernel
        auto vc_res = AssertArrayResultIsOk(arrow::compute::ValueCounts(m_array));
        auto struct_arr = std::dynamic_pointer_cast<arrow::StructArray>(vc_res);
        if (!struct_arr) {
            throw std::runtime_error("value_counts: kernel did not return StructArray");
        }

        // The struct has 2 fields: "values" (same type as m_array) and "counts" (Int64)
        auto values_arr = struct_arr->GetFieldByName("values");
        auto counts_arr = std::dynamic_pointer_cast<arrow::Int64Array>(struct_arr->GetFieldByName("counts"));
        if (!counts_arr) {
            throw std::runtime_error("value_counts: 'counts' field is not Int64Array");
        }

        // Wrap "values" in a new ArrowIndex
        std::shared_ptr<Index> values_index = std::make_shared<ArrowIndex<T>>(PtrCast<T>(values_arr), m_name);

        // Return (Index of unique values, UInt64Array of counts)
        return std::pair{values_index, AssertCastArrayResultIsOk<arrow::UInt64Array>(
                arrow::compute::Cast(counts_arr, arrow::uint64()))};
    }

    template<typename T>
    uint64_t ArrowIndex<T>::searchsorted(arrow::ScalarPtr const &value,
                                         SearchSortedSide side) const {
        AssertWithTraceFromFormat(value->type->id() == m_array->type_id(),
                                  fmt::format("searchsorted: value type {} does not match index type {}",
                                              value->type->ToString(), m_array->type()->ToString()));

        auto iter = side == SearchSortedSide::Left ? std::lower_bound(m_array->begin(), m_array->end(), value,
                                                                      [](auto const &l, arrow::ScalarPtr const &r) {
                                                                          if (!l) return true;
                                                                          if constexpr (std::is_same_v<T, arrow::TimestampArray>) {
                                                                              return *l <
                                                                                     std::static_pointer_cast<arrow::TimestampScalar>(
                                                                                             r)->value;
                                                                          } else if constexpr (std::is_same_v<T, arrow::UInt64Array>) {
                                                                              return *l <
                                                                                     std::static_pointer_cast<arrow::UInt64Scalar>(
                                                                                             r)->value;
                                                                          } else if constexpr (std::is_same_v<T, arrow::StringArray>) {
                                                                              return std::string{*l} < r->ToString();
                                                                          }
                                                                          throw std::runtime_error(
                                                                                  "searchsorted: unsupported type");
                                                                      }) : std::upper_bound(m_array->begin(),
                                                                                            m_array->end(), value,
                                                                                            [](arrow::ScalarPtr const &l,
                                                                                               auto const &r) {
                                                                                                if (!l) return true;
                                                                                                if constexpr (std::is_same_v<T, arrow::TimestampArray>) {
                                                                                                    return std::static_pointer_cast<arrow::TimestampScalar>(
                                                                                                            l)->value <
                                                                                                           *r;
                                                                                                } else if constexpr (std::is_same_v<T, arrow::UInt64Array>) {
                                                                                                    return std::static_pointer_cast<arrow::UInt64Scalar>(
                                                                                                            l)->value <
                                                                                                           *r;
                                                                                                } else if constexpr (std::is_same_v<T, arrow::StringArray>) {
                                                                                                    return l->ToString() <
                                                                                                           std::string{
                                                                                                                   *r};
                                                                                                }
                                                                                                throw std::runtime_error(
                                                                                                        "searchsorted: unsupported type");
                                                                                            });
        return std::distance(m_array->begin(), iter);
    }

    template
    class ArrowIndex<arrow::TimestampArray>;

    template
    class ArrowIndex<arrow::UInt64Array>;

    template
    class ArrowIndex<arrow::StringArray>;

} // namespace epochframe
