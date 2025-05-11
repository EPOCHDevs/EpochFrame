//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epoch_frame/array.h"

#define CALL_STRING_OPERATION(name) Type name() const { return call_function(#name); }
#define CALL_TRIM_OPERATION(name) Type name(const arrow::compute::TrimOptions& options) const { return call_function(#name, &options); }
#define CALL_PAD_OPERATION(name) Type name(const arrow::compute::PadOptions& options) const { return call_function(#name, &options); }
#define CALL_SPLIT_OPERATION(name) Type name(const arrow::compute::SplitOptions& options) const { return call_function(#name, &options); }
#define CALL_REPLACE_OPERATION(name) Type name(const arrow::compute::ReplaceSliceOptions& options) const { return call_function(#name, &options); }
#define CALL_REPLACE_SUBSTRING_OPERATION(name) Type name(const arrow::compute::ReplaceSubstringOptions& options) const { return call_function(#name, &options); }
namespace epoch_frame {
    template<bool is_array>
    class StringOperation {

    public:
        using Type = std::conditional_t<is_array, Array, Scalar>;

        explicit StringOperation(const Type& data);

        // String Predicates
        CALL_STRING_OPERATION(ascii_is_alnum)
        CALL_STRING_OPERATION(ascii_is_alpha)
        CALL_STRING_OPERATION(ascii_is_decimal)
        CALL_STRING_OPERATION(ascii_is_lower)
        CALL_STRING_OPERATION(ascii_is_printable)
        CALL_STRING_OPERATION(ascii_is_space)
        CALL_STRING_OPERATION(ascii_is_upper)
        CALL_STRING_OPERATION(utf8_is_alnum)
        CALL_STRING_OPERATION(utf8_is_alpha)
        CALL_STRING_OPERATION(utf8_is_decimal)
        CALL_STRING_OPERATION(utf8_is_digit)
        CALL_STRING_OPERATION(utf8_is_lower)
        CALL_STRING_OPERATION(utf8_is_numeric)
        CALL_STRING_OPERATION(utf8_is_printable)
        CALL_STRING_OPERATION(utf8_is_space)
        CALL_STRING_OPERATION(utf8_is_upper)

        CALL_STRING_OPERATION(ascii_is_title)
        CALL_STRING_OPERATION(utf8_is_title)

        CALL_STRING_OPERATION(string_is_ascii)

        // String Transforms
        CALL_STRING_OPERATION(ascii_capitalize)
        CALL_STRING_OPERATION(ascii_lower)
        CALL_STRING_OPERATION(ascii_reverse)
        CALL_STRING_OPERATION(ascii_swapcase)
        CALL_STRING_OPERATION(ascii_title)
        CALL_STRING_OPERATION(ascii_upper)
        CALL_STRING_OPERATION(binary_length)
        CALL_STRING_OPERATION(binary_repeat)
        CALL_REPLACE_OPERATION(binary_replace_slice)
        CALL_STRING_OPERATION(binary_reverse)
        CALL_REPLACE_SUBSTRING_OPERATION(replace_substring)
        CALL_REPLACE_OPERATION(replace_substring_regex)
        CALL_STRING_OPERATION(utf8_capitalize)
        CALL_STRING_OPERATION(utf8_length)
        CALL_STRING_OPERATION(utf8_lower)
        CALL_REPLACE_OPERATION(utf8_replace_slice)
        CALL_STRING_OPERATION(utf8_reverse)
        CALL_STRING_OPERATION(utf8_swapcase)
        CALL_STRING_OPERATION(utf8_title)
        CALL_STRING_OPERATION(utf8_upper)

        // String Padding
        CALL_PAD_OPERATION(ascii_center)
        CALL_PAD_OPERATION(ascii_lpad)
        CALL_PAD_OPERATION(ascii_rpad)
        CALL_PAD_OPERATION(utf8_center)
        CALL_PAD_OPERATION(utf8_lpad)
        CALL_PAD_OPERATION(utf8_rpad)

        // String Padding
        CALL_TRIM_OPERATION(ascii_ltrim)
        CALL_STRING_OPERATION(ascii_ltrim_whitespace)
        CALL_TRIM_OPERATION(ascii_rtrim)
        CALL_STRING_OPERATION(ascii_rtrim_whitespace)
        CALL_TRIM_OPERATION(ascii_trim)
        CALL_STRING_OPERATION(ascii_trim_whitespace)
        CALL_TRIM_OPERATION(utf8_ltrim)
        CALL_STRING_OPERATION(utf8_ltrim_whitespace)
        CALL_TRIM_OPERATION(utf8_rtrim)
        CALL_STRING_OPERATION(utf8_rtrim_whitespace)
        CALL_TRIM_OPERATION(utf8_trim)
        CALL_STRING_OPERATION(utf8_trim_whitespace)

        // String Splitting
        Type ascii_split_whitespace(const arrow::compute::SplitOptions& options) const {
            return call_function("ascii_split_whitespace", &options);
        }
        Type split_pattern(const arrow::compute::SplitPatternOptions& options) const {
            return call_function("split_pattern", &options);
        }
        Type split_pattern_regex(const arrow::compute::SplitPatternOptions& options) const {
            return call_function("split_pattern_regex", &options);
        }
        Type utf8_split_whitespace(const arrow::compute::SplitOptions& options) const {
            return call_function("utf8_split_whitespace", &options);
        }

        // String Component extraction
        Type extract_regex(const arrow::compute::ExtractRegexOptions& options) const{
            return call_function("extract_regex", &options);
        }

        // String slicing
        Type binary_slice(const arrow::compute::SliceOptions& options) const {
            return call_function("binary_slice", &options);
        }
        Type utf8_slice_codeunits(const arrow::compute::SliceOptions& options) const {
            return call_function("utf8_slice_codeunits", &options);
        }
        Type strptime(arrow::compute::StrptimeOptions const& options) const
        {
            return call_function("strptime", &options);
        }
        // String containment
        Type count_substring(arrow::compute::MatchSubstringOptions const& options) const {
            return call_function("count_substring", &options);
        }
        Type count_substring_regex(arrow::compute::MatchSubstringOptions const& options) const {
            return call_function("count_substring_regex", &options);
        }
        Type ends_with(arrow::compute::MatchSubstringOptions const& options) const {
            return call_function("ends_with", &options);
        }
        Type find_substring(arrow::compute::MatchSubstringOptions const& options) const {
            return call_function("find_substring", &options);
        }
        Type match_like(arrow::compute::MatchSubstringOptions const& options) const {
            return call_function("match_like", &options);
        }
        Type match_substring(arrow::compute::MatchSubstringOptions const& options) const {
            return call_function("match_substring", &options);
        }
        Type match_substring_regex(arrow::compute::MatchSubstringOptions const& options) const {
            return call_function("match_substring_regex", &options);
        }
        Type starts_with(arrow::compute::MatchSubstringOptions const& options) const {
            return call_function("starts_with", &options);
        }
        Type index_in(arrow::compute::SetLookupOptions const& options) const {
            return call_function("index_in", &options);
        }
        Type is_in(arrow::compute::SetLookupOptions const& options) const {
            return call_function("is_in", &options);
        }

        private:
        Type m_data;

        Type call_function(const std::string& name, const arrow::compute::FunctionOptions* options=nullptr) const;
    };

    extern template class StringOperation<true>;
    extern template class StringOperation<false>;
}
