//
// Created by adesola on 2/13/25.
//

#pragma once
#include "epochframe/aliases.h"
#include <arrow/compute/api.h>

namespace epochframe {
    class StringOperations {

    public:
        StringOperations(TableComponent data);

        // String Predicates
        arrow::TablePtr ascii_is_alnum() const;
        arrow::TablePtr ascii_is_alpha() const;
        arrow::TablePtr ascii_is_decimal() const;
        arrow::TablePtr ascii_is_lower() const;
        arrow::TablePtr ascii_is_printable() const;
        arrow::TablePtr ascii_is_space() const;
        arrow::TablePtr ascii_is_upper() const;
        arrow::TablePtr utf8_is_alnum() const;
        arrow::TablePtr utf8_is_alpha() const;
        arrow::TablePtr utf8_is_decimal() const;
        arrow::TablePtr utf8_is_digit() const;
        arrow::TablePtr utf8_is_lower() const;
        arrow::TablePtr utf8_is_numeric() const;
        arrow::TablePtr utf8_is_printable() const;
        arrow::TablePtr utf8_is_space() const;
        arrow::TablePtr utf8_is_upper() const;

        arrow::TablePtr ascii_is_title() const;
        arrow::TablePtr utf8_is_title() const;

        arrow::TablePtr string_is_ascii() const;

        // String Transforms
        arrow::TablePtr ascii_capitalize() const;
        arrow::TablePtr ascii_lower() const;
        arrow::TablePtr ascii_reverse() const;
        arrow::TablePtr ascii_swapcase() const;
        arrow::TablePtr ascii_title() const;
        arrow::TablePtr ascii_upper() const;
        arrow::TablePtr binary_length() const;
        arrow::TablePtr binary_repeat() const;
        arrow::TablePtr binary_replace_slice(const arrow::compute::ReplaceSliceOptions& ) const;
        arrow::TablePtr binary_reverse() const;
        arrow::TablePtr replace_substring(const arrow::compute::ReplaceSliceOptions& ) const;
        arrow::TablePtr replace_substring_regex(const arrow::compute::ReplaceSliceOptions& ) const;
        arrow::TablePtr utf8_capitalize() const;
        arrow::TablePtr utf8_length() const;
        arrow::TablePtr utf8_lower() const;
        arrow::TablePtr utf8_replace_slice(const arrow::compute::ReplaceSliceOptions& ) const;
        arrow::TablePtr utf8_reverse() const;
        arrow::TablePtr utf8_swapcase() const;
        arrow::TablePtr utf8_title() const;
        arrow::TablePtr utf8_upper() const;

        // String Padding
        arrow::TablePtr ascii_center(const arrow::compute::PadOptions& ) const;
        arrow::TablePtr ascii_lpad(const arrow::compute::PadOptions& ) const;
        arrow::TablePtr ascii_rpad(const arrow::compute::PadOptions& ) const;
        arrow::TablePtr utf8_center(const arrow::compute::PadOptions& ) const;
        arrow::TablePtr utf8_lpad(const arrow::compute::PadOptions& ) const;
        arrow::TablePtr utf8_rpad(const arrow::compute::PadOptions& ) const;

        // String Padding
        arrow::TablePtr ascii_ltrim(const arrow::compute::TrimOptions& ) const;
        arrow::TablePtr ascii_ltrim_whitespace() const;
        arrow::TablePtr ascii_rtrim(const arrow::compute::TrimOptions& ) const;
        arrow::TablePtr ascii_rtrim_whitespace() const;
        arrow::TablePtr ascii_trim(const arrow::compute::TrimOptions& ) const;
        arrow::TablePtr ascii_trim_whitespace() const;
        arrow::TablePtr utf8_ltrim(const arrow::compute::TrimOptions& ) const;
        arrow::TablePtr utf8_ltrim_whitespace() const;
        arrow::TablePtr utf8_rtrim(const arrow::compute::TrimOptions& ) const;
        arrow::TablePtr utf8_rtrim_whitespace() const;
        arrow::TablePtr utf8_trim(const arrow::compute::TrimOptions& ) const;
        arrow::TablePtr utf8_trim_whitespace() const;

        // String Splitting
        arrow::TablePtr ascii_split_whitespace(const arrow::compute::SplitOptions& ) const;
        arrow::TablePtr split_pattern(const arrow::compute::SplitPatternOptions& ) const;
        arrow::TablePtr split_pattern_regex(const arrow::compute::SplitPatternOptions& ) const;
        arrow::TablePtr utf8_split_whitespace(const arrow::compute::SplitOptions& ) const;

        // String Component extraction
        arrow::TablePtr extract_regex(TableComponent const&, const arrow::compute::ExtractRegexOptions& ) const;

        // String joining
        arrow::TablePtr binary_join(IndexPtr const&, arrow::TablePtr const&) const;
        arrow::TablePtr binary_join_element_wise(TableComponents const&, const arrow::compute::JoinOptions& ) const;

        // String slicing
        arrow::TablePtr binary_slice(const arrow::compute::SliceOptions&) const;
        arrow::TablePtr utf8_slice_codeunits(const arrow::compute::SliceOptions&) const;

        // String containment
        arrow::TablePtr count_substring(arrow::compute::MatchSubstringOptions const&) const;
        arrow::TablePtr count_substring_regex(arrow::compute::MatchSubstringOptions const&) const;
        arrow::TablePtr ends_with(arrow::compute::MatchSubstringOptions const&) const;
        arrow::TablePtr find_substring(arrow::compute::MatchSubstringOptions const&) const;
        arrow::TablePtr find_substring_regex(arrow::compute::MatchSubstringOptions const&) const;
        arrow::TablePtr match_like(arrow::compute::MatchSubstringOptions const&) const;
        arrow::TablePtr match_substring(arrow::compute::MatchSubstringOptions const&) const;
        arrow::TablePtr match_substring_regex(arrow::compute::MatchSubstringOptions const&) const;
        arrow::TablePtr starts_with(arrow::compute::MatchSubstringOptions const&) const;

        arrow::TablePtr index_in(arrow::compute::SetLookupOptions const&) const;
        arrow::TablePtr is_in(arrow::compute::SetLookupOptions const&) const;

    private:
        TableComponent m_table;
    };
}
