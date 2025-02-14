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
        arrow::RecordBatchPtr ascii_is_alnum() const;
        arrow::RecordBatchPtr ascii_is_alpha() const;
        arrow::RecordBatchPtr ascii_is_decimal() const;
        arrow::RecordBatchPtr ascii_is_lower() const;
        arrow::RecordBatchPtr ascii_is_printable() const;
        arrow::RecordBatchPtr ascii_is_space() const;
        arrow::RecordBatchPtr ascii_is_upper() const;
        arrow::RecordBatchPtr utf8_is_alnum() const;
        arrow::RecordBatchPtr utf8_is_alpha() const;
        arrow::RecordBatchPtr utf8_is_decimal() const;
        arrow::RecordBatchPtr utf8_is_digit() const;
        arrow::RecordBatchPtr utf8_is_lower() const;
        arrow::RecordBatchPtr utf8_is_numeric() const;
        arrow::RecordBatchPtr utf8_is_printable() const;
        arrow::RecordBatchPtr utf8_is_space() const;
        arrow::RecordBatchPtr utf8_is_upper() const;

        arrow::RecordBatchPtr ascii_is_title() const;
        arrow::RecordBatchPtr utf8_is_title() const;

        arrow::RecordBatchPtr string_is_ascii() const;

        // String Transforms
        arrow::RecordBatchPtr ascii_capitalize() const;
        arrow::RecordBatchPtr ascii_lower() const;
        arrow::RecordBatchPtr ascii_reverse() const;
        arrow::RecordBatchPtr ascii_swapcase() const;
        arrow::RecordBatchPtr ascii_title() const;
        arrow::RecordBatchPtr ascii_upper() const;
        arrow::RecordBatchPtr binary_length() const;
        arrow::RecordBatchPtr binary_repeat() const;
        arrow::RecordBatchPtr binary_replace_slice(const arrow::compute::ReplaceSliceOptions& ) const;
        arrow::RecordBatchPtr binary_reverse() const;
        arrow::RecordBatchPtr replace_substring(const arrow::compute::ReplaceSliceOptions& ) const;
        arrow::RecordBatchPtr replace_substring_regex(const arrow::compute::ReplaceSliceOptions& ) const;
        arrow::RecordBatchPtr utf8_capitalize() const;
        arrow::RecordBatchPtr utf8_length() const;
        arrow::RecordBatchPtr utf8_lower() const;
        arrow::RecordBatchPtr utf8_replace_slice(const arrow::compute::ReplaceSliceOptions& ) const;
        arrow::RecordBatchPtr utf8_reverse() const;
        arrow::RecordBatchPtr utf8_swapcase() const;
        arrow::RecordBatchPtr utf8_title() const;
        arrow::RecordBatchPtr utf8_upper() const;

        // String Padding
        arrow::RecordBatchPtr ascii_center(const arrow::compute::PadOptions& ) const;
        arrow::RecordBatchPtr ascii_lpad(const arrow::compute::PadOptions& ) const;
        arrow::RecordBatchPtr ascii_rpad(const arrow::compute::PadOptions& ) const;
        arrow::RecordBatchPtr utf8_center(const arrow::compute::PadOptions& ) const;
        arrow::RecordBatchPtr utf8_lpad(const arrow::compute::PadOptions& ) const;
        arrow::RecordBatchPtr utf8_rpad(const arrow::compute::PadOptions& ) const;

        // String Padding
        arrow::RecordBatchPtr ascii_ltrim(const arrow::compute::TrimOptions& ) const;
        arrow::RecordBatchPtr ascii_ltrim_whitespace() const;
        arrow::RecordBatchPtr ascii_rtrim(const arrow::compute::TrimOptions& ) const;
        arrow::RecordBatchPtr ascii_rtrim_whitespace() const;
        arrow::RecordBatchPtr ascii_trim(const arrow::compute::TrimOptions& ) const;
        arrow::RecordBatchPtr ascii_trim_whitespace() const;
        arrow::RecordBatchPtr utf8_ltrim(const arrow::compute::TrimOptions& ) const;
        arrow::RecordBatchPtr utf8_ltrim_whitespace() const;
        arrow::RecordBatchPtr utf8_rtrim(const arrow::compute::TrimOptions& ) const;
        arrow::RecordBatchPtr utf8_rtrim_whitespace() const;
        arrow::RecordBatchPtr utf8_trim(const arrow::compute::TrimOptions& ) const;
        arrow::RecordBatchPtr utf8_trim_whitespace() const;

        // String Splitting
        arrow::RecordBatchPtr ascii_split_whitespace(const arrow::compute::SplitOptions& ) const;
        arrow::RecordBatchPtr split_pattern(const arrow::compute::SplitPatternOptions& ) const;
        arrow::RecordBatchPtr split_pattern_regex(const arrow::compute::SplitPatternOptions& ) const;
        arrow::RecordBatchPtr utf8_split_whitespace(const arrow::compute::SplitOptions& ) const;

        // String Component extraction
        arrow::RecordBatchPtr extract_regex(TableComponent const&, const arrow::compute::ExtractRegexOptions& ) const;

        // String joining
        arrow::RecordBatchPtr binary_join(IndexPtr const&, arrow::RecordBatchPtr const&) const;
        arrow::RecordBatchPtr binary_join_element_wise(TableComponents const&, const arrow::compute::JoinOptions& ) const;

        // String slicing
        arrow::RecordBatchPtr binary_slice(const arrow::compute::SliceOptions&) const;
        arrow::RecordBatchPtr utf8_slice_codeunits(const arrow::compute::SliceOptions&) const;

        // String containment
        arrow::RecordBatchPtr count_substring(arrow::compute::MatchSubstringOptions const&) const;
        arrow::RecordBatchPtr count_substring_regex(arrow::compute::MatchSubstringOptions const&) const;
        arrow::RecordBatchPtr ends_with(arrow::compute::MatchSubstringOptions const&) const;
        arrow::RecordBatchPtr find_substring(arrow::compute::MatchSubstringOptions const&) const;
        arrow::RecordBatchPtr find_substring_regex(arrow::compute::MatchSubstringOptions const&) const;
        arrow::RecordBatchPtr match_like(arrow::compute::MatchSubstringOptions const&) const;
        arrow::RecordBatchPtr match_substring(arrow::compute::MatchSubstringOptions const&) const;
        arrow::RecordBatchPtr match_substring_regex(arrow::compute::MatchSubstringOptions const&) const;
        arrow::RecordBatchPtr starts_with(arrow::compute::MatchSubstringOptions const&) const;

        arrow::RecordBatchPtr index_in(arrow::compute::SetLookupOptions const&) const;
        arrow::RecordBatchPtr is_in(arrow::compute::SetLookupOptions const&) const;

    private:
        TableComponent m_table;
    };
}
