//
// Created by adesola on 2/16/25.
//
// String operations test for Series and Scalar

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <catch2/catch_approx.hpp>
#include "epochframe/dataframe.h"
#include "epochframe/series.h"
#include "epochframe/scalar.h"
#include "factory/index_factory.h"
#include "factory/array_factory.h"

using namespace epochframe;
using namespace epochframe::factory::index;
using namespace epochframe::factory::array;

TEST_CASE("String operations on Series", "[Series][StringOps]") {
    // Create a Series with string values for testing
    auto idx = from_range(5);
    std::vector<std::string> test_strings = {
        "HELLO",
        "world",
        "Hello World",
        "  trimme  ",
        "123abc"
    };
    
    Series string_series = make_array<std::string>(test_strings);
    string_series = Series(idx, string_series.array(), "string_col");
    
    SECTION("Basic string transformations") {
        // Test upper/lower case transformations
        auto upper_result = string_series.str().utf8_upper();
        auto lower_result = string_series.str().utf8_lower();
        
        REQUIRE(upper_result.to_vector<std::string>()[0] == "HELLO");
        REQUIRE(upper_result.to_vector<std::string>()[1] == "WORLD");
        REQUIRE(upper_result.to_vector<std::string>()[2] == "HELLO WORLD");
        
        REQUIRE(lower_result.to_vector<std::string>()[0] == "hello");
        REQUIRE(lower_result.to_vector<std::string>()[1] == "world");
        REQUIRE(lower_result.to_vector<std::string>()[2] == "hello world");
    }
    
    SECTION("String length operations") {
        auto length_result = string_series.str().utf8_length();
        
        REQUIRE(length_result.to_vector<int32_t>()[0] == 5);
        REQUIRE(length_result.to_vector<int32_t>()[1] == 5);
        REQUIRE(length_result.to_vector<int32_t>()[2] == 11);
        REQUIRE(length_result.to_vector<int32_t>()[3] == 10);
        REQUIRE(length_result.to_vector<int32_t>()[4] == 6);
    }
    
    SECTION("String trim operations") {
        // Test trimming whitespace
        auto trimmed = string_series.str().utf8_trim_whitespace();
        
        REQUIRE(trimmed.to_vector<std::string>()[0] == "HELLO");
        REQUIRE(trimmed.to_vector<std::string>()[1] == "world");
        REQUIRE(trimmed.to_vector<std::string>()[2] == "Hello World");
        REQUIRE(trimmed.to_vector<std::string>()[3] == "trimme");
        REQUIRE(trimmed.to_vector<std::string>()[4] == "123abc");
        
        // Test left and right trim operations
        auto left_trimmed = string_series.str().utf8_ltrim_whitespace();
        auto right_trimmed = string_series.str().utf8_rtrim_whitespace();
        
        REQUIRE(left_trimmed.to_vector<std::string>()[3] == "trimme  ");
        REQUIRE(right_trimmed.to_vector<std::string>()[3] == "  trimme");
    }
    
    SECTION("String contains operations") {
        // Test starts_with
        arrow::compute::MatchSubstringOptions starts_options("H");
        auto starts_with_result = string_series.str().starts_with(starts_options);
        
        REQUIRE(starts_with_result.to_vector<bool>()[0] == true);
        REQUIRE(starts_with_result.to_vector<bool>()[1] == false);
        REQUIRE(starts_with_result.to_vector<bool>()[2] == true);
        REQUIRE(starts_with_result.to_vector<bool>()[3] == false);
        REQUIRE(starts_with_result.to_vector<bool>()[4] == false);
        
        // Test ends_with
        arrow::compute::MatchSubstringOptions ends_options("d");
        auto ends_with_result = string_series.str().ends_with(ends_options);
        
        REQUIRE(ends_with_result.to_vector<bool>()[0] == false);
        REQUIRE(ends_with_result.to_vector<bool>()[1] == true);
        REQUIRE(ends_with_result.to_vector<bool>()[2] == true);  // Fixed: "Hello World" does end with 'd'
        
        // Test contains substring
        arrow::compute::MatchSubstringOptions contains_options("o");
        auto contains_result = string_series.str().match_substring(contains_options);
        
        REQUIRE(contains_result.to_vector<bool>()[0] == false);
        REQUIRE(contains_result.to_vector<bool>()[1] == true);
        REQUIRE(contains_result.to_vector<bool>()[2] == true);
    }
    
    SECTION("String count operations") {
        // Test count_substring with uppercase L
        arrow::compute::MatchSubstringOptions count_options_upper("L");
        auto count_result_upper = string_series.str().count_substring(count_options_upper);
        
        REQUIRE(count_result_upper.to_vector<int32_t>()[0] == 2);  // HELLO has 2 'L's
        
        // Test count_substring with lowercase l
        arrow::compute::MatchSubstringOptions count_options_lower("l");
        auto count_result_lower = string_series.str().count_substring(count_options_lower);
        
        REQUIRE(count_result_lower.to_vector<int32_t>()[1] == 1);  // world has 1 'l'
        REQUIRE(count_result_lower.to_vector<int32_t>()[2] == 3);  // Hello World has 3 'l's
    }
    
    SECTION("String replace operations") {
        // Test replace_substring
        arrow::compute::ReplaceSubstringOptions replace_options("l", "L");
        auto replaced = string_series.str().replace_substring(replace_options);
        
        REQUIRE(replaced.to_vector<std::string>()[0] == "HELLO");  // no change, already all uppercase
        REQUIRE(replaced.to_vector<std::string>()[1] == "worLd");  // l -> L
        REQUIRE(replaced.to_vector<std::string>()[2] == "HeLLo WorLd");  // all l's -> L's
    }
    
    SECTION("String is_X predicate operations") {
        // Create a series with different types of strings
        std::vector<std::string> predicates = {"abc", "123", "ABC", " \t\n"};
        Series pred_series = make_array<std::string>(predicates);
        pred_series = Series(from_range(4), pred_series.array(), "pred_series");
        
        // Test alpha predicates
        auto is_alpha = pred_series.str().utf8_is_alpha();
        
        REQUIRE(is_alpha.to_vector<bool>()[0] == true);   // abc
        REQUIRE(is_alpha.to_vector<bool>()[1] == false);  // 123
        REQUIRE(is_alpha.to_vector<bool>()[2] == true);   // ABC
        REQUIRE(is_alpha.to_vector<bool>()[3] == false);  // whitespace
        
        // Test numeric predicates
        auto is_digit = pred_series.str().utf8_is_digit();
        
        REQUIRE(is_digit.to_vector<bool>()[0] == false);  // abc
        REQUIRE(is_digit.to_vector<bool>()[1] == true);   // 123
        REQUIRE(is_digit.to_vector<bool>()[2] == false);  // ABC
        REQUIRE(is_digit.to_vector<bool>()[3] == false);  // whitespace
        
        // Test case predicates
        auto is_lower = pred_series.str().utf8_is_lower();
        auto is_upper = pred_series.str().utf8_is_upper();
        
        REQUIRE(is_lower.to_vector<bool>()[0] == true);   // abc
        REQUIRE(is_lower.to_vector<bool>()[2] == false);  // ABC
        
        REQUIRE(is_upper.to_vector<bool>()[0] == false);  // abc
        REQUIRE(is_upper.to_vector<bool>()[2] == true);   // ABC
        
        // Test whitespace predicate
        auto is_space = pred_series.str().utf8_is_space();
        
        REQUIRE(is_space.to_vector<bool>()[0] == false);  // abc
        REQUIRE(is_space.to_vector<bool>()[3] == true);   // whitespace
    }
    
    SECTION("String padding operations") {
        std::vector<std::string> pad_strings = {"abc", "12"};
        Series pad_series = make_array<std::string>(pad_strings);
        pad_series = Series(from_range(2), pad_series.array(), "pad_series");
        
        // Test center padding
        arrow::compute::PadOptions center_options(5, " ");
        auto centered = pad_series.str().utf8_center(center_options);
        
        REQUIRE(centered.to_vector<std::string>()[0] == " abc ");  // centered in 5 chars
        REQUIRE(centered.to_vector<std::string>()[1] == " 12  ");  // centered in 5 chars
        
        // Test left and right padding
        arrow::compute::PadOptions pad_options(5, "0");
        auto left_padded = pad_series.str().utf8_lpad(pad_options);
        auto right_padded = pad_series.str().utf8_rpad(pad_options);
        
        REQUIRE(left_padded.to_vector<std::string>()[0] == "00abc");  // left padded to 5 chars
        REQUIRE(left_padded.to_vector<std::string>()[1] == "00012");  // left padded to 5 chars
        
        REQUIRE(right_padded.to_vector<std::string>()[0] == "abc00");  // right padded to 5 chars
        REQUIRE(right_padded.to_vector<std::string>()[1] == "12000");  // right padded to 5 chars
    }

    /*
    // String split operations can't be easily tested without understanding the return type structure
    // We'll skip this for now and add it later
    SECTION("String split operations") {
        std::vector<std::string> split_strings = {"a b c", "1,2,3"};
        Series split_series = make_array<std::string>(split_strings);
        split_series = Series(from_range(2), split_series.array(), "split_series");
        
        // Test split by whitespace
        arrow::compute::SplitOptions whitespace_options;
        auto whitespace_split = split_series.str().utf8_split_whitespace(whitespace_options);
        
        // Test split by pattern
        arrow::compute::SplitPatternOptions comma_options(",");
        auto comma_split = split_series.str().split_pattern(comma_options);
    }
    */
}

// For now, we'll skip the Scalar tests since the API may be different than what we assumed
/*
TEST_CASE("String operations on Scalar", "[Scalar][StringOps]") {
    // Create a string scalar for testing
    arrow::ScalarPtr scalar_ptr = arrow::MakeScalar("Hello World");
    Scalar string_scalar(scalar_ptr);
    
    SECTION("Basic string transformations") {
        // Test upper/lower case transformations
        auto upper_result = string_scalar.str().utf8_upper();
        auto lower_result = string_scalar.str().utf8_lower();
        
        // Check the results - the exact API will depend on the implementation
        // We can enable these tests once we understand the Scalar API better
    }
}
*/ 