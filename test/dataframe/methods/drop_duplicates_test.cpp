/*
 * File: drop_duplicates_test.cpp
 * Purpose: Comprehensive tests for DataFrame.drop_duplicates() functionality
 */

#include <catch2/catch_test_macros.hpp>
#include "epoch_frame/dataframe.h"
#include "epoch_frame/common.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include <vector>
#include <string>

using namespace epoch_frame;
using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory::dataframe;

TEST_CASE("Drop Duplicates - Index-based with KeepFirst", "[drop_duplicates][index]") {
    // Create DataFrame with duplicate index values
    // Index: [1, 2, 1, 3, 2, 4]
    // Col A: [10, 20, 30, 40, 50, 60]
    // Col B: [100, 200, 300, 400, 500, 600]
    auto idx = make_index_from_vector<int64_t>({1, 2, 1, 3, 2, 4});
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{10, 20, 30, 40, 50, 60}, {100, 200, 300, 400, 500, 600}},
        {"A", "B"}
    );

    SECTION("Keep first occurrence") {
        auto result = df.drop_duplicates(DropDuplicatesKeepPolicy::First);

        REQUIRE(result.num_rows() == 4);  // Should have 4 unique index values: 1, 2, 3, 4
        REQUIRE(result.num_cols() == 2);

        // Check that we kept the first occurrences
        // Index 1: first occurrence has A=10, B=100
        // Index 2: first occurrence has A=20, B=200
        auto index_values = result.index()->to_vector<int64_t>();
        REQUIRE(index_values.size() == 4);

        // Values should be from first occurrences
        auto colA = result["A"].to_vector<int64_t>();
        auto colB = result["B"].to_vector<int64_t>();

        // Check that 10 and 20 are in the result (first occurrences of 1 and 2)
        REQUIRE(std::find(colA.begin(), colA.end(), 10) != colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 20) != colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 40) != colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 60) != colA.end());

        // 30 and 50 should NOT be present (they're second occurrences)
        REQUIRE(std::find(colA.begin(), colA.end(), 30) == colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 50) == colA.end());
    }
}

TEST_CASE("Drop Duplicates - Index-based with KeepLast", "[drop_duplicates][index]") {
    auto idx = make_index_from_vector<int64_t>({1, 2, 1, 3, 2, 4});
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{10, 20, 30, 40, 50, 60}},
        {"A"}
    );

    SECTION("Keep last occurrence") {
        auto result = df.drop_duplicates(DropDuplicatesKeepPolicy::Last);

        REQUIRE(result.num_rows() == 4);
        REQUIRE(result.num_cols() == 1);

        auto colA = result["A"].to_vector<int64_t>();

        // Should keep last occurrences: index 1->30, index 2->50
        REQUIRE(std::find(colA.begin(), colA.end(), 30) != colA.end());  // Last occurrence of index 1
        REQUIRE(std::find(colA.begin(), colA.end(), 50) != colA.end());  // Last occurrence of index 2
        REQUIRE(std::find(colA.begin(), colA.end(), 40) != colA.end());  // Index 3
        REQUIRE(std::find(colA.begin(), colA.end(), 60) != colA.end());  // Index 4

        // First occurrences should NOT be present
        REQUIRE(std::find(colA.begin(), colA.end(), 10) == colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 20) == colA.end());
    }
}

TEST_CASE("Drop Duplicates - Index-based with KeepFalse", "[drop_duplicates][index]") {
    auto idx = make_index_from_vector<int64_t>({1, 2, 1, 3, 2, 4});
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{10, 20, 30, 40, 50, 60}},
        {"A"}
    );

    SECTION("Drop all duplicates") {
        auto result = df.drop_duplicates(DropDuplicatesKeepPolicy::False);

        // Only indices 3 and 4 appear once, so we should have 2 rows
        REQUIRE(result.num_rows() == 2);
        REQUIRE(result.num_cols() == 1);

        auto colA = result["A"].to_vector<int64_t>();
        auto index_values = result.index()->to_vector<int64_t>();

        // Should only have values for unique indices
        REQUIRE(colA.size() == 2);
        REQUIRE(std::find(colA.begin(), colA.end(), 40) != colA.end());  // Index 3
        REQUIRE(std::find(colA.begin(), colA.end(), 60) != colA.end());  // Index 4

        // Check index values
        REQUIRE(std::find(index_values.begin(), index_values.end(), 3) != index_values.end());
        REQUIRE(std::find(index_values.begin(), index_values.end(), 4) != index_values.end());
        REQUIRE(std::find(index_values.begin(), index_values.end(), 1) == index_values.end());
        REQUIRE(std::find(index_values.begin(), index_values.end(), 2) == index_values.end());
    }
}

TEST_CASE("Drop Duplicates - Index-based with no duplicates", "[drop_duplicates][index]") {
    auto idx = make_index_from_vector<int64_t>({1, 2, 3, 4, 5});
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{10, 20, 30, 40, 50}},
        {"A"}
    );

    SECTION("Should return original DataFrame") {
        auto result = df.drop_duplicates();

        REQUIRE(result.num_rows() == 5);
        REQUIRE(result.num_cols() == 1);

        auto colA = result["A"].to_vector<int64_t>();
        REQUIRE(colA == std::vector<int64_t>{10, 20, 30, 40, 50});
    }
}

TEST_CASE("Drop Duplicates - Column-based with KeepFirst", "[drop_duplicates][columns]") {
    // Create DataFrame with duplicate rows based on column values
    auto idx = from_range(6);
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {
            {1, 2, 1, 3, 2, 4},     // Col A - has duplicates
            {100, 200, 100, 300, 200, 400}  // Col B - matches A's pattern
        },
        {"A", "B"}
    );

    SECTION("Keep first occurrence based on all columns") {
        auto result = df.drop_duplicates({}, DropDuplicatesKeepPolicy::First);

        // Rows 0 and 2 are identical (A=1, B=100)
        // Rows 1 and 4 are identical (A=2, B=200)
        // Should keep 4 unique rows
        REQUIRE(result.num_rows() == 4);
        REQUIRE(result.num_cols() == 2);
    }

    SECTION("Keep first occurrence based on subset of columns") {
        auto result = df.drop_duplicates({"A"}, DropDuplicatesKeepPolicy::First);

        // Based on column A only: 1, 2, 1, 3, 2, 4
        // Unique values: 1, 2, 3, 4
        REQUIRE(result.num_rows() == 4);

        auto colA = result["A"].to_vector<int64_t>();
        // Should contain one of each: 1, 2, 3, 4
        REQUIRE(std::find(colA.begin(), colA.end(), 1) != colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 2) != colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 3) != colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 4) != colA.end());
    }
}

TEST_CASE("Drop Duplicates - Column-based with KeepLast", "[drop_duplicates][columns]") {
    auto idx = from_range(6);
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{1, 2, 1, 3, 2, 4}},
        {"A"}
    );

    SECTION("Keep last occurrence") {
        auto result = df.drop_duplicates({"A"}, DropDuplicatesKeepPolicy::Last);

        REQUIRE(result.num_rows() == 4);

        // Should keep the last occurrence of each value
        auto index_values = result.index()->to_vector<int64_t>();

        // Last occurrence of 1 is at index 2
        // Last occurrence of 2 is at index 4
        // First/last of 3 is at index 3
        // First/last of 4 is at index 5
        REQUIRE(std::find(index_values.begin(), index_values.end(), 2) != index_values.end());
        REQUIRE(std::find(index_values.begin(), index_values.end(), 4) != index_values.end());
        REQUIRE(std::find(index_values.begin(), index_values.end(), 3) != index_values.end());
        REQUIRE(std::find(index_values.begin(), index_values.end(), 5) != index_values.end());

        // First occurrences should NOT be present
        REQUIRE(std::find(index_values.begin(), index_values.end(), 0) == index_values.end());
        REQUIRE(std::find(index_values.begin(), index_values.end(), 1) == index_values.end());
    }
}

TEST_CASE("Drop Duplicates - Column-based with KeepFalse", "[drop_duplicates][columns]") {
    auto idx = from_range(6);
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{1, 2, 1, 3, 2, 4}},
        {"A"}
    );

    SECTION("Drop all duplicates") {
        auto result = df.drop_duplicates({"A"}, DropDuplicatesKeepPolicy::False);

        // Only 3 and 4 appear once
        REQUIRE(result.num_rows() == 2);

        auto colA = result["A"].to_vector<int64_t>();
        REQUIRE(std::find(colA.begin(), colA.end(), 3) != colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 4) != colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 1) == colA.end());
        REQUIRE(std::find(colA.begin(), colA.end(), 2) == colA.end());
    }
}

TEST_CASE("Drop Duplicates - Multiple columns", "[drop_duplicates][columns]") {
    auto idx = from_range(7);
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {
            {1, 1, 2, 2, 1, 3, 3},      // Col A
            {10, 20, 10, 10, 10, 10, 10} // Col B
        },
        {"A", "B"}
    );

    SECTION("Drop duplicates based on both columns") {
        auto result = df.drop_duplicates({"A", "B"}, DropDuplicatesKeepPolicy::First);

        // Unique combinations of (A, B):
        // (1, 10) - rows 0 and 4
        // (1, 20) - row 1
        // (2, 10) - rows 2 and 3
        // (3, 10) - rows 5 and 6
        // Should have 4 unique combinations
        REQUIRE(result.num_rows() == 4);
    }

    SECTION("Drop duplicates based on single column A") {
        auto result = df.drop_duplicates({"A"}, DropDuplicatesKeepPolicy::First);

        // Unique values in A: 1, 2, 3
        REQUIRE(result.num_rows() == 3);
    }

    SECTION("Drop duplicates based on single column B") {
        auto result = df.drop_duplicates({"B"}, DropDuplicatesKeepPolicy::First);

        // Only 2 unique values in B: 10 and 20
        REQUIRE(result.num_rows() == 2);

        auto colB = result["B"].to_vector<int64_t>();
        REQUIRE(std::find(colB.begin(), colB.end(), 10) != colB.end());
        REQUIRE(std::find(colB.begin(), colB.end(), 20) != colB.end());
    }
}

TEST_CASE("Drop Duplicates - String index", "[drop_duplicates][index][string]") {
    auto idx = make_index_from_vector<std::string>({"a", "b", "a", "c", "b", "d"});
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{10, 20, 30, 40, 50, 60}},
        {"value"}
    );

    SECTION("Keep first with string index") {
        auto result = df.drop_duplicates(DropDuplicatesKeepPolicy::First);

        REQUIRE(result.num_rows() == 4);  // Unique: a, b, c, d

        auto values = result["value"].to_vector<int64_t>();
        REQUIRE(std::find(values.begin(), values.end(), 10) != values.end());  // First 'a'
        REQUIRE(std::find(values.begin(), values.end(), 20) != values.end());  // First 'b'
        REQUIRE(std::find(values.begin(), values.end(), 40) != values.end());  // 'c'
        REQUIRE(std::find(values.begin(), values.end(), 60) != values.end());  // 'd'
    }
}

TEST_CASE("Drop Duplicates - DateTime index", "[drop_duplicates][index][datetime]") {
    const int64_t day_ns = 86400000000000LL;
    const int64_t base_time = 1609459200000000000LL; // 2021-01-01

    std::vector<int64_t> times = {
        base_time,
        base_time + day_ns,
        base_time,              // Duplicate
        base_time + 2 * day_ns,
        base_time + day_ns,     // Duplicate
        base_time + 3 * day_ns
    };

    auto idx = make_datetime_index(times, "", "UTC");
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{10, 20, 30, 40, 50, 60}},
        {"value"}
    );

    SECTION("Keep first with datetime index") {
        auto result = df.drop_duplicates(DropDuplicatesKeepPolicy::First);

        REQUIRE(result.num_rows() == 4);  // 4 unique timestamps

        auto values = result["value"].to_vector<int64_t>();
        REQUIRE(std::find(values.begin(), values.end(), 10) != values.end());  // First base_time
        REQUIRE(std::find(values.begin(), values.end(), 20) != values.end());  // First base_time+day
        REQUIRE(std::find(values.begin(), values.end(), 40) != values.end());
        REQUIRE(std::find(values.begin(), values.end(), 60) != values.end());
    }
}

TEST_CASE("Drop Duplicates - Empty DataFrame", "[drop_duplicates][edge]") {
    auto idx = from_range(0);
    DataFrame df = make_dataframe<int64_t>(idx, {{}}, {"A"});

    SECTION("Empty DataFrame should return empty") {
        auto result = df.drop_duplicates();
        REQUIRE(result.num_rows() == 0);
        REQUIRE(result.num_cols() == 1);
    }
}

TEST_CASE("Drop Duplicates - Invalid column name", "[drop_duplicates][error]") {
    auto idx = from_range(3);
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{1, 2, 3}},
        {"A"}
    );

    SECTION("Should throw for non-existent column") {
        REQUIRE_THROWS_AS(
            df.drop_duplicates({"NonExistent"}),
            std::invalid_argument
        );
    }
}

TEST_CASE("Drop Duplicates - All rows identical", "[drop_duplicates][edge]") {
    auto idx = from_range(5);
    DataFrame df = make_dataframe<int64_t>(
        idx,
        {{1, 1, 1, 1, 1}},
        {"A"}
    );

    SECTION("Keep first should return 1 row") {
        auto result = df.drop_duplicates({"A"}, DropDuplicatesKeepPolicy::First);
        REQUIRE(result.num_rows() == 1);
        REQUIRE(result["A"].to_vector<int64_t>()[0] == 1);
    }

    SECTION("Keep last should return 1 row") {
        auto result = df.drop_duplicates({"A"}, DropDuplicatesKeepPolicy::Last);
        REQUIRE(result.num_rows() == 1);
        REQUIRE(result["A"].to_vector<int64_t>()[0] == 1);
    }

    SECTION("Keep false should return 0 rows") {
        auto result = df.drop_duplicates({"A"}, DropDuplicatesKeepPolicy::False);
        REQUIRE(result.num_rows() == 0);
    }
}

TEST_CASE("Drop Duplicates - Mixed data types", "[drop_duplicates][columns][mixed]") {
    auto idx = from_range(6);

    // Create DataFrame with mixed types
    auto table = arrow::Table::Make(
        arrow::schema({
            arrow::field("int_col", arrow::int64()),
            arrow::field("str_col", arrow::utf8())
        }),
        {
            make_contiguous_array<int64_t>({1, 2, 1, 3, 2, 4}).as_chunked_array(),
            make_contiguous_array<std::string>({"a", "b", "a", "c", "b", "d"}).as_chunked_array()
        }
    );

    DataFrame df(idx, table);

    SECTION("Drop duplicates based on both int and string columns") {
        auto result = df.drop_duplicates({"int_col", "str_col"}, DropDuplicatesKeepPolicy::First);

        // Rows 0 and 2 are identical (1, "a")
        // Rows 1 and 4 are identical (2, "b")
        REQUIRE(result.num_rows() == 4);
    }
}
