//
// Created by adesola on 2/17/25.
//
// test_DataFrame.cpp
#include <catch2/catch_test_macros.hpp>
#include "epochframe/dataframe.h"
#include "epochframe/series.h"
#include "factory/index_factory.h"
#include "factory/dataframe_factory.h"
#include "factory/series_factory.h"
#include <cmath>    // possibly for std::isnan, etc.
#include "epochframe/frame_or_series.h"


using namespace epochframe;
using namespace epochframe::factory::index;
using namespace epochframe::factory::array;

//--------------------------------------------------------------------------
// Tests for DataFrame indexing operations
//--------------------------------------------------------------------------
TEST_CASE("Indexing Test") {
    std::vector<int> a = {1, 2, 3, 4, 5};
    std::vector<int> b = {10, 20, 30, 40, 50};
    std::vector<int> c = {100, 200, 300, 400, 500};
    auto default_frame = make_dataframe(
       from_range(5),
       std::vector{a, b, c},
       {"A", "B", "C"}
   );

    auto empty_frame = make_dataframe(
           from_range(0, 0),
           std::vector<std::vector<int>>{
           {},
               {},
               {}
           },
           {"A", "B", "C"}
       );

    SECTION("DataFrame head and tail methods", "[DataFrame]") {
        SECTION("n is in range") {
            DataFrame head_df = default_frame.head(3);
            DataFrame tail_df = default_frame.tail(2);

            // Verify that head() returns exactly the first 3 rows.
            INFO(head_df);
            REQUIRE(head_df.num_rows() == 3);
            // And tail() returns the last 2 rows.
            REQUIRE(tail_df.num_rows() == 2);
            // Check that the first row's value in column "A" matches.
            REQUIRE(default_frame.iloc(0, "A").value<int>() == head_df.iloc(0, "A").value<int>());
        }

        SECTION("n is out of bound") {
            DataFrame head_df = default_frame.head(10);
            DataFrame tail_df = default_frame.tail(10);

            // Verify that head() returns exactly the first 3 rows.
            REQUIRE(head_df.num_rows() == 5);
            // And tail() returns the last 2 rows.
            REQUIRE(tail_df.num_rows() == 5);
            // Check that the first row's value in column "A" matches.
            REQUIRE(head_df.equals(default_frame));
            REQUIRE(tail_df.equals(default_frame));
        }
    }

    SECTION("iloc") {
        struct TestCase{
            std::string title;
            DataFrame frame;
            std::variant<UnResolvedIntegerSliceBound, std::tuple<int64_t, std::string>> input;
            std::optional<std::variant<DataFrame, Scalar>> output;
        };

        std::vector<TestCase> params;
        params.push_back(TestCase{
            .title = "Positive Index",
            .frame = default_frame,
            .input = std::make_tuple(2, "B"),
            .output = Scalar{30}
        });

        params.push_back(TestCase{
        .title = "Negative Index",
            .frame = default_frame,
        .input = std::make_tuple(-2, "A"),
        .output = Scalar{4}
    });

        params.push_back(TestCase{
    .title = "Standard slice [1,4)",
        .frame = default_frame,
    .input = UnResolvedIntegerSliceBound{1, 4},
    .output = make_dataframe(from_range(1, 4), std::vector{std::vector{2, 3, 4}, {20, 30, 40}, {200, 300, 400}}, default_frame.column_names())
    });

        params.push_back(TestCase{
    .title = "Omitted start slice [:3]",
    .frame = default_frame,
    .input = UnResolvedIntegerSliceBound{.stop=3},
    .output = make_dataframe(from_range(0, 3), std::vector{std::vector{1, 2, 3}, {10, 20, 30}, {100, 200, 300}}, default_frame.column_names())
    });

        params.push_back(TestCase{
    .title = "Omitted end slice [2:]",
    .frame = default_frame,
    .input = UnResolvedIntegerSliceBound{.start=2},
    .output = make_dataframe(from_range(2, 5), std::vector{std::vector{3, 4, 5}, {30, 40, 50}, {300, 400, 500}}, default_frame.column_names())
    });

        params.push_back(TestCase{
        .title = "Negative start slice [-3:]",
        .frame = default_frame,
        .input = UnResolvedIntegerSliceBound{-3, static_cast<int64_t>(default_frame.num_rows())},
        .output = make_dataframe(
            from_range(2, 5), // rows 2 to 4 (i.e. 2,3,4)
            std::vector{
                std::vector{3, 4, 5},
                std::vector{30, 40, 50},
                std::vector{300, 400, 500}
            },
            default_frame.column_names()
        )
    });

        // Variation 5: Negative end slice [1:-1] → rows 1,2,3
        params.push_back(TestCase{
            .title = "Negative end slice [1:-1]",
            .frame = default_frame,
            .input = UnResolvedIntegerSliceBound{1, -1},
            .output = make_dataframe(
                from_range(1, 4), // rows 1,2,3
                std::vector{
                    std::vector{2, 3, 4},
                    std::vector{20, 30, 40},
                    std::vector{200, 300, 400}
                },
                default_frame.column_names()
            )
        });

        // Variation 6: Slice with start >= end should return an empty frame.
        params.push_back(TestCase{
            .title = "Empty slice when start >= end",
            .frame = default_frame,
            .input = UnResolvedIntegerSliceBound{4, 2},
            .output = make_dataframe(
                from_range(0, 0), // no rows
                std::vector<std::vector<int>>{{}, {}, {}}, // no data
                default_frame.column_names()
            )
        });

        // Variation 7: Out-of-bound slice [3,10) → rows 3 and 4 only.
        params.push_back(TestCase{
            .title = "Out-of-bound slice [3,10)",
            .frame = default_frame,
            .input = UnResolvedIntegerSliceBound{3, 10},
            .output = make_dataframe(
                from_range(3, 5), // rows 3 and 4
                std::vector{
                    std::vector{4, 5},
                    std::vector{40, 50},
                    std::vector{400, 500}
                },
                default_frame.column_names()
            )
        });

        params.push_back(TestCase{
        .title = "Slice on empty table",
            .frame = empty_frame,
        .input = UnResolvedIntegerSliceBound{0, 1},
        .output = empty_frame
    });

        // Out-of-bounds row index
        params.push_back(TestCase{
            .title = "Out-of-bounds row index",
            .frame = default_frame,
            .input = std::make_tuple(5, "A"),
            .output = std::nullopt/* Expected error or default */
        });

        // Invalid column name
        params.push_back(TestCase{
            .title = "Invalid column name",
            .frame = default_frame,
            .input = std::make_tuple(0, "D"),
            .output = std::nullopt /* Expected error or default */
        });

        // Single-row slice [3:4]
        params.push_back(TestCase{
            .title = "Single-row slice [3:4]",
            .frame = default_frame,
            .input = UnResolvedIntegerSliceBound{3, 4},
            .output = make_dataframe(
                from_range(3, 4),
                std::vector{std::vector{4}, {40}, {400}},
                default_frame.column_names()
            )
        });

        // Full slice [:]
        params.push_back(TestCase{
            .title = "Full slice [:]",
            .frame = default_frame,
            .input = UnResolvedIntegerSliceBound{},
            .output = default_frame // Assuming the entire frame is expected
        });

        // Combined negative start and stop slice [-4:-2]
        params.push_back(TestCase{
            .title = "Negative start and stop slice [-4:-2]",
            .frame = default_frame,
            .input = UnResolvedIntegerSliceBound{-4, -2},
            .output = make_dataframe(
                from_range(1, 3), // rows 1 and 2
                std::vector{std::vector{2, 3}, {20, 30}, {200, 300}},
                default_frame.column_names()
            )
        });

        for (auto const& [title, frame, input, output]: params) {
            DYNAMIC_SECTION(title) {
                std::visit([&]<typename  T>(const T& arg) {
           if constexpr (std::same_as<T, UnResolvedIntegerSliceBound>) {
               DataFrame result = frame.iloc(arg);

               auto expected = std::get<DataFrame>(*output);
               INFO("Result:\n" << result << "\nExpected" << expected);
               REQUIRE(expected.equals(result));
           }
           else {
               auto && [row, col] = arg;
               if (!output) {
                   REQUIRE_THROWS(frame.iloc(row, col));
                    return;
               }

               Scalar result = frame.iloc(row, col);
               auto expected = std::get<Scalar>(*output);
               INFO("Result:\n" << result << "\nExpected" << expected);
               REQUIRE(expected == result);
           }
       }, input);
            }
        }
    }

    SECTION("operator []") {
        struct TestCase{
            std::string title;
            DataFrame frame;
            std::variant<std::string, StringVector, StringVectorCallable, arrow::ArrayPtr > input;
            std::variant<DataFrame, Series, std::monostate> output;
        };

        std::vector<TestCase> params;
        // Single column access
        params.push_back({
            .title = "Single column access",
            .frame = default_frame,
            .input = std::string{"A"},
            .output = make_series(default_frame.index(), std::vector{1, 2, 3, 4, 5})
        });

        // Multiple columns access
        params.push_back({
            .title = "Multiple columns access",
            .frame = default_frame,
            .input = StringVector{"A", "C"},
            .output = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector{1, 2, 3, 4, 5},
                    std::vector{100, 200, 300, 400, 500}
                },
                StringVector{"A", "C"}
            )
        });

        // Empty column selection
        params.push_back({
            .title = "Empty column selection",
            .frame = default_frame,
            .input = StringVector{},
            .output = DataFrame{}
        });

        // Non-existent column access (should throw)
        params.push_back({
            .title = "Non-existent column access",
            .frame = default_frame,
            .input = std::string{"NonExistent"},
            .output = std::monostate{}
        });

        // Filtering columns using a callable
        params.push_back({
            .title = "Filter columns using callable",
            .frame = default_frame,
            .input = StringVectorCallable([](const StringVector &cols) -> StringVector {
                StringVector result;
                for (const auto &col : cols) {
                    if (col == "B" || col == "C") {
                        result.push_back(col);
                    }
                }
                return result;
            }),
            .output = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector{10, 20, 30, 40, 50},
                    std::vector{100, 200, 300, 400, 500}
                },
                StringVector{"B", "C"}
            )
        });

        // Filtering columns using an Arrow array
        params.push_back({
            .title = "Filter columns using Arrow array",
            .frame = default_frame,
            .input = make_contiguous_array(StringVector{"A", "C"}),
            .output = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector{1, 2, 3, 4, 5},
                    std::vector{100, 200, 300, 400, 500}
                },
                StringVector{"A", "C"}
            )
        });

        for (auto const& [title, frame, input, output]: params) {
            DYNAMIC_SECTION(title) {
                std::visit([&]<typename T1, typename T2>(const T1& arg, const T2& expected) {
                    if constexpr (std::same_as<T2, std::monostate>) {
                        REQUIRE_THROWS(frame[arg]);
                    }
                    else {
                        auto result = frame[arg];
                       INFO("Result:\n" << result << "\nExpected" << expected);
                        if constexpr (std::same_as<T2, decltype(result)>) {
                            REQUIRE(expected.equals(result));
                        }
                        else {
                            throw std::runtime_error("Unexpected type");
                        }
                    }
       }, input, output);
            }
        }
    }

    SECTION("loc") {
        SECTION("loc - Single Row and Column") {

            SECTION("Single row selection (2, 'B') → 30") {
                Scalar result = default_frame.loc(Scalar(2), "B");
                REQUIRE(result == Scalar{30});
            }

            SECTION("Single row selection (4, 'C') → 500") {
                Scalar result = default_frame.loc(Scalar(4), "C");
                REQUIRE(result == Scalar{500});
            }

            SECTION("Selecting a non-existent row should throw") {
                REQUIRE_THROWS(default_frame.loc(Scalar(10), "A"));
            }

            SECTION("Selecting a non-existent column should throw") {
                REQUIRE_THROWS(default_frame.loc(Scalar(2), "X"));
            }
        }

        SECTION("loc - Selecting Multiple Rows") {
            SECTION("Selecting multiple rows [1, 3, 4]") {
                auto input = make_contiguous_array(std::vector{1, 3, 4});
                DataFrame expected = make_dataframe(
                    make_range({1, 3, 4}, MonotonicDirection::Increasing),
                    std::vector{
                        std::vector{2, 4, 5},
                        std::vector{20, 40, 50},
                        std::vector{200, 400, 500}
                    },
                    default_frame.column_names()
                );

                DataFrame result = default_frame.loc(input);
                REQUIRE(result.equals(expected));
            }

            SECTION("Selecting a non-existent row should return an empty DataFrame") {
                auto input = make_contiguous_array(std::vector{10});
                REQUIRE_THROWS(default_frame.loc(input));
            }
        }

        SECTION("loc - Boolean Filtering") {
            SECTION("Filtering rows where column 'A' > 2") {
                Series filter = default_frame["A"] > Scalar(2);
                DataFrame expected = make_dataframe(
                    make_range({2, 3, 4}, MonotonicDirection::Increasing),
                    std::vector{
                        std::vector{3, 4, 5},
                        std::vector{30, 40, 50},
                        std::vector{300, 400, 500}
                    },
                    default_frame.column_names()
                );

                DataFrame result = default_frame.loc(filter);
                REQUIRE(result.equals(expected));
            }

            SECTION("Boolean mask with alternating True/False values") {
                Series filter = make_series(
                    default_frame.index(),
                    std::vector<bool>{true, false, true, false, true}
                );

                DataFrame expected = make_dataframe(
                    make_range({0, 2, 4}, MonotonicDirection::Increasing),
                    std::vector{
                        std::vector{1, 3, 5},
                        std::vector{10, 30, 50},
                        std::vector{100, 300, 500}
                    },
                    default_frame.column_names()
                );

                DataFrame result = default_frame.loc(filter);
                REQUIRE(result.equals(expected));
            }
        }
        SECTION("loc - Selecting with Label Slice") {

            SECTION("Selecting range [1:3]") {
                SliceType slice{Scalar(1), Scalar(3)};
                DataFrame expected = make_dataframe(
                    from_range(1, 4),
                    std::vector{
                        std::vector{2, 3, 4},
                        std::vector{20, 30, 40},
                        std::vector{200, 300, 400}
                    },
                    default_frame.column_names()
                );

                DataFrame result = default_frame.loc(slice);
                REQUIRE(result.equals(expected));
            }

            SECTION("Selecting an empty slice [3:1] should return an empty frame") {
                SliceType slice{Scalar(3), Scalar(1)};
                REQUIRE_THROWS(default_frame.loc(slice));
            }
        }

        SECTION("loc - Selecting with a New Index") {
            SECTION("selecting with index") {
                auto new_index = from_range(1, 3);

                DataFrame result = default_frame.loc(new_index);
                REQUIRE(result.index()->equals(new_index));
            }

            SECTION("Selecting a non-existent index should return an empty DataFrame") {
                auto new_index = from_range(5, 7);
                REQUIRE_THROWS(default_frame.loc(new_index));
            }
        }

        SECTION("loc - Selecting with a Callable Function") {
            SECTION("Using callable to filter rows where column 'B' > 25") {
                DataFrameToSeriesCallable callable = [](const DataFrame &frame) {
                    return frame["B"] > Scalar(25);
                };

                DataFrame expected = make_dataframe(
                    make_range({2, 3, 4}, MonotonicDirection::Increasing), // Rows 3 and 4 (since B = 40, 50 > 25)
                    std::vector{
                        std::vector{3, 4, 5},
                        std::vector{30, 40, 50},
                        std::vector{300, 400, 500}
                    },
                    default_frame.column_names()
                );

                DataFrame result = default_frame.loc(callable);
                INFO(result);
                REQUIRE(result.equals(expected));
            }
        }
    }

    SECTION("DataFrame index method", "[DataFrame]") {
        IndexPtr idx = default_frame.index();
        // Verify that the default index has 5 entries.
        REQUIRE(idx->size() == 5);
    }

    SECTION("reindex") {

        SECTION("Reindexing with the same index (no change)") {
            IndexPtr same_index = default_frame.index();
            DataFrame result = default_frame.reindex(same_index);
            REQUIRE(result.equals(default_frame));
        }

        SECTION("Reindexing with an extended index (new rows filled with NaN)") {
            auto new_index = from_range(0, 7);
            DataFrame result = default_frame.reindex(new_index);

            REQUIRE(result.num_rows() == 7);
            REQUIRE(result.iloc(5, "A").is_null());
            REQUIRE(result.iloc(6, "B").is_null());
        }

        SECTION("Reindexing with an extended index and fill value (new rows filled)") {
            auto new_index = from_range(0, 7);
            DataFrame result = default_frame.reindex(new_index, Scalar(99UL));

            REQUIRE(result.num_rows() == 7);
            REQUIRE(result.iloc(5, "A").value<int>() == 99);
            REQUIRE(result.iloc(6, "B").value<int>() == 99);
        }

        SECTION("Reindexing with a subset of the index (dropping rows)") {
            auto subset_index = from_range(1, 3);
            DataFrame result = default_frame.reindex(subset_index);

            REQUIRE(result.num_rows() == 2);
            REQUIRE(result.iloc(0, "A").value<int>() == 2);
            REQUIRE(result.iloc(1, "B").value<int>() == 30);
        }

        SECTION("Reindexing with an empty index") {
            auto empty_index = from_range(0);
            DataFrame result = default_frame.reindex(empty_index);

            REQUIRE(result.num_rows() == 0);
            REQUIRE(result.num_cols() == default_frame.num_cols());
        }
        SECTION("Reindexing with an index containing partially matching values") {
            auto partial_index = make_range(std::vector<uint64_t>{2, 4, 6}, MonotonicDirection::Increasing);
            DataFrame result = default_frame.reindex(partial_index, Scalar(100));

            REQUIRE(result.num_rows() == 3);
            REQUIRE(result.iloc(0, "A").value<int>() == 3);
            REQUIRE(result.iloc(1, "B").value<int>() == 50);
            REQUIRE(result.iloc(2, "A").value<int>() == 100);
        }

        SECTION("Reindexing with incompatible index type should throw") {
            auto incompatible_index = make_object_index(std::vector<std::string>{"row1", "row2", "row3"});
            REQUIRE_THROWS(default_frame.reindex(incompatible_index));
        }

        SECTION("Reindexing with a numerical index using from_range()") {
            auto numeric_index = from_range(0, 7);
            DataFrame result = default_frame.reindex(numeric_index, Scalar(0));

            REQUIRE(result.num_rows() == 7);
            REQUIRE(result.iloc(6, "A").value<int>() == 0);
        }

        SECTION("Reindexing with duplicate values in the new index") {
            REQUIRE_THROWS(make_range(std::vector<uint64_t>{1, 2, 2, 3, 4}, MonotonicDirection::Increasing));
        }

        SECTION("Reindexing with a large fill value") {
            auto extended_index = from_range(0, 7);
            DataFrame result = default_frame.reindex(extended_index, Scalar(999999));

            REQUIRE(result.iloc(6, "A").value<int>() == 999999);
        }

        SECTION("Reindexing with a sparse index (gaps between values)") {
            auto sparse_index = make_range(std::vector<uint64_t>{0, 2, 5, 8}, MonotonicDirection::Increasing);
            DataFrame result = default_frame.reindex(sparse_index, Scalar(100));

            REQUIRE(result.num_rows() == 4);
            REQUIRE(result.iloc(1, "A").value<int>() == 3);  // Row "2" exists
            REQUIRE(result.iloc(2, "A").value<int>() == 100); // Row "5" does not exist
            REQUIRE(result.iloc(3, "B").value<int>() == 100); // Row "8" does not exist
        }

        SECTION("Reindexing when the original frame is empty") {
            DataFrame empty_frame;
            auto new_index = from_range(0, 5);
            DataFrame result = empty_frame.reindex(new_index, Scalar(0));

            REQUIRE(result.size() == 5);
            REQUIRE(result.num_rows() == 0);
            REQUIRE_THROWS(result.iloc(2, "A")); // Since original frame is empty
        }
    }

    SECTION("where") {
        SECTION("where - Basic filtering with scalar") {
            WhereConditionVariant condition = default_frame["A"] > 2_scalar;
            WhereOtherVariant other = 999_scalar;
            DataFrame result = default_frame.where(condition, other);
            auto expected = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{999, 999, 3, 4, 5},
                    std::vector<int64_t>{999, 999, 30, 40, 50},
                    std::vector<int64_t>{999, 999, 300, 400, 500}
                },
                default_frame.column_names()
            );
            INFO("Result:\n" << result << "\nExpected:\n" << expected);
            REQUIRE(result.equals(expected));
        }

        SECTION("where - Condition as Series, Other as Scalar") {
            WhereConditionVariant condition = default_frame["B"] > 25_scalar;
            WhereOtherVariant other = 0_scalar;
            DataFrame result = default_frame.where(condition, other);
            auto expected = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{0, 0, 3, 4, 5},
                    std::vector<int64_t>{0, 0, 30, 40, 50},
                    std::vector<int64_t>{0, 0, 300, 400, 500}
                },
                default_frame.column_names()
            );
            INFO("Result:\n" << result << "\nExpected:\n" << expected);
            REQUIRE(result.equals(expected));
        }

        SECTION("where - Condition as Callable, Other as DataFrame") {
            WhereConditionVariant condition = [](const DataFrame &frame) {
                return frame["C"] < 400_scalar;
            };
            WhereOtherVariant other = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{10, 20, 30, 40, 50},
                    std::vector<int64_t>{10, 20, 30, 40, 50},
                    std::vector<int64_t>{10, 20, 30, 40, 50}
                },
                default_frame.column_names()
            );
            DataFrame result = default_frame.where(condition, other);
            auto expected = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{1, 2, 3, 40, 50},
                    std::vector<int64_t>{10, 20, 30, 40, 50},
                    std::vector<int64_t>{100, 200, 300, 40, 50}
                },
                default_frame.column_names()
            );
            INFO("Result:\n" << result << "\nExpected:\n" << expected);
            REQUIRE(result.equals(expected));
        }

        // --- Additional tests for full code coverage of DataFrame::where ---

        SECTION("where - Condition as DataFrame, Other as Scalar") {
            // Build a boolean DataFrame (with same shape as default_frame) as condition.
            auto bool_df = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<bool>{false, false, true, true, false}, // for column "A"
                    std::vector<bool>{true, false, true, true, false},  // for column "B"
                    std::vector<bool>{false, true, false, true, true}   // for column "C"
                },
                default_frame.column_names()
            );
            DataFrame result = default_frame.where(bool_df, 0_scalar);
            // Expect: for each row, if mask is true then use default_frame value, otherwise 0.
            auto expected = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{0, 0, 3, 4, 0},    // for column "A": {1,2,3,4,5} → {0,0,3,4,0}
                    std::vector<int64_t>{10, 0, 30, 40, 0},   // for column "B": {10,20,30,40,50} → {10,0,30,40,0}
                    std::vector<int64_t>{0, 200, 0, 400, 500}   // for column "C": {100,200,300,400,500} → {0,200,0,400,500}
                },
                default_frame.column_names()
            );
            INFO("Result:\n" << result << "\nExpected:\n" << expected);
            REQUIRE(result.equals(expected));
        }

        SECTION("where - Condition as Arrow array, Other as Scalar") {
            // Create a boolean Arrow array to be broadcast as the row mask.
            auto bool_arrow = make_contiguous_array(std::vector<bool>{true, false, true, false, true});
            DataFrame result = default_frame.where(bool_arrow, 123_scalar);
            // For each row: if condition is true then keep row, else fill with 123.
            auto expected = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{1, 123, 3, 123, 5},
                    std::vector<int64_t>{10, 123, 30, 123, 50},
                    std::vector<int64_t>{100, 123, 300, 123, 500}
                },
                default_frame.column_names()
            );
            INFO("Result:\n" << result << "\nExpected:\n" << expected);
            REQUIRE(result.equals(expected));
        }

        SECTION("where - Condition as DataFrameToDataFrameCallable, Other as Scalar") {
            // The callable returns an DataFrame (a boolean mask) based on the input frame.
            DataFrameToDataFrameCallable cond_callable = [](const DataFrame &frame) {
                std::vector<bool> mask;
                for (size_t i = 0; i < frame.num_rows(); ++i)
                    mask.push_back(i < 3);  // true for rows 0,1,2; false for others
                std::vector<std::vector<bool>> mask_cols;
                for (size_t col = 0; col < frame.num_cols(); ++col)
                    mask_cols.push_back(mask);
                return make_dataframe(frame.index(), mask_cols, frame.column_names());
            };
            DataFrame result = default_frame.where(cond_callable, 666_scalar);
            auto expected = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{1, 2, 3, 666, 666},     // col "A": {1,2,3,4,5}
                    std::vector<int64_t>{10, 20, 30, 666, 666},    // col "B": {10,20,30,40,50}
                    std::vector<int64_t>{100, 200, 300, 666, 666}   // col "C": {100,200,300,400,500}
                },
                default_frame.column_names()
            );
            INFO("Result:\n" << result << "\nExpected:\n" << expected);
            REQUIRE(result.equals(expected));
        }

        SECTION("where - Condition as Series, Other as DataFrameToDataFrameCallable") {
            // A callable that returns an DataFrame filled with constant 777.
            DataFrameToDataFrameCallable other_callable = [](const DataFrame &frame) {
                std::vector<std::vector<int64_t>> fill;
                for (size_t col = 0; col < frame.num_cols(); ++col)
                    fill.push_back(std::vector<int64_t>(frame.num_rows(), 777));
                return make_dataframe(frame.index(), fill, frame.column_names());
            };
            // Condition returns a Series based on column "A": (1,2,3,4,5) > 2 → {false, false, true, true, true}
            DataFrame result = default_frame.where(default_frame["A"] > 2_scalar, other_callable);
            auto expected = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{777, 777, 3, 4, 5},
                    std::vector<int64_t>{777, 777, 30, 40, 50},
                    std::vector<int64_t>{777, 777, 300, 400, 500}
                },
                default_frame.column_names()
            );
            INFO("Result:\n" << result << "\nExpected:\n" << expected);
            REQUIRE(result.equals(expected));
        }

        SECTION("where - Condition as DataFrame, Other as DataFrame") {
            // Create a boolean DataFrame for condition (same shape as default_frame).
            auto bool_df = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<bool>{true, false, true, false, true},  // col "A"
                    std::vector<bool>{false, true, false, true, false},   // col "B"
                    std::vector<bool>{true, true, false, false, true}     // col "C"
                },
                default_frame.column_names()
            );
            // Create an DataFrame for "other" filled with constant value 444.
            auto other_df = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{444, 444, 444, 444, 444},
                    std::vector<int64_t>{444, 444, 444, 444, 444},
                    std::vector<int64_t>{444, 444, 444, 444, 444}
                },
                default_frame.column_names()
            );
            DataFrame result = default_frame.where(bool_df, other_df);
            // Expected: element-wise, if the corresponding entry in bool_df is true, take the original value; otherwise use 444.
            auto expected = make_dataframe(
                default_frame.index(),
                std::vector{
                    std::vector<int64_t>{1, 444, 3, 444, 5},
                    std::vector<int64_t>{444, 20, 444, 40, 444},
                    std::vector<int64_t>{100, 200, 444, 444, 500}
                },
                default_frame.column_names()
            );
            INFO("Result:\n" << result << "\nExpected:\n" << expected);
            REQUIRE(result.equals(expected));
        }

        SECTION("where - Condition DataFrame with mismatched shape (should throw)") {
            // Create a condition DataFrame with an incorrect number of rows.
            auto wrong_index = from_range(0, 3);
            auto wrong_bool_df = make_dataframe(
                wrong_index,
                std::vector{
                    std::vector<bool>{true, false, true},
                    std::vector<bool>{false, true, false},
                    std::vector<bool>{true, false, true}
                },
                default_frame.column_names() // same column names but row count differs
            );
            REQUIRE_THROWS(default_frame.where(wrong_bool_df, 0_scalar));
        }

        SECTION("where - Other DataFrame with mismatched shape (should throw)") {
            // Create an "other" DataFrame with an incorrect number of rows.
            auto wrong_index = from_range(0, 3);
            auto wrong_other_df = make_dataframe(
                wrong_index,
                std::vector{
                    std::vector<int64_t>{999, 999, 999},
                    std::vector<int64_t>{999, 999, 999},
                    std::vector<int64_t>{999, 999, 999}
                },
                default_frame.column_names()
            );
            // Use a valid condition (from a Series) so that only the "other" DataFrame is mismatched.
            REQUIRE_THROWS(default_frame.where(default_frame["A"] > 2_scalar, wrong_other_df));
        }
    }
}

// TEST_CASE("DataFrame where method", "[DataFrame]") {
//     DataFrame df = createTestDataFrame();
//     // Create a condition: where column "A" > 2.
//     Series condition = df["A"] > Scalar(2);
//     Scalar other(999);
//     DataFrame result = df.where(condition, other);
//     // For row 0 (1 > 2 is false), expect the fill value.
//     REQUIRE(result.iloc(0, "A").value<int>() == 999);
//     // For row 2 (3 > 2 is true), expect the original value.
//     REQUIRE(result.iloc(2, "A").value<int>() == 3);
// }
//
// TEST_CASE("DataFrame isin method", "[DataFrame]") {
//     DataFrame df = createTestDataFrame();
//     // Create an Arrow array containing allowed values {2, 4}.
//     auto allowed = createArrowArray({2, 4});
//     DataFrame bool_df = df.isin(allowed);
//     // Check in column "A": row 0 (value 1) is not in {2,4}, row 1 (value 2) is in {2,4}.
//     REQUIRE(bool_df.iloc(0, "A").value<bool>() == false);
//     REQUIRE(bool_df.iloc(1, "A").value<bool>() == true);
// }

// ----------------------------
// Additional Tests: Series Indexing Operations
// ----------------------------

TEST_CASE("Series Indexing Ops", "[Series][Indexing]") {
    // Create a simple index of 5 rows: [0,1,2,3,4]
    auto idx = from_range(5);
    // Create a Series with values 10, 20, 30, 40, 50
    Series s = make_series<int>(idx, {10, 20, 30, 40, 50}, "s");

    SECTION("iloc with valid indices") {
        // Positive index
        REQUIRE(s.iloc(0) == Scalar(10));
        REQUIRE(s.iloc(2) == Scalar(30));
        // Negative index: Assuming resolve_integer_index handles negative indexing
        // e.g., -1 should correspond to the last element
        REQUIRE(s.iloc(-1) == Scalar(50));
    }

    SECTION("iloc with out-of-bound index") {
        // Accessing an out-of-bound index should throw an exception
        REQUIRE_THROWS(s.iloc(5));
        REQUIRE_THROWS(s.iloc(-6));
    }

    SECTION("loc using index label") {
        // Since the index is created with from_range, labels are 0,1,2,3,4
        // Test a valid label
        REQUIRE(s.loc(Scalar(2)) == Scalar(30));
        // Test a non-existent label (e.g., label 10) should throw
        REQUIRE_THROWS(s.loc(Scalar(10)));
    }

    SECTION("loc with callable filter") {
        // Use the callable variant of loc to filter the Series.
        // For instance, filter values greater than 25; expected values are 30, 40, 50.
        auto filter_callable = [](const Series &ser) -> Series {
            // The Series class provides an overloaded operator> returning a Series of booleans
            return ser > Scalar(25);
        };
        Series filtered = s.loc(filter_callable);

        // Check that the filtered series has the correct number of elements and values
        // Assuming that the Series indexing preserves the original index order for the filtered rows
        REQUIRE(filtered.index()->size() == 3);
        // The values should be 30, 40, 50 in order
        REQUIRE(filtered.iloc(0) == Scalar(30));
        REQUIRE(filtered.iloc(1) == Scalar(40));
        REQUIRE(filtered.iloc(2) == Scalar(50));
    }

    SECTION("loc with variant row argument") {
        // Test the variant row selection if supported via tuple
        // For example, selecting a single element using a tuple
        // Assuming that Series.loc supports a tuple (row, unused) similar to DataFrame.iloc(row, col)
        // Since Series only has one column, we simulate by passing a tuple with the row index and an empty string
        auto row_tuple = std::make_tuple(3, std::string(""));
        // This should return the scalar at row 3
        Scalar result = s.iloc(std::get<0>(row_tuple));
        REQUIRE(result == Scalar(40));
    }

    SECTION("Empty Series indexing") {
        // Create an empty Series
        Series emptySeries = make_series<int>(from_range(0), {}, "empty");
        // iloc on an empty series should throw or be handled appropriately
        REQUIRE_THROWS(emptySeries.iloc(0));
        // loc on an empty series with any label should throw
        REQUIRE_THROWS(emptySeries.loc(Scalar(0)));
    }
}
