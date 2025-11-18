#include <iostream>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_all.hpp>

#include "epoch_frame/dataframe.h"
#include "epoch_frame/series.h"
#include "epoch_frame/scalar.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"

using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory;
using namespace epoch_frame;

TEST_CASE("GroupBy", "[groupby]") {
    auto idx = from_range(10);
    auto df = make_dataframe<int64_t>(
        idx,
        {{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        {10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
        {0, 0, 0, 0, 4, 4, 6, 8, 8, 8}},
        {"a", "b", "c"}
    );

    std::vector<std::pair<std::string, std::variant<std::vector<std::string>, arrow::ChunkedArrayVector>>> params{
                {"group by `c`", std::vector<std::string>{"c"}},
                {"group by `df[c]`", std::vector{df["c"].array()}}
    };

    for (auto const &[title, param] : params) {
        DYNAMIC_SECTION(title) {
            std::visit([&]<typename T>(T const& arg) {
                 DataFrame result = df.group_by_agg(arg).agg("sum");
                 REQUIRE(result.num_rows() == 4);
                 REQUIRE(result.num_cols() == (std::is_same_v<T, arrow::ChunkedArrayVector> ? 3 : 2));

                INFO(result);

                 REQUIRE(result.loc(0_scalar, "a").value<int64_t>() == 10);
                 REQUIRE(result.loc(4_scalar, "a").value<int64_t>() == 11);
                 REQUIRE(result.loc(6_scalar, "a").value<int64_t>() == 7);
                 REQUIRE(result.loc(8_scalar, "a").value<int64_t>() == 27);
                 REQUIRE(result.loc(0_scalar, "b").value<int64_t>() == 100);
                 REQUIRE(result.loc(4_scalar, "b").value<int64_t>() == 110);
                 REQUIRE(result.loc(6_scalar, "b").value<int64_t>() == 70);
                 REQUIRE(result.loc(8_scalar, "b").value<int64_t>() == 270);

                 if constexpr (std::is_same_v<T, arrow::ChunkedArrayVector>) {
                     REQUIRE(result.loc(0_scalar, "c").value<int64_t>() == 0);
                     REQUIRE(result.loc(4_scalar, "c").value<int64_t>() == 8);
                     REQUIRE(result.loc(6_scalar, "c").value<int64_t>() == 6);
                     REQUIRE(result.loc(8_scalar, "c").value<int64_t>() == 24);
                 }
            }, param);
        }
    }

    SECTION("Group by apply") {
        auto grouper = df.group_by_apply("c");

        SECTION("Validate Group") {
            const auto groups = grouper.groups();
            REQUIRE(groups.size() == 4);

            REQUIRE(groups[0].first.value<int64_t>() == 0);
            REQUIRE_THAT(std::vector(groups[0].second->raw_values(), groups[0].second->raw_values()+4), Catch::Matchers::Equals(std::vector<uint64_t>{0, 1, 2, 3}) );

            REQUIRE(groups[1].first.value<int64_t>() == 4);
            REQUIRE_THAT(std::vector(groups[1].second->raw_values(), groups[1].second->raw_values()+2), Catch::Matchers::Equals(std::vector<uint64_t>{4, 5}) );

            REQUIRE(groups[2].first.value<int64_t>() == 6);
            REQUIRE_THAT(std::vector(groups[2].second->raw_values(), groups[2].second->raw_values()+1), Catch::Matchers::Equals(std::vector<uint64_t>{6}) );

            REQUIRE(groups[3].first.value<int64_t>() == 8);
            REQUIRE_THAT(std::vector(groups[3].second->raw_values(), groups[3].second->raw_values()+3), Catch::Matchers::Equals(std::vector<uint64_t>{7, 8, 9}) );
        }
    }
}

TEST_CASE("Advanced GroupBy", "[groupby]") {
    auto idx = from_range(8);
    auto df = make_dataframe(
        idx,
        {{"foo"_scalar, "bar"_scalar, "foo"_scalar, "bar"_scalar, "foo"_scalar, "bar"_scalar, "foo"_scalar, "foo"_scalar},
        {"one"_scalar, "one"_scalar, "two"_scalar, "three"_scalar, "two"_scalar, "two"_scalar, "one"_scalar, "three"_scalar},
        {1.0_scalar, 2.0_scalar, 3.0_scalar, 4.0_scalar, 5.0_scalar, 6.0_scalar, 7.0_scalar, 8.0_scalar},
        {10_scalar, 20_scalar, 30_scalar, 40_scalar, 50_scalar, 60_scalar, 70_scalar, 80_scalar}},
        {arrow::field("a", arrow::utf8()),
        field("b", arrow::utf8()),
        field("c", arrow::float64()),
        field("d", arrow::int64())});

    SECTION("Multiple keys") {
        auto group = df.group_by_agg(std::vector<std::string>{"a", "b"});

        DataFrame result = group.agg("sum");
        INFO(result);

        REQUIRE(result.num_rows() == 6);
        REQUIRE(result.num_cols() == 2);
        INFO(result);

        std::vector<std::string> fields{"a", "b"};
        arrow::ScalarPtr locIndex = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("bar"), arrow::MakeScalar("one")}, fields).MoveValueUnsafe();
        REQUIRE(result.loc(Scalar{locIndex}, "c").value<double>() == 2.0);
        REQUIRE(result.loc(Scalar{locIndex}, "d").value<int64_t>() == 20);

        locIndex = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("bar"), arrow::MakeScalar("three")}, fields).MoveValueUnsafe();
        REQUIRE(result.loc(Scalar{locIndex}, "c").value<double>() == 4.0);
        REQUIRE(result.loc(Scalar{locIndex}, "d").value<int64_t>() == 40);

        locIndex = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("bar"), arrow::MakeScalar("two")}, fields).MoveValueUnsafe();
        REQUIRE(result.loc(Scalar{locIndex}, "c").value<double>() == 6.0);
        REQUIRE(result.loc(Scalar{locIndex}, "d").value<int64_t>() == 60);

        locIndex = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("foo"), arrow::MakeScalar("one")}, fields).MoveValueUnsafe();
        REQUIRE(result.loc(Scalar{locIndex}, "c").value<double>() == 8.0);
        REQUIRE(result.loc(Scalar{locIndex}, "d").value<int64_t>() == 80);

        locIndex = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("foo"), arrow::MakeScalar("three")}, fields).MoveValueUnsafe();
        REQUIRE(result.loc(Scalar{locIndex}, "c").value<double>() == 8.0);
        REQUIRE(result.loc(Scalar{locIndex}, "d").value<int64_t>() == 80);

        locIndex = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("foo"), arrow::MakeScalar("two")}, fields).MoveValueUnsafe();
        REQUIRE(result.loc(Scalar{locIndex}, "c").value<double>() == 8.0);
        REQUIRE(result.loc(Scalar{locIndex}, "d").value<int64_t>() == 80);
    }

    SECTION("Validate group") {
        auto grouper = df.group_by_apply(std::vector<std::string>{"a", "b"});

        SECTION("Validate Group") {
            const auto groups = grouper.groups();
            REQUIRE(groups.size() == 6);

            std::vector<std::string> fields{"a", "b"};

            arrow::ScalarPtr foo_one = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("foo"), arrow::MakeScalar("one")}, fields).MoveValueUnsafe();
            REQUIRE(groups[0].first == Scalar{foo_one});
            REQUIRE(groups[0].second->length() == 2);
            REQUIRE_THAT(std::vector(groups[0].second->raw_values(), groups[0].second->raw_values()+2), Catch::Matchers::Equals(std::vector<uint64_t>{0, 6}) );

            arrow::ScalarPtr bar_one = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("bar"), arrow::MakeScalar("one")}, fields).MoveValueUnsafe();
            REQUIRE(groups[1].first == Scalar{bar_one});
            REQUIRE(groups[1].second->length() == 1);
            REQUIRE_THAT(std::vector(groups[1].second->raw_values(), groups[1].second->raw_values()+1), Catch::Matchers::Equals(std::vector<uint64_t>{1}) );

            arrow::ScalarPtr foo_two = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("foo"), arrow::MakeScalar("two")}, fields).MoveValueUnsafe();
            REQUIRE(groups[2].first == Scalar{foo_two});
            REQUIRE(groups[2].second->length() == 2);
            REQUIRE_THAT(std::vector(groups[2].second->raw_values(), groups[2].second->raw_values()+2), Catch::Matchers::Equals(std::vector<uint64_t>{2, 4}) );

            arrow::ScalarPtr bar_three = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("bar"), arrow::MakeScalar("three")}, fields).MoveValueUnsafe();
            REQUIRE(groups[3].first == Scalar{bar_three});
            REQUIRE(groups[3].second->length() == 1);
            REQUIRE_THAT(std::vector(groups[3].second->raw_values(), groups[3].second->raw_values()+1), Catch::Matchers::Equals(std::vector<uint64_t>{3}) );

            arrow::ScalarPtr bar_two = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("bar"), arrow::MakeScalar("two")}, fields).MoveValueUnsafe();
            REQUIRE(groups[4].first == Scalar{bar_two});
            REQUIRE(groups[4].second->length() == 1);
            REQUIRE_THAT(std::vector(groups[4].second->raw_values(), groups[4].second->raw_values()+1), Catch::Matchers::Equals(std::vector<uint64_t>{5}) );

            arrow::ScalarPtr foo_three = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("foo"), arrow::MakeScalar("three")}, fields).MoveValueUnsafe();
            REQUIRE(groups[5].first == Scalar{foo_three});
            REQUIRE(groups[5].second->length() == 1);
            REQUIRE_THAT(std::vector(groups[5].second->raw_values(), groups[5].second->raw_values()+1), Catch::Matchers::Equals(std::vector<uint64_t>{7}) );
        }
    }

    SECTION("Apply Single Key") {
        SECTION("group_keys=true") {
            SECTION("scalar output") {
                auto result = df.group_by_apply("a").apply([](DataFrame const& x) {
                    return (x["c"] + x["d"]).sum();
                });
                REQUIRE(result.loc("foo"_scalar).value<double>() == 264);
                REQUIRE(result.loc("bar"_scalar).value<double>() == 132);
            }
            SECTION("series output") {
                auto result = df.group_by_apply("a").apply([](DataFrame const& x) {
                    return x["c"] + x["d"];
                });
                INFO(result);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"", 1_uscalar}}}).value<double>() == 22.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"", 3_uscalar}}}).value<double>() == 44.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"", 5_uscalar}}}).value<double>() == 66.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"", 0_uscalar}}}).value<double>() == 11.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"", 2_uscalar}}}).value<double>() == 33.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"", 4_uscalar}}}).value<double>() == 55.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"", 6_uscalar}}}).value<double>() == 77.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"", 7_uscalar}}}).value<double>() == 88.0);
            }
            SECTION("dataframe output") {
                auto result = df.group_by_apply("a").apply([](DataFrame const& x) {
                    return x.iloc({.start = -1});
                });

                const Scalar bar_5 = Scalar{{{"a", "bar"_scalar}, {"", 5_uscalar}}};
                const Scalar foo_7 = Scalar{{{"a", "foo"_scalar}, {"", 7_uscalar}}};

                REQUIRE(result.loc(bar_5, "a").value<std::string>().value() == "bar");
                REQUIRE(result.loc(bar_5, "b").value<std::string>().value() == "two");
                REQUIRE(result.loc(bar_5, "c").value<double>().value() == 6.0);
                REQUIRE(result.loc(bar_5, "d").value<int64_t>().value() == 60);
                REQUIRE(result.loc(foo_7, "a").value<std::string>().value() == "foo");
                REQUIRE(result.loc(foo_7, "b").value<std::string>().value() == "three");
                REQUIRE(result.loc(foo_7, "c").value<double>().value() == 8.0);
                REQUIRE(result.loc(foo_7, "d").value<int64_t>().value() == 80);
            }
        }

        SECTION("group_keys=false") {
            SECTION("scalar output") {
                auto result = df.group_by_apply("a", false).apply([](DataFrame const& x) {
                       return (x["c"] + x["d"]).sum();
                   });
                REQUIRE(result.loc("foo"_scalar).value<double>() == 264);
                REQUIRE(result.loc("bar"_scalar).value<double>() == 132);
            }
            SECTION("series output") {
                auto result = df.group_by_apply("a", false).apply([](DataFrame const& x) {
                     return x["c"] + x["d"];
                 });
                INFO(result);
                REQUIRE(result.loc(1_uscalar).value<double>() == 22.0);
                REQUIRE(result.loc(3_uscalar).value<double>() == 44.0);
                REQUIRE(result.loc(5_uscalar).value<double>() == 66.0);
                REQUIRE(result.loc(0_uscalar).value<double>() == 11.0);
                REQUIRE(result.loc(2_uscalar).value<double>() == 33.0);
                REQUIRE(result.loc(4_uscalar).value<double>() == 55.0);
                REQUIRE(result.loc(6_uscalar).value<double>() == 77.0);
                REQUIRE(result.loc(7_uscalar).value<double>() == 88.0);
            }
            SECTION("dataframe output") {
                auto result = df.group_by_apply("a", false).apply([](DataFrame const& x) {
                    return x.iloc({.start = -1});
                });

                REQUIRE(result.loc(5_uscalar, "a").value<std::string>().value() == "bar");
                REQUIRE(result.loc(5_uscalar, "b").value<std::string>().value() == "two");
                REQUIRE(result.loc(5_uscalar, "c").value<double>().value() == 6.0);
                REQUIRE(result.loc(5_uscalar, "d").value<int64_t>().value() == 60);
                REQUIRE(result.loc(7_uscalar, "a").value<std::string>().value() == "foo");
                REQUIRE(result.loc(7_uscalar, "b").value<std::string>().value() == "three");
                REQUIRE(result.loc(7_uscalar, "c").value<double>().value() == 8.0);
                REQUIRE(result.loc(7_uscalar, "d").value<int64_t>().value() == 80);
            }
        }
    }

    SECTION("Apply Multiple Keys") {
        SECTION("group_keys=true") {
            SECTION("scalar output") {
                auto result = df.group_by_apply(std::vector<std::string>{"a", "b"}).apply([](DataFrame const& x) {
                    return (x["c"] + x["d"]).sum();
                });
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "one"_scalar}}}).value<double>() == 88.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"b", "one"_scalar}}}).value<double>() == 22.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "two"_scalar}}}).value<double>() == 88.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"b", "three"_scalar}}}).value<double>() == 44.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"b", "two"_scalar}}}).value<double>() == 66.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "three"_scalar}}}).value<double>() == 88.0);
            }
            SECTION("series output") {
                auto result = df.group_by_apply(std::vector<std::string>{"a", "b"}).apply([](DataFrame const& x) {
                    return x["c"] + x["d"];
                });
                INFO(result);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "one"_scalar}, {"", 0_uscalar}}}).value<double>() == 11.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "one"_scalar}, {"", 6_uscalar}}}).value<double>() == 77.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"b", "one"_scalar}, {"", 1_uscalar}}}).value<double>() == 22.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "two"_scalar}, {"", 2_uscalar}}}).value<double>() == 33.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "two"_scalar}, {"", 4_uscalar}}}).value<double>() == 55.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"b", "three"_scalar}, {"", 3_uscalar}}}).value<double>() == 44.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"b", "two"_scalar}, {"", 5_uscalar}}}).value<double>() == 66.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "three"_scalar}, {"", 7_uscalar}}}).value<double>() == 88.0);
            }
            SECTION("dataframe output") {
                auto result = df.group_by_apply(std::vector<std::string>{"a", "b"}).apply([](DataFrame const& x) {
                    return x.iloc({.start = -1});
                });

                const Scalar foo_one_6 = Scalar{{{"a", "foo"_scalar}, {"b", "one"_scalar}, {"", 6_uscalar}}};
                const Scalar bar_one_1 = Scalar{{{"a", "bar"_scalar}, {"b", "one"_scalar}, {"", 1_uscalar}}};
                const Scalar foo_two_4 = Scalar{{{"a", "foo"_scalar}, {"b", "two"_scalar}, {"", 4_uscalar}}};
                const Scalar bar_three_3 = Scalar{{{"a", "bar"_scalar}, {"b", "three"_scalar}, {"", 3_uscalar}}};
                const Scalar bar_two_5 = Scalar{{{"a", "bar"_scalar}, {"b", "two"_scalar}, {"", 5_uscalar}}};
                const Scalar foo_three_7 = Scalar{{{"a", "foo"_scalar}, {"b", "three"_scalar}, {"", 7_uscalar}}};

                REQUIRE(result.loc(foo_one_6, "a").value<std::string>().value() == "foo");
                REQUIRE(result.loc(foo_one_6, "b").value<std::string>().value() == "one");
                REQUIRE(result.loc(foo_one_6, "c").value<double>().value() == 7.0);
                REQUIRE(result.loc(foo_one_6, "d").value<int64_t>().value() == 70);

                REQUIRE(result.loc(bar_one_1, "a").value<std::string>().value() == "bar");
                REQUIRE(result.loc(bar_one_1, "b").value<std::string>().value() == "one");
                REQUIRE(result.loc(bar_one_1, "c").value<double>().value() == 2.0);
                REQUIRE(result.loc(bar_one_1, "d").value<int64_t>().value() == 20);

                REQUIRE(result.loc(foo_two_4, "a").value<std::string>().value() == "foo");
                REQUIRE(result.loc(foo_two_4, "b").value<std::string>().value() == "two");
                REQUIRE(result.loc(foo_two_4, "c").value<double>().value() == 5.0);
                REQUIRE(result.loc(foo_two_4, "d").value<int64_t>().value() == 50);

                REQUIRE(result.loc(bar_three_3, "a").value<std::string>().value() == "bar");
                REQUIRE(result.loc(bar_three_3, "b").value<std::string>().value() == "three");
                REQUIRE(result.loc(bar_three_3, "c").value<double>().value() == 4.0);
                REQUIRE(result.loc(bar_three_3, "d").value<int64_t>().value() == 40);

                REQUIRE(result.loc(bar_two_5, "a").value<std::string>().value() == "bar");
                REQUIRE(result.loc(bar_two_5, "b").value<std::string>().value() == "two");
                REQUIRE(result.loc(bar_two_5, "c").value<double>().value() == 6.0);
                REQUIRE(result.loc(bar_two_5, "d").value<int64_t>().value() == 60);

                REQUIRE(result.loc(foo_three_7, "a").value<std::string>().value() == "foo");
                REQUIRE(result.loc(foo_three_7, "b").value<std::string>().value() == "three");
                REQUIRE(result.loc(foo_three_7, "c").value<double>().value() == 8.0);
                REQUIRE(result.loc(foo_three_7, "d").value<int64_t>().value() == 80);
            }
        }

        SECTION("group_keys=false") {
            SECTION("scalar output") {
                auto result = df.group_by_apply(std::vector<std::string>{"a", "b"}, false).apply([](DataFrame const& x) {
                    return (x["c"] + x["d"]).sum();
                });
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "one"_scalar}}}).value<double>() == 88.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"b", "one"_scalar}}}).value<double>() == 22.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "two"_scalar}}}).value<double>() == 88.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"b", "three"_scalar}}}).value<double>() == 44.0);
                REQUIRE(result.loc(Scalar{{{"a", "bar"_scalar}, {"b", "two"_scalar}}}).value<double>() == 66.0);
                REQUIRE(result.loc(Scalar{{{"a", "foo"_scalar}, {"b", "three"_scalar}}}).value<double>() == 88.0);
            }
            SECTION("series output") {
                auto result = df.group_by_apply(std::vector<std::string>{"a", "b"}, false).apply([](DataFrame const& x) {
                    return x["c"] + x["d"];
                });
                INFO(result);
                REQUIRE(result.loc(0_uscalar).value<double>() == 11.0);
                REQUIRE(result.loc(6_uscalar).value<double>() == 77.0);
                REQUIRE(result.loc(1_uscalar).value<double>() == 22.0);
                REQUIRE(result.loc(2_uscalar).value<double>() == 33.0);
                REQUIRE(result.loc(4_uscalar).value<double>() == 55.0);
                REQUIRE(result.loc(3_uscalar).value<double>() == 44.0);
                REQUIRE(result.loc(5_uscalar).value<double>() == 66.0);
                REQUIRE(result.loc(7_uscalar).value<double>() == 88.0);
            }
            SECTION("dataframe output") {
                auto result = df.group_by_apply(std::vector<std::string>{"a", "b"}, false).apply([](DataFrame const& x) {
                    return x.iloc({.start = -1});
                });

                REQUIRE(result.loc(6_uscalar, "a").value<std::string>().value() == "foo");
                REQUIRE(result.loc(6_uscalar, "b").value<std::string>().value() == "one");
                REQUIRE(result.loc(6_uscalar, "c").value<double>().value() == 7.0);
                REQUIRE(result.loc(6_uscalar, "d").value<int64_t>().value() == 70);

                REQUIRE(result.loc(1_uscalar, "a").value<std::string>().value() == "bar");
                REQUIRE(result.loc(1_uscalar, "b").value<std::string>().value() == "one");
                REQUIRE(result.loc(1_uscalar, "c").value<double>().value() == 2.0);
                REQUIRE(result.loc(1_uscalar, "d").value<int64_t>().value() == 20);

                REQUIRE(result.loc(4_uscalar, "a").value<std::string>().value() == "foo");
                REQUIRE(result.loc(4_uscalar, "b").value<std::string>().value() == "two");
                REQUIRE(result.loc(4_uscalar, "c").value<double>().value() == 5.0);
                REQUIRE(result.loc(4_uscalar, "d").value<int64_t>().value() == 50);

                REQUIRE(result.loc(3_uscalar, "a").value<std::string>().value() == "bar");
                REQUIRE(result.loc(3_uscalar, "b").value<std::string>().value() == "three");
                REQUIRE(result.loc(3_uscalar, "c").value<double>().value() == 4.0);
                REQUIRE(result.loc(3_uscalar, "d").value<int64_t>().value() == 40);

                REQUIRE(result.loc(5_uscalar, "a").value<std::string>().value() == "bar");
                REQUIRE(result.loc(5_uscalar, "b").value<std::string>().value() == "two");
                REQUIRE(result.loc(5_uscalar, "c").value<double>().value() == 6.0);
                REQUIRE(result.loc(5_uscalar, "d").value<int64_t>().value() == 60);

                REQUIRE(result.loc(7_uscalar, "a").value<std::string>().value() == "foo");
                REQUIRE(result.loc(7_uscalar, "b").value<std::string>().value() == "three");
                REQUIRE(result.loc(7_uscalar, "c").value<double>().value() == 8.0);
                REQUIRE(result.loc(7_uscalar, "d").value<int64_t>().value() == 80);
            }
        }
    }

    SECTION("Multiple aggregations with multiple keys") {
        auto group = df.group_by_agg(std::vector<std::string>{"a", "b"});

        std::vector<std::string> agg_names{"sum", "mean"};
        std::shared_ptr<arrow::compute::FunctionOptions> options = std::make_shared<arrow::compute::ScalarAggregateOptions>(true, 1);

        auto result_map = group.agg(agg_names, {options, options});

        REQUIRE(result_map.size() == 2);

        auto sum_result = result_map["sum"];
        INFO(sum_result);
        REQUIRE(sum_result.num_rows() == 6);
        REQUIRE(sum_result.num_cols() == 2);

        auto mean_result = result_map["mean"];
        INFO(mean_result);
        REQUIRE(mean_result.num_rows() == 6);
        REQUIRE(mean_result.num_cols() == 2);

        // Validate some results for sum
        arrow::ScalarPtr locIndex = arrow::StructScalar::Make(arrow::ScalarVector{arrow::MakeScalar("bar"), arrow::MakeScalar("one")}, {"a", "b"}).MoveValueUnsafe();
        REQUIRE(sum_result.loc(Scalar{locIndex}, "c").value<double>() == 2.0);
        REQUIRE(sum_result.loc(Scalar{locIndex}, "d").value<int64_t>() == 20);

        // Validate some results for mean
        REQUIRE(mean_result.loc(Scalar{locIndex}, "c").value<double>() == 2.0);
        REQUIRE(mean_result.loc(Scalar{locIndex}, "d").value<double>() == 20);
    }
}
