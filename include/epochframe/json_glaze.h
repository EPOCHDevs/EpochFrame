#pragma once

#include <glaze/glaze.hpp>
#include "epochframe/dataframe.h"
#include "epochframe/series.h"
#include "epochframe/serialization.h"

namespace glz {
    template<>
    struct meta<epochframe::Series> {
        static constexpr auto read = [](epochframe::Series &x, const std::vector<uint8_t> &input) {
            // Convert binary data to Series using read_binary
            auto read_options = epochframe::BinaryReadOptions{.index_column = "index"};
            auto dataframe = epochframe::read_binary(input, read_options);
            // Ensure we have just one column for Series
            if (dataframe.num_cols() != 1) {
                throw std::runtime_error("Series must have only one column");
            }
            // Convert the DataFrame to a Series
            x = dataframe.to_series();
        };

        static constexpr auto write = [](const epochframe::Series &x) -> auto {
            // Convert Series to binary data
            std::vector<uint8_t> binary_output;
            epochframe::BinaryWriteOptions write_options{
                .include_index = true,
                .index_label = "index"
            };
            // Convert to binary using the write_binary function
            epochframe::write_binary(x, binary_output, write_options);
            return binary_output;
        };

        static constexpr auto value = glz::custom<read, write>;
    };

    template<>
    struct meta<epochframe::DataFrame> {
        static constexpr auto read = [](epochframe::DataFrame &x, const std::vector<uint8_t> &input) {
            // Convert binary data to DataFrame using read_binary
            auto read_options = epochframe::BinaryReadOptions{.index_column = "index"};
            x = epochframe::read_binary(input, read_options);
        };

        static constexpr auto write = [](const epochframe::DataFrame &x) -> auto {
            // Convert DataFrame to binary data
            std::vector<uint8_t> binary_output;
            epochframe::BinaryWriteOptions write_options{
                .include_index = true,
                .index_label = "index"
            };
            // Convert to binary using the write_binary function
            epochframe::write_binary(x, binary_output, write_options);
            return binary_output;
        };

        static constexpr auto value = glz::custom<read, write>;
    };
} 