#pragma once

#include <vector>
#include <cstdint>
#include <string>

namespace epochframe::datetime {

// A placeholder constant for nanosecond resolution (NPY_FR_ns).
// Adjust this value as needed.
    constexpr int NPY_FR_ns = 0;

/**
 * @brief Given a vector of int64_t representing a datetime index, return a vector
 * of booleans indicating whether each timestamp is at the start or end of the month/quarter/year.
 *
 * The function checks the specified field (e.g. "is_month_start", "is_quarter_end", etc.)
 * using additional parameters:
 *   - freq_name: an optional frequency name (empty string if not provided)
 *   - month_kw: an optional integer (default 12) that influences the month boundary logic
 *   - reso: an integer representing the resolution (default is NPY_FR_ns)
 *
 * @param dtindex   A vector of int64_t values representing datetime values.
 * @param field     A string specifying the field to check. Expected values include:
 *                  "is_month_start", "is_quarter_start", "is_year_start",
 *                  "is_month_end", "is_quarter_end", "is_year_end".
 * @param freq_name An optional frequency name; if not provided, defaults to the empty string.
 * @param month_kw  An optional integer (default 12) representing the month keyword.
 * @param reso      An integer representing the datetime resolution; defaults to NPY_FR_ns.
 * @return std::vector<bool> A vector of booleans where each element corresponds to a value in dtindex.
 */
    std::vector<bool> get_start_end_field(
            const std::vector<std::int64_t> &dtindex,
            const std::string &field,
            const std::string &freq_name = "",
            int month_kw = 12,
            int reso = NPY_FR_ns
    );

}
