#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>

#include "epoch_frame/dataframe.h"
#include "epoch_frame/enums.h"
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/series_factory.h"
#include "epoch_frame/index.h"
#include "epoch_frame/serialization.h"
#include "epoch_frame/series.h"
#include "methods/time_grouper.h"

namespace efo = epoch_frame;
using namespace efo::factory::array;
using namespace efo::factory::offset;
using namespace efo::factory::index;
using namespace std::chrono_literals;

// Test parameters extracted from filenames
struct ResampleParams
{
    std::string timeframe;
    std::string label;
    std::string closed;
    std::string origin;

    // Constructor with defaults
    ResampleParams(std::string tf = "15min", std::string lbl = "left", std::string cls = "left",
                   std::string org = "default")
        : timeframe(tf), label(lbl), closed(cls), origin(org)
    {
    }

    // For descriptive test names
    std::string to_string() const
    {
        return "TF=" + timeframe + " label=" + label + " closed=" + closed + " origin=" + origin;
    }
};

// Convert string timeframe to proper offset
epoch_frame::DateOffsetHandlerPtr get_offset_from_timeframe(const std::string& tf)
{
    if (tf == "15min")
        return minutes(15);
    if (tf == "2hr")
        return hours(2);
    if (tf == "1D")
        return days(1);
    if (tf == "1W")
        return weeks(1, epoch_core::EpochDayOfWeek::Sunday);
    if (tf == "1ME")
        return month_end(1);
    if (tf == "1Quarter")
        return quarter_end(1);
    if (tf == "1Year")
        return year_end(1);

    // Default fallback
    return minutes(15);
}

// Convert string values to appropriate enums
epoch_core::GrouperLabelType get_label_type(const std::string& label)
{
    if (label == "right")
        return epoch_core::GrouperLabelType::Right;
    return epoch_core::GrouperLabelType::Left;
}

epoch_core::GrouperClosedType get_closed_type(const std::string& closed)
{
    if (closed == "right")
        return epoch_core::GrouperClosedType::Right;
    return epoch_core::GrouperClosedType::Left;
}

std::variant<epoch_frame::DateTime, epoch_core::GrouperOrigin>
get_origin_value(const std::string& origin)
{
    if (origin == "default")
    {
        return epoch_core::GrouperOrigin::StartDay;
    }
    if (origin == "epoch")
    {
        return epoch_core::GrouperOrigin::Epoch;
    }
    if (origin == "start")
    {
        return epoch_core::GrouperOrigin::Start;
    }
    if (origin == "start_day")
    {
        return epoch_core::GrouperOrigin::StartDay;
    }
    if (origin == "end")
    {
        return epoch_core::GrouperOrigin::End;
    }
    if (origin == "end_day")
    {
        return epoch_core::GrouperOrigin::EndDay;
    }
    // Try to parse as timestamp
    try
    {
        return efo::DateTime::from_str(origin);
    }
    catch (...)
    {
        // Fall back to default
        return epoch_core::GrouperOrigin::StartDay;
    }
}

// Extract parameters from filename
ResampleParams parse_filename(const std::string& filename)
{
    ResampleParams params;

    // Use regex to extract parameters from filename
    std::regex  pattern(R"(EURUSD_([^_]+)_label-([^_]+)_closed-([^_]+)_origin-([^\.]+)\.csv)");
    std::smatch matches;

    if (std::regex_search(filename, matches, pattern) && matches.size() == 5)
    {
        params.timeframe = matches[1].str();
        params.label     = matches[2].str();
        params.closed    = matches[3].str();
        params.origin    = matches[4].str();
    }

    return params;
}

// Load CSV data into a DataFrame
efo::DataFrame load_csv(const std::string&                filepath,
                        std::optional<std::string> const& format = std::nullopt)
{
    std::ifstream file(filepath);
    if (!file.is_open())
    {
        throw std::runtime_error("Failed to open file: " + filepath);
    }

    // Read header
    std::string header_line;
    std::getline(file, header_line);

    // Parse header to get column names
    std::vector<std::string> column_names;
    std::string              token;
    std::istringstream       header_stream(header_line);
    while (std::getline(header_stream, token, ','))
    {
        if (token != "Date")
        { // Skip the Date column
            column_names.push_back(token);
        }
    }

    // Read data
    std::vector<efo::DateTime>       dates;
    std::vector<std::vector<double>> columns(column_names.size());

    std::string line;
    while (std::getline(file, line))
    {
        std::istringstream line_stream(line);
        std::string        date_str;
        std::getline(line_stream, date_str, ',');

        // Parse date (assumes format: YYYY-MM-DD HH:MM:SS)
        efo::DateTime date;
        try
        {
            date = date_str.contains(' ') ? efo::DateTime::from_str(date_str, "", format)
                                          : efo::DateTime::from_date_str(date_str, "", format);
            dates.push_back(date);
        }
        catch (...)
        {
            continue; // Skip invalid dates
        }

        // Parse values
        for (size_t i = 0; i < column_names.size(); ++i)
        {
            std::string value_str;
            std::getline(line_stream, value_str, ',');

            try
            {
                double value = std::stod(value_str);
                columns[i].push_back(value);
            }
            catch (...)
            {
                columns[i].push_back(0.0); // Default for invalid values
            }
        }
    }

    // Create timestamp array for index
    std::vector<arrow::TimestampScalar> timestamps;
    for (const auto& date : dates)
    {
        timestamps.push_back(date.timestamp());
    }

    auto index = efo::factory::index::make_index(
        efo::factory::array::make_timestamp_array(timestamps), std::nullopt, "");

    // Create arrays for each column
    std::vector<arrow::ChunkedArrayPtr> data_arrays;
    for (const auto& column : columns)
    {
        data_arrays.push_back(efo::factory::array::make_array(column));
    }

    // Create DataFrame
    return efo::make_dataframe(index, data_arrays, column_names);
}

TEST_CASE("Test resampled files", "[resample_file_test]")
{
    auto init = []()
    {
        // Load the base file first
        std::string base_file_path =
            std::filesystem::current_path().string() + "/test_files/EURUSD_15M.csv";
        efo::DataFrame base_df;

        try
        {
            base_df = load_csv(base_file_path, "%Y.%m.%d %H:%M:%S");
            INFO("Base file loaded successfully with " << base_df.size() << " rows");
        }
        catch (const std::exception& e)
        {
            FAIL("Failed to load base file: " << e.what());
        }

        REQUIRE(!base_df.empty());

        // Find all resampled CSV files
        std::string test_files_dir = std::filesystem::current_path().string() + "/test_files";
        std::vector<std::string> csv_files;

        for (const auto& entry : std::filesystem::directory_iterator(test_files_dir))
        {
            if (entry.path().extension() == ".csv" &&
                entry.path().filename().string().find("EURUSD_") == 0 &&
                entry.path().filename().string() != "EURUSD_15M.csv")
            {
                csv_files.push_back(entry.path().string());
            }
        }

        INFO("Found " << csv_files.size() << " resampled CSV files");

        // Group files by timeframe for better test organization
        std::unordered_map<std::string, std::vector<std::pair<std::string, ResampleParams>>>
            files_by_timeframe;

        for (const auto& filepath : csv_files)
        {
            std::string    filename = std::filesystem::path(filepath).filename().string();
            ResampleParams params   = parse_filename(filename);
            files_by_timeframe[params.timeframe].push_back({filepath, params});
        }

        return std::make_tuple(base_df, files_by_timeframe);
    };

    static const auto [base_df, files_by_timeframe] = init();

    // Create test sections for each timeframe
    for (const auto& [timeframe, files] : files_by_timeframe)
    {
        SECTION("Timeframe: " + timeframe)
        {
            // Test each file within this timeframe
            for (const auto& [filepath, params] : files)
            {
                SECTION(params.to_string())
                {
                    // Load the resampled file
                    efo::DataFrame resampled_df;
                    try
                    {
                        resampled_df = load_csv(filepath);
                        INFO("Resampled file loaded successfully with " << resampled_df.size()
                                                                        << " rows");
                    }
                    catch (const std::exception& e)
                    {
                        SPDLOG_ERROR("Failed to load resampled file: {}", e.what());
                        continue;
                    }

                    REQUIRE(!resampled_df.empty());

                    // Set up the resampling options based on the parameters
                    efo::TimeGrouperOptions options;
                    options.freq   = get_offset_from_timeframe(params.timeframe);
                    options.label  = get_label_type(params.label);
                    options.closed = get_closed_type(params.closed);
                    options.origin = get_origin_value(params.origin);

                    // Apply resample to base dataframe
                    efo::DataFrame manually_resampled;
                    try
                    {
                        manually_resampled =
                            base_df.resample_by_ohlcv(options, {{"open", "Open"},
                                                                {"high", "High"},
                                                                {"low", "Low"},
                                                                {"close", "Close"},
                                                                {"volume", "Volume"}});
                        INFO("Manual resample successful with " << manually_resampled.size()
                                                                << " rows");
                    }
                    catch (const std::exception& e)
                    {
                        FAIL("Manual resample failed: " << e.what());
                        // Skip comparison for unsupported combinations
                        continue;
                    }

                    std::vector<std::string> columns = {"Open", "High", "Low", "Close", "Volume"};

                    // Compare results (shape, first few values)
                    INFO(resampled_df[columns].diff(manually_resampled[columns]));
                    REQUIRE(resampled_df[columns].equals(manually_resampled[columns]));
                }
            }
        }
    }
}
