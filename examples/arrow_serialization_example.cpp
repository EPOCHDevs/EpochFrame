#include <iostream>
#include <string>

#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/serialization.h"

int main()
{
    using namespace epoch_frame;

    std::cout << "=== EpochFrame Arrow Format Serialization Example ===" << std::endl;

    // Create a sample DataFrame
    auto index = factory::index::from_range(0, 4);

    arrow::FieldVector col_names = {
        arrow::field("Name", arrow::utf8()), arrow::field("Age", arrow::int64()),
        arrow::field("City", arrow::utf8()), arrow::field("Salary", arrow::int64())};

    std::vector<std::vector<Scalar>> data = {
        {"John"_scalar, "Anna"_scalar, "Peter"_scalar, "Linda"_scalar},
        {28_scalar, 34_scalar, 29_scalar, 42_scalar},
        {"New York"_scalar, "Boston"_scalar, "San Francisco"_scalar, "Chicago"_scalar},
        {75000_scalar, 85000_scalar, 92000_scalar, 78000_scalar}};

    auto df = make_dataframe(index, data, col_names);

    std::cout << "Original DataFrame:" << std::endl;
    std::cout << df << std::endl;

    // Write to Arrow format
    std::string arrow_file = "example_data.arrow";

    ArrowWriteOptions write_options;
    write_options.include_index = true;
    write_options.index_label   = "id";
    write_options.metadata      = {{"author", "EpochFrame"},
                                   {"version", "1.0"},
                                   {"description", "Sample data for Arrow format demonstration"}};

    auto write_status = write_arrow(df, arrow_file, write_options);
    if (write_status.ok())
    {
        std::cout << "\nSuccessfully wrote DataFrame to " << arrow_file << std::endl;
    }
    else
    {
        std::cout << "\nFailed to write DataFrame: " << write_status.ToString() << std::endl;
        return 1;
    }

    // Read from Arrow format
    ArrowReadOptions read_options;
    read_options.index_column = "id";

    auto read_result = read_arrow(arrow_file, read_options);
    if (read_result.ok())
    {
        auto read_df = read_result.ValueOrDie();
        std::cout << "\nRead DataFrame from Arrow format:" << std::endl;
        std::cout << read_df << std::endl;

        // Verify data integrity
        if (read_df.equals(df))
        {
            std::cout << "\n✓ Data integrity verified - read DataFrame matches original!"
                      << std::endl;
        }
        else
        {
            std::cout << "\n✗ Data integrity check failed!" << std::endl;
        }
    }
    else
    {
        std::cout << "\nFailed to read DataFrame: " << read_result.status().ToString() << std::endl;
        return 1;
    }

    // Demonstrate reading with column selection
    std::cout << "\n=== Reading with column selection ===" << std::endl;
    ArrowReadOptions select_options;
    select_options.index_column = "id";
    select_options.columns      = {0, 2}; // Only read Name and City columns

    auto select_result = read_arrow(arrow_file, select_options);
    if (select_result.ok())
    {
        auto selected_df = select_result.ValueOrDie();
        std::cout << "DataFrame with selected columns (Name, City):" << std::endl;
        std::cout << selected_df << std::endl;
    }

    // Clean up
    std::filesystem::remove(arrow_file);
    std::cout << "\nCleaned up temporary file: " << arrow_file << std::endl;

    std::cout << "\n=== Arrow Format Serialization Example Complete ===" << std::endl;
    return 0;
}