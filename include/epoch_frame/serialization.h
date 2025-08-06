#pragma once

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/json/api.h>
#include <filesystem>
#include <optional>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <string>

#include "epoch_frame/dataframe.h"
#include "epoch_frame/frame_or_series.h"
#include "epoch_frame/series.h"

namespace epoch_frame
{

    // Options structs for each format

    struct CSVReadOptions
    {
        bool                                    infer_schema = true;
        char                                    delimiter    = ',';
        bool                                    has_header   = true;
        std::optional<std::string>              index_column = std::nullopt;
        std::optional<std::vector<std::string>> use_columns  = std::nullopt;
        std::optional<std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>> dtype =
            std::nullopt;
        bool ignore_empty_lines = true;
    };

    struct CSVWriteOptions
    {
        char                       delimiter      = ',';
        bool                       include_header = true;
        bool                       include_index  = true;
        std::optional<std::string> index_label    = std::nullopt;
    };

    struct ParquetReadOptions
    {
        std::optional<std::vector<int>> columns      = std::nullopt;
        std::optional<std::string>      index_column = std::nullopt;
    };

    struct ParquetWriteOptions
    {
        arrow::Compression::type   compression   = arrow::Compression::GZIP;
        bool                       include_index = true;
        std::optional<std::string> index_label   = std::nullopt;
    };

    struct JSONReadOptions
    {
        bool                                          lines = false; // True for line-delimited JSON
        std::optional<std::shared_ptr<arrow::Schema>> schema       = std::nullopt;
        std::optional<std::string>                    index_column = std::nullopt;
    };

    struct BinaryReadOptions
    {
        std::optional<std::string> index_column = std::nullopt;
    };

    struct BinaryWriteOptions
    {
        bool                                                        include_index = true;
        std::optional<std::string>                                  index_label   = std::nullopt;
        std::optional<std::unordered_map<std::string, std::string>> metadata      = std::nullopt;
    };

    struct ArrowReadOptions
    {
        std::optional<std::vector<int>> columns      = std::nullopt;
        std::optional<std::string>      index_column = std::nullopt;
    };

    struct ArrowWriteOptions
    {
        bool                                                        include_index = true;
        std::optional<std::string>                                  index_label   = std::nullopt;
        std::optional<std::unordered_map<std::string, std::string>> metadata      = std::nullopt;
    };

    // Function declarations for different I/O operations

    // CSV operations
    arrow::Result<DataFrame> read_csv(const std::string&    csv_content,
                                      const CSVReadOptions& options = {});
    arrow::Result<DataFrame> read_csv_file(const std::string&    file_path,
                                           const CSVReadOptions& options = {});
    arrow::Status            write_csv(const FrameOrSeries& data, std::string& output,
                                       const CSVWriteOptions& options = {});
    arrow::Status            write_csv_file(const FrameOrSeries& data, const std::string& file_path,
                                            const CSVWriteOptions& options = {});

    // JSON operations
    // Arrow implementation
    arrow::Result<DataFrame> read_json(const std::string&     json_content,
                                       const JSONReadOptions& options = {});
    arrow::Result<DataFrame> read_json_file(const std::string&     file_path,
                                            const JSONReadOptions& options = {});

    // Parquet operations
    arrow::Result<DataFrame> read_parquet(const std::string&        file_path,
                                          const ParquetReadOptions& options = {});
    arrow::Status            write_parquet(const FrameOrSeries& data, const std::string& file_path,
                                           const ParquetWriteOptions& options = {});

    // Binary operations
    arrow::Result<DataFrame> read_binary(const std::vector<uint8_t>& data,
                                         const BinaryReadOptions&    options = {});
    arrow::Result<DataFrame> read_buffer(const std::shared_ptr<arrow::Buffer>& buffer,
                                         const BinaryReadOptions&              options = {});
    arrow::Status            write_binary(const FrameOrSeries& data, std::vector<uint8_t>& output,
                                          const BinaryWriteOptions& options = {});
    arrow::Status write_buffer(const FrameOrSeries& data, std::shared_ptr<arrow::Buffer>& buffer,
                               const BinaryWriteOptions& options = {});
    arrow::Status write_buffer(const FrameOrSeries&                     data,
                               std::shared_ptr<arrow::ResizableBuffer>& buffer,
                               const BinaryWriteOptions&                options = {});

    // Arrow format operations
    arrow::Result<DataFrame> read_arrow(const std::string&      file_path,
                                        const ArrowReadOptions& options = {});
    arrow::Status            write_arrow(const FrameOrSeries& data, const std::string& file_path,
                                         const ArrowWriteOptions& options = {});

    // Utility functions
    bool                                                  is_s3_path(const std::string& path);
    arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> get_s3_filesystem();
    std::pair<std::string, std::string>                   parse_s3_path(const std::string& path);

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    get_input_stream(const std::string& path);
    arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
    get_output_stream(const std::string& path);

    struct ScopedS3
    {
        ScopedS3();
        ~ScopedS3();
    };

} // namespace epoch_frame
