#include "epoch_frame/serialization.h"
#include "common/asserts.h"
#include "epoch_frame/factory/index_factory.h"
#include <arrow/buffer.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <atomic>
#include <fmt/format.h>
#include <mutex>
#include <spdlog/spdlog.h>

#include "s3_manager.h"
#include <arrow/compute/api.h>
#include <sstream>

namespace epoch_frame
{
    // Global S3 manager class for handling initialization/finalization

    // Initialize S3 if not already initialized
    void S3Manager::initialize()
    {
        if (arrow::fs::IsS3Initialized())
        {
            return;
        }

        arrow::fs::S3GlobalOptions s3_options;

        // Use proper AWS_LOG_LEVEL environment variable
        auto log_level_str = getenv("AWS_LOG_LEVEL");
        if (log_level_str)
        {
            std::string level(log_level_str);
            if (level == "off")
                s3_options.log_level = arrow::fs::S3LogLevel::Off;
            else if (level == "fatal")
                s3_options.log_level = arrow::fs::S3LogLevel::Fatal;
            else if (level == "error")
                s3_options.log_level = arrow::fs::S3LogLevel::Error;
            else if (level == "warn")
                s3_options.log_level = arrow::fs::S3LogLevel::Warn;
            else if (level == "info")
                s3_options.log_level = arrow::fs::S3LogLevel::Info;
            else if (level == "debug")
                s3_options.log_level = arrow::fs::S3LogLevel::Debug;
            else if (level == "trace")
                s3_options.log_level = arrow::fs::S3LogLevel::Trace;
            else
                s3_options.log_level = arrow::fs::S3LogLevel::Error;
        }
        else
        {
            s3_options.log_level = arrow::fs::S3LogLevel::Error;
        }

        if (auto status = arrow::fs::InitializeS3(s3_options); !status.ok())
        {
            SPDLOG_ERROR("Failed to initialize S3: {}", status.ToString());
        }
    }

    S3Manager::S3Manager()
    {
        initialize();
    }

    void S3Manager::finalize()
    {
        if (auto status = arrow::fs::EnsureS3Finalized(); !status.ok())
        {
            SPDLOG_ERROR("Failed to finalize S3: {}", status.ToString());
        }
    }

    S3Manager& S3Manager::Instance()
    {
        static S3Manager s3_manager;
        return s3_manager;
    }

    // Get S3 filesystem, initializing if necessary
    arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> S3Manager::get_filesystem()
    {
        // Initialize if needed
        if (arrow::fs::IsS3Initialized() && s3fs_)
        {
            return s3fs_;
        }

        // Create S3 filesystem
        arrow::fs::S3Options options = arrow::fs::S3Options::Defaults();

        // Configure credentials
        const auto aws_access_key = getenv("AWS_ACCESS_KEY_ID");
        const auto aws_secret_key = getenv("AWS_SECRET_ACCESS_KEY");
        const auto aws_region     = getenv("AWS_REGION");

        if (aws_access_key && aws_secret_key)
        {
            options.ConfigureAccessKey(aws_access_key, aws_secret_key);
        }
        else
        {
            options.ConfigureDefaultCredentials();
        }

        if (aws_region)
        {
            options.region = aws_region;
        }

        // Create filesystem
        return arrow::fs::S3FileSystem::Make(options);
    }

    // Utility functions
    bool is_s3_path(const std::string& path)
    {
        return path.substr(0, 5) == "s3://";
    }

    // Parse S3 path into bucket and key
    std::pair<std::string, std::string> parse_s3_path(const std::string& path)
    {
        if (!is_s3_path(path))
        {
            throw std::invalid_argument("Not an S3 path: " + path);
        }

        // Remove s3:// prefix
        std::string path_without_prefix = path.substr(5);

        // Split by first '/'
        auto first_slash = path_without_prefix.find('/');
        if (first_slash == std::string::npos)
        {
            // Just a bucket with no key
            return {path_without_prefix, ""};
        }
        else
        {
            // Bucket and key
            std::string bucket = path_without_prefix.substr(0, first_slash);
            std::string key    = path_without_prefix.substr(first_slash + 1);
            return {bucket, key};
        }
    }

    // Get S3FileSystem - main function to use in get_* functions
    arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> get_s3_filesystem()
    {
        return S3Manager::Instance().get_filesystem();
    }

    // Get appropriate filesystem based on path (S3 or local)
    arrow::Result<std::shared_ptr<arrow::fs::FileSystem>>
    get_filesystem_for_path(const std::string& path)
    {
        if (is_s3_path(path))
        {
            return get_s3_filesystem();
        }
        else
        {
            return std::make_shared<arrow::fs::LocalFileSystem>();
        }
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    get_input_stream(const std::string& path)
    {
        if (is_s3_path(path))
        {
            // Get S3 filesystem using the S3Manager

            ARROW_ASSIGN_OR_RAISE(auto s3fs, get_s3_filesystem());

            // Parse the S3 URI to get bucket and key
            auto [bucket, key] = parse_s3_path(path);

            // Open the file
            std::shared_ptr<arrow::io::RandomAccessFile> file;
            ARROW_ASSIGN_OR_RAISE(file, s3fs->OpenInputFile(bucket + "/" + key));

            return file;
        }
        else
        {
            // Local file
            std::shared_ptr<arrow::io::RandomAccessFile> file;
            ARROW_ASSIGN_OR_RAISE(file, arrow::io::ReadableFile::Open(path));
            return file;
        }
    }

    arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
    get_output_stream(const std::string& path)
    {
        if (is_s3_path(path))
        {
            // Get S3 filesystem using the S3Manager
            ARROW_ASSIGN_OR_RAISE(auto s3fs, get_s3_filesystem());

            // Parse the S3 URI to get bucket and key
            auto [bucket, key] = parse_s3_path(path);

            // Open the file
            ARROW_ASSIGN_OR_RAISE(auto file_result, s3fs->OpenOutputStream(bucket + "/" + key));

            return file_result;
        }
        else
        {
            // Local file
            ARROW_ASSIGN_OR_RAISE(auto file_result, arrow::io::FileOutputStream::Open(path));
            return file_result;
        }
    }

    // Helper function to extract index column if specified
    std::pair<std::shared_ptr<arrow::Table>, arrow::ArrayPtr>
    extract_index_column(std::shared_ptr<arrow::Table>     table,
                         const std::optional<std::string>& index_column)
    {

        arrow::ArrayPtr index_array = nullptr;

        if (index_column)
        {
            auto index_pos = table->schema()->GetFieldIndex(*index_column);
            if (index_pos != -1)
            {
                // Extract the index column
                auto chunked_index = table->column(index_pos);
                if (chunked_index->num_chunks() == 1)
                {
                    index_array = chunked_index->chunk(0);
                }
                else
                {
                    // Combine chunks if necessary
                    arrow::ArrayVector chunks;
                    for (int i = 0; i < chunked_index->num_chunks(); i++)
                    {
                        chunks.push_back(chunked_index->chunk(i));
                    }
                    auto result = arrow::Concatenate(chunks);
                    if (!result.ok())
                    {
                        throw std::runtime_error(std::format(
                            "Failed to concatenate index chunks: {}", result.status().ToString()));
                    }
                    index_array = result.ValueOrDie();
                }

                // Remove the index column from the table
                auto result = table->RemoveColumn(index_pos);
                if (!result.ok())
                {
                    throw std::runtime_error(std::format("Failed to remove index column: {}",
                                                         result.status().ToString()));
                }
                table = result.ValueOrDie();
            }
            else
            {
                SPDLOG_WARN("Specified index column '{}' not found in table", *index_column);
            }
        }

        return {table, index_array};
    }

    // CSV Operations
    arrow::Result<DataFrame> read_csv(const std::string& csv_content, const CSVReadOptions& options)
    {
        // Create memory input stream from the CSV content
        auto buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(csv_content.data()), csv_content.size());
        auto input = std::make_shared<arrow::io::BufferReader>(buffer);

        // Configure CSV reading options
        arrow::csv::ReadOptions read_options   = arrow::csv::ReadOptions::Defaults();
        read_options.skip_rows                 = 0;
        read_options.autogenerate_column_names = !options.has_header;

        arrow::csv::ParseOptions parse_options = arrow::csv::ParseOptions::Defaults();
        parse_options.delimiter                = options.delimiter;

        arrow::csv::ConvertOptions convert_options = arrow::csv::ConvertOptions::Defaults();
        if (options.dtype)
        {
            for (const auto& [col, type] : *options.dtype)
            {
                convert_options.column_types[col] = type;
            }
        }

        if (options.use_columns)
        {
            convert_options.include_columns = *options.use_columns;
        }

        // Create CSV reader
        ARROW_ASSIGN_OR_RAISE(auto csv_reader, arrow::csv::TableReader::Make(
                                                   arrow::io::default_io_context(), input,
                                                   read_options, parse_options, convert_options));

        // Read the CSV data
        ARROW_ASSIGN_OR_RAISE(auto table, csv_reader->Read());

        // Extract index column if specified
        arrow::ArrayPtr index_array = nullptr;
        if (options.index_column)
        {
            auto [new_table, extracted_index] = extract_index_column(table, options.index_column);
            table                             = new_table;
            index_array                       = extracted_index;
        }

        // Create and return the DataFrame
        if (index_array)
        {
            auto index = factory::index::make_index(index_array, std::nullopt, "");
            return DataFrame(index, table);
        }
        else
        {
            return DataFrame(table);
        }
    }

    arrow::Result<DataFrame> read_csv_file(const std::string&    file_path,
                                           const CSVReadOptions& options)
    {
        ARROW_ASSIGN_OR_RAISE(auto input, get_input_stream(file_path));

        // Configure CSV reading options
        arrow::csv::ReadOptions read_options   = arrow::csv::ReadOptions::Defaults();
        read_options.skip_rows                 = 0;
        read_options.autogenerate_column_names = !options.has_header;

        arrow::csv::ParseOptions parse_options = arrow::csv::ParseOptions::Defaults();
        parse_options.delimiter                = options.delimiter;

        arrow::csv::ConvertOptions convert_options = arrow::csv::ConvertOptions::Defaults();
        if (options.dtype)
        {
            for (const auto& [col, type] : *options.dtype)
            {
                convert_options.column_types[col] = type;
            }
        }

        if (options.use_columns)
        {
            convert_options.include_columns = *options.use_columns;
        }

        // Create CSV reader
        ARROW_ASSIGN_OR_RAISE(auto csv_reader, arrow::csv::TableReader::Make(
                                                   arrow::io::default_io_context(), input,
                                                   read_options, parse_options, convert_options));

        // Read the CSV data
        ARROW_ASSIGN_OR_RAISE(auto table, csv_reader->Read());

        // Extract index column if specified
        arrow::ArrayPtr index_array = nullptr;
        if (options.index_column)
        {
            auto [new_table, extracted_index] = extract_index_column(table, options.index_column);
            table                             = new_table;
            index_array                       = extracted_index;
        }

        // Create and return the DataFrame
        if (index_array)
        {
            auto index = factory::index::make_index(index_array, std::nullopt, "");
            return DataFrame(index, table);
        }
        else
        {
            return DataFrame(table);
        }
    }

    arrow::Status write_csv(const FrameOrSeries& data, std::string& output,
                            const CSVWriteOptions& options)
    {
        // Convert to DataFrame if Series
        auto df = data.is_frame() ? data.frame() : data.series().to_frame();

        // Add index as a column if requested
        std::shared_ptr<arrow::Table> table_to_write;
        if (options.include_index)
        {
            auto        index_array = df.index()->array().value();
            std::string index_name  = options.index_label.value_or("index");

            arrow::FieldVector fields = df.table()->schema()->fields();
            fields.insert(fields.begin(), arrow::field(index_name, index_array->type()));

            arrow::ChunkedArrayVector columns = df.table()->columns();
            columns.insert(columns.begin(), arrow::ChunkedArray::Make({index_array}).ValueOrDie());

            table_to_write = arrow::Table::Make(arrow::schema(fields), columns);
        }
        else
        {
            table_to_write = df.table();
        }

        // Create a buffer output stream using Arrow's API
        auto buffer_result = arrow::AllocateResizableBuffer(0);
        if (!buffer_result.ok())
        {
            throw std::runtime_error(
                std::format("Failed to allocate buffer: {}", buffer_result.status().ToString()));
        }

        // Convert the unique_ptr to a shared_ptr
        std::shared_ptr<arrow::ResizableBuffer> resizable_buffer =
            std::move(buffer_result).ValueOrDie();

        // Create the BufferOutputStream with the shared_ptr
        auto output_stream = std::make_shared<arrow::io::BufferOutputStream>(resizable_buffer);

        // Configure CSV writing options
        arrow::csv::WriteOptions write_options;
        write_options.include_header = options.include_header;
        write_options.delimiter      = options.delimiter;

        // Write the table to CSV
        ARROW_RETURN_NOT_OK(
            arrow::csv::WriteCSV(*table_to_write, write_options, output_stream.get()));

        // Finish writing and get the buffer
        ARROW_ASSIGN_OR_RAISE(auto buffer, output_stream->Finish());

        // Convert buffer to string and assign to output
        output.assign(reinterpret_cast<const char*>(buffer->data()), buffer->size());
        return arrow::Status::OK();
    }

    arrow::Status write_csv_file(const FrameOrSeries& data, const std::string& file_path,
                                 const CSVWriteOptions& options)
    {
        // Convert to DataFrame if Series
        auto df = data.is_frame() ? data.frame() : data.series().to_frame();

        // Add index as a column if requested
        std::shared_ptr<arrow::Table> table_to_write;
        if (options.include_index)
        {
            auto        index_array = df.index()->array().value();
            std::string index_name  = options.index_label.value_or("index");

            arrow::FieldVector fields = df.table()->schema()->fields();
            fields.insert(fields.begin(), arrow::field(index_name, index_array->type()));

            arrow::ChunkedArrayVector columns = df.table()->columns();
            columns.insert(columns.begin(), arrow::ChunkedArray::Make({index_array}).ValueOrDie());

            table_to_write = arrow::Table::Make(arrow::schema(fields), columns);
        }
        else
        {
            table_to_write = df.table();
        }

        // Get output stream for the file
        ARROW_ASSIGN_OR_RAISE(auto output_stream, get_output_stream(file_path));

        // Configure CSV writing options
        arrow::csv::WriteOptions write_options;
        write_options.include_header = options.include_header;
        write_options.delimiter      = options.delimiter;

        // Write the table to CSV
        ARROW_RETURN_NOT_OK(
            arrow::csv::WriteCSV(*table_to_write, write_options, output_stream.get()));

        // Close the output stream
        return output_stream->Close();
    }

    // JSON Operations

    // New implementation using Arrow's JSON parser
    arrow::Result<DataFrame> read_json(const std::string&     json_content,
                                       const JSONReadOptions& options)
    {
        if (json_content.empty())
        {
            return DataFrame();
        }
        try
        {
            // Create an input stream from the JSON content
            auto buffer = std::make_shared<arrow::Buffer>(
                reinterpret_cast<const uint8_t*>(json_content.data()), json_content.size());
            auto input = std::make_shared<arrow::io::BufferReader>(buffer);

            // Configure JSON reading options
            arrow::json::ReadOptions read_options = arrow::json::ReadOptions::Defaults();
            read_options.block_size               = 1024 * 1024; // 1MB blocks

            arrow::json::ParseOptions parse_options = arrow::json::ParseOptions::Defaults();
            // Allow newlines in strings, etc.
            parse_options.newlines_in_values = false;

            // Create JSON reader
            ARROW_ASSIGN_OR_RAISE(
                auto reader, arrow::json::TableReader::Make(arrow::default_memory_pool(), input,
                                                            read_options, parse_options));

            // Read the JSON data
            ARROW_ASSIGN_OR_RAISE(auto table, reader->Read());

            // Extract index column if specified
            arrow::ArrayPtr index_array = nullptr;
            if (options.index_column)
            {
                auto [new_table, extracted_index] =
                    extract_index_column(table, options.index_column);
                table       = new_table;
                index_array = extracted_index;
            }

            // Create and return the DataFrame
            if (index_array)
            {
                auto index = factory::index::make_index(index_array, std::nullopt, "");
                return DataFrame(index, table);
            }
            return DataFrame(table);
        }
        catch (const std::exception& e)
        {
            return arrow::Status::Invalid(
                std::format("Failed to read JSON with Arrow: {}", e.what()));
        }
    }

    arrow::Result<DataFrame> read_json_file(const std::string&     file_path,
                                            const JSONReadOptions& options)
    {
        try
        {
            ARROW_ASSIGN_OR_RAISE(auto input, get_input_stream(file_path));

            // Configure JSON reading options
            arrow::json::ReadOptions read_options = arrow::json::ReadOptions::Defaults();
            read_options.block_size               = 1024 * 1024; // 1MB blocks

            arrow::json::ParseOptions parse_options = arrow::json::ParseOptions::Defaults();
            parse_options.newlines_in_values        = true;

            // Create JSON reader
            ARROW_ASSIGN_OR_RAISE(
                auto reader, arrow::json::TableReader::Make(arrow::default_memory_pool(), input,
                                                            read_options, parse_options));

            // Read the JSON data
            ARROW_ASSIGN_OR_RAISE(auto table, reader->Read());

            // Extract index column if specified
            arrow::ArrayPtr index_array = nullptr;
            if (options.index_column)
            {
                auto [new_table, extracted_index] =
                    extract_index_column(table, options.index_column);
                table       = new_table;
                index_array = extracted_index;
            }

            // Create and return the DataFrame
            if (index_array)
            {
                auto index = factory::index::make_index(index_array, std::nullopt, "");
                return DataFrame(index, table);
            }
            else
            {
                return DataFrame(table);
            }
        }
        catch (const std::exception& e)
        {
            return arrow::Status::Invalid(
                std::format("Failed to read JSON file with Arrow: {}", e.what()));
        }
    }

    // Parquet Operations
    arrow::Result<DataFrame> read_parquet(const std::string&        file_path,
                                          const ParquetReadOptions& options)
    {
        ARROW_ASSIGN_OR_RAISE(auto input, get_input_stream(file_path));

        // Configure parquet reader
        ARROW_ASSIGN_OR_RAISE(auto parquet_reader,
                              parquet::arrow::OpenFile(input, arrow::default_memory_pool()));

        // Read the full table or selected columns
        std::shared_ptr<arrow::Table> table;
        if (options.columns)
        {
            ARROW_RETURN_NOT_OK(parquet_reader->ReadTable(*options.columns, &table));
        }
        else
        {
            ARROW_RETURN_NOT_OK(parquet_reader->ReadTable(&table));
        }

        // Extract index column if specified
        arrow::ArrayPtr index_array = nullptr;
        if (options.index_column)
        {
            auto [new_table, extracted_index] = extract_index_column(table, options.index_column);
            table                             = new_table;
            index_array                       = extracted_index;
        }

        // Create and return the DataFrame
        if (index_array)
        {
            auto index = factory::index::make_index(index_array, std::nullopt, "");
            return DataFrame(index, table);
        }
        else
        {
            return DataFrame(table);
        }
    }

    arrow::Status write_parquet(const FrameOrSeries& data, const std::string& file_path,
                                const ParquetWriteOptions& options)
    {
        // Convert to DataFrame if Series
        auto df = data.is_frame() ? data.frame() : data.series().to_frame();

        // Add index as a column if requested
        std::shared_ptr<arrow::Table> table_to_write;
        if (options.include_index)
        {
            auto        index_array = df.index()->array().value();
            std::string index_name  = options.index_label.value_or("index");

            arrow::FieldVector fields = df.table()->schema()->fields();
            fields.insert(fields.begin(), arrow::field(index_name, index_array->type()));

            arrow::ChunkedArrayVector columns = df.table()->columns();
            columns.insert(columns.begin(), arrow::ChunkedArray::Make({index_array}).ValueOrDie());

            table_to_write = arrow::Table::Make(arrow::schema(fields), columns);
        }
        else
        {
            table_to_write = df.table();
        }

        // Get output stream for the file
        ARROW_ASSIGN_OR_RAISE(auto output_stream, get_output_stream(file_path));

        // Configure parquet writing options
        std::shared_ptr<parquet::WriterProperties> props =
            parquet::WriterProperties::Builder().compression(options.compression)->build();

        // Write the table to parquet
        ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table_to_write,
                                                       arrow::default_memory_pool(), output_stream,
                                                       table_to_write->num_rows()));

        // Close the output stream
        return output_stream->Close();
    }

    // Binary Operations
    arrow::Result<DataFrame> read_binary(const std::vector<uint8_t>& data,
                                         const BinaryReadOptions&    options)
    {
        auto buffer = std::make_shared<arrow::Buffer>(data.data(), data.size());
        return read_buffer(buffer, options);
    }

    arrow::Result<DataFrame> read_buffer(const std::shared_ptr<arrow::Buffer>& buffer,
                                         const BinaryReadOptions&              options)
    {
        auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

        // Open the IPC stream reader
        ARROW_ASSIGN_OR_RAISE(auto reader,
                              arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));

        // Read all record batches
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());

        // Extract index column if specified
        arrow::ArrayPtr index_array = nullptr;
        if (options.index_column)
        {
            auto [new_table, extracted_index] = extract_index_column(table, options.index_column);
            table                             = new_table;
            index_array                       = extracted_index;
        }

        // Create and return the DataFrame
        if (index_array)
        {
            auto index =
                factory::index::make_index(index_array, std::nullopt, options.index_column.value());
            return DataFrame(index, table);
        }
        else
        {
            return DataFrame(table);
        }
    }

    arrow::Status write_binary(const FrameOrSeries& data, std::vector<uint8_t>& output,
                               const BinaryWriteOptions& options)
    {
        // Create a buffer to write to
        std::shared_ptr<arrow::ResizableBuffer> buffer;
        ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateResizableBuffer(0));

        // Convert from unique_ptr to shared_ptr
        ARROW_RETURN_NOT_OK(write_buffer(data, buffer, options));

        // Copy data to output vector
        output.resize(buffer->size());
        std::memcpy(output.data(), buffer->data(), buffer->size());
        return arrow::Status::OK();
    }

    // Add this overload for ResizableBuffer
    arrow::Status write_buffer(const FrameOrSeries&                     data,
                               std::shared_ptr<arrow::ResizableBuffer>& buffer,
                               const BinaryWriteOptions&                options)
    {
        // ResizableBuffer is-a Buffer, so we can just cast and call the other version
        std::shared_ptr<arrow::Buffer> buf = buffer;
        ARROW_RETURN_NOT_OK(write_buffer(data, buf, options));
        // The buffer may have been replaced, so update the input parameter
        buffer = std::dynamic_pointer_cast<arrow::ResizableBuffer>(buf);
        return arrow::Status::OK();
    }

    arrow::Status write_buffer(const FrameOrSeries& data, std::shared_ptr<arrow::Buffer>& buffer,
                               const BinaryWriteOptions& options)
    {
        // Convert to DataFrame if Series
        auto df = data.is_frame() ? data.frame() : data.series().to_frame();

        // Add index as a column if requested
        std::shared_ptr<arrow::Table> table_to_write;
        if (options.include_index)
        {
            auto        index_array = df.index()->array().value();
            std::string index_name  = options.index_label.value_or("index");

            arrow::FieldVector fields = df.table()->schema()->fields();
            fields.insert(fields.begin(), arrow::field(index_name, index_array->type()));

            arrow::ChunkedArrayVector columns = df.table()->columns();
            ARROW_ASSIGN_OR_RAISE(auto array, arrow::ChunkedArray::Make({index_array}));
            columns.insert(columns.begin(), array);

            table_to_write = arrow::Table::Make(arrow::schema(fields), columns);
        }
        else
        {
            table_to_write = df.table();
        }

        // Create a buffer output stream
        auto resizable_buffer = std::dynamic_pointer_cast<arrow::ResizableBuffer>(buffer);
        if (!resizable_buffer)
        {
            ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateResizableBuffer(0));

            // Convert from unique_ptr to shared_ptr
            resizable_buffer = std::dynamic_pointer_cast<arrow::ResizableBuffer>(buffer);
        }

        // Create the BufferOutputStream with the shared_ptr
        auto output_stream = std::make_shared<arrow::io::BufferOutputStream>(resizable_buffer);

        // Create metadata if needed
        std::shared_ptr<arrow::KeyValueMetadata> kv_metadata;
        if (options.metadata)
        {
            std::vector<std::string> keys, values;
            for (const auto& [key, value] : *options.metadata)
            {
                keys.push_back(key);
                values.push_back(value);
            }
            kv_metadata = arrow::KeyValueMetadata::Make(keys, values);
        }

        // Create record batch writer
        auto schema = table_to_write->schema();
        if (kv_metadata)
        {
            schema = schema->WithMetadata(kv_metadata);
        }

        ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(output_stream, schema));

        // Convert table to record batches for writing
        arrow::TableBatchReader reader(table_to_write);

        std::shared_ptr<arrow::RecordBatch> batch;
        while (true)
        {
            ARROW_ASSIGN_OR_RAISE(batch, reader.Next());
            if (batch == nullptr)
            {
                break; // No more batches
            }

            ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
        }

        // Close the writer
        ARROW_RETURN_NOT_OK(writer->Close());

        // Close the output stream
        ARROW_RETURN_NOT_OK(output_stream->Close());

        // Update the buffer with the finished stream
        ARROW_ASSIGN_OR_RAISE(buffer, output_stream->Finish());
        return arrow::Status::OK();
    }

    ScopedS3::ScopedS3()
    {
        S3Manager::Instance().initialize();
    }

    ScopedS3::~ScopedS3()
    {
        S3Manager::Instance().finalize();
    }

} // namespace epoch_frame
