//
// Created by adesola on 3/21/25.
//

#pragma once
#include <filesystem>
#include <arrow/filesystem/s3fs.h>
#include <arrow/filesystem/localfs.h>

namespace epoch_frame {
// Global S3 manager class for handling initialization/finalization
class S3Manager {
private:
     std::atomic<bool> s3_initialized_;
     std::shared_ptr<arrow::fs::S3FileSystem> s3fs_;

    // Check if initialized
    bool is_initialized() const;

    S3Manager();

public:
    static S3Manager& Instance();

    ~S3Manager();
    // Get S3 filesystem, initializing if necessary
    arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> get_filesystem();

    // Initialize S3 if not already initialized
    arrow::Status initialize();
    // Manual finalization (should rarely be needed)
    arrow::Status finalize();

};
}
