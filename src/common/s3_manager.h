//
// Created by adesola on 3/21/25.
//

#pragma once
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/s3fs.h>

namespace epoch_frame
{
    // Global S3 manager class for handling initialization/finalization
    class S3Manager
    {
      private:
        std::shared_ptr<arrow::fs::S3FileSystem> s3fs_;
        S3Manager();

      public:
        static S3Manager& Instance();

        // Get S3 filesystem, initializing if necessary
        arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> get_filesystem();

        // Initialize S3 if not already initialized
        void initialize();
        // Manual finalization (should rarely be needed)
        void finalize();
    };
} // namespace epoch_frame
