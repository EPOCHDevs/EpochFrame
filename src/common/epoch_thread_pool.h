#pragma once

#include <oneapi/tbb/task_arena.h>

namespace epoch_frame
{

    class EpochThreadPool
    {
      private:
        // Private constructor
        EpochThreadPool()
        {
            // Initialize with hardware concurrency
            arena.initialize();
        }

        // The task arena that manages our thread pool
        static tbb::task_arena arena;

      public:
        // Delete copy constructor and assignment
        EpochThreadPool(const EpochThreadPool&) = delete;
        void operator=(const EpochThreadPool&)  = delete;

        // Meyer's singleton implementation - thread safe by C++11 guarantee
        static EpochThreadPool& getInstance()
        {
            static EpochThreadPool instance;
            return instance;
        }

        // Execute work in our fixed thread pool
        template <typename F> void execute(F&& func)
        {
            arena.execute(std::forward<F>(func));
        }

        template <typename F> void enqueue(F&& func)
        {
            arena.enqueue(std::forward<F>(func));
        }

        // Get the number of threads in the pool
        int get_max_concurrency() const
        {
            return arena.max_concurrency();
        }
    };

    // Initialize the static arena with desired thread count
    inline tbb::task_arena EpochThreadPool::arena(tbb::task_arena::automatic);

} // namespace epoch_frame
