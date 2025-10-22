#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <sstream>
#include <glaze/glaze.hpp>

namespace sql_benchmark {

struct BenchmarkMetadata {
    std::string data_type;
    int rows = 0;
    std::string category;
    bool timezone_sensitive = false;
    int enum_columns = 0;
};

struct BenchmarkResult {
    std::string name;
    double mean_ms = 0.0;
    double median_ms = 0.0;
    double std_dev_ms = 0.0;
    double min_ms = 0.0;
    double max_ms = 0.0;
    int samples = 0;
    std::string timestamp;
    BenchmarkMetadata metadata;
};

struct BenchmarkFile {
    std::string version = "1.0";
    int64_t updated = 0; // nanoseconds since epoch
    std::vector<BenchmarkResult> benchmarks;
};

class BenchmarkWriter {
public:
    void add_result(const std::string& name,
                    const std::vector<double>& timings_ms,
                    const BenchmarkMetadata& metadata) {
        if (timings_ms.empty()) return;

        BenchmarkResult result;
        result.name = name;
        result.samples = timings_ms.size();
        result.metadata = metadata;

        // Calculate statistics
        result.mean_ms = std::accumulate(timings_ms.begin(), timings_ms.end(), 0.0) / timings_ms.size();

        std::vector<double> sorted = timings_ms;
        std::sort(sorted.begin(), sorted.end());

        size_t mid = sorted.size() / 2;
        if (sorted.size() % 2 == 0) {
            result.median_ms = (sorted[mid-1] + sorted[mid]) / 2.0;
        } else {
            result.median_ms = sorted[mid];
        }

        result.min_ms = sorted.front();
        result.max_ms = sorted.back();

        // Standard deviation
        double sq_sum = 0.0;
        for (const auto& val : timings_ms) {
            sq_sum += (val - result.mean_ms) * (val - result.mean_ms);
        }
        result.std_dev_ms = std::sqrt(sq_sum / timings_ms.size());

        // ISO 8601 timestamp
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        std::tm tm_utc;
        gmtime_r(&time_t_now, &tm_utc);

        std::ostringstream oss;
        oss << std::put_time(&tm_utc, "%Y-%m-%dT%H:%M:%SZ");
        result.timestamp = oss.str();

        results.benchmarks.push_back(result);
    }

    void write_to_file(const std::string& filepath) {
        // Update timestamp to now in nanoseconds
        auto now = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
        results.updated = nanos;

        std::string json;
        auto ec = glz::write_json(results, json);
        if (ec) {
            throw std::runtime_error("Failed to serialize JSON");
        }

        std::ofstream file(filepath);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file for writing: " + filepath);
        }
        file << json;
    }

private:
    BenchmarkFile results;
};

} // namespace sql_benchmark
