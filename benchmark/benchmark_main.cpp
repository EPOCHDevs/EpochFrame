#define CATCH_CONFIG_MAIN
#define CATCH_CONFIG_ENABLE_BENCHMARKING
#include <catch2/catch_all.hpp>
#include <tabulate/table.hpp>
#include <iostream>

// Track global benchmark results for summary
struct BenchmarkResult {
    std::string category;
    std::string operation;
    size_t dataSize;
    double epochframeTime;
    double pandasTime;
    double speedupRatio;
};

// Global collector for benchmark results
std::vector<BenchmarkResult> globalBenchmarkResults;

// Provide a function to add benchmark results
void recordBenchmark(const std::string& category, const std::string& operation, 
                    size_t dataSize, double efTime, double pdTime) {
    BenchmarkResult result;
    result.category = category;
    result.operation = operation;
    result.dataSize = dataSize;
    result.epochframeTime = efTime;
    result.pandasTime = pdTime;
    result.speedupRatio = (pdTime > 0 && efTime > 0) ? (pdTime / efTime) : 0.0;
    
    globalBenchmarkResults.push_back(result);
}

// Print the global summary at the end
struct GlobalSummaryReporter : Catch::EventListenerBase {
    using EventListenerBase::EventListenerBase;
    
    void testRunEnded(Catch::TestRunStats const& testRunStats) override {
        // Skip printing if no benchmarks were recorded
        if (globalBenchmarkResults.empty()) {
            return;
        }
        
        // Create a summary table
        tabulate::Table summaryTable;
        
        // Add headers
        summaryTable.add_row({"Category", "Operation", "Data Size", "EpochFrame (s)", 
                          "Pandas (s)", "Speedup Ratio"});
        
        // Format the table
        summaryTable.format()
            .multi_byte_characters(true)
            .border_top("-")
            .border_bottom("-")
            .border_left("|")
            .border_right("|")
            .corner("+");
            
        summaryTable[0].format()
            .font_color(tabulate::Color::green)
            .font_style({tabulate::FontStyle::bold})
            .border_top("-")
            .border_bottom("-");
            
        // Add all benchmark results
        for (const auto& result : globalBenchmarkResults) {
            // Format numbers with proper precision
            std::ostringstream timeEF, timePD, speedup;
            timeEF << std::fixed << std::setprecision(6) << result.epochframeTime;
            timePD << std::fixed << std::setprecision(6) << result.pandasTime;
            speedup << std::fixed << std::setprecision(2) << result.speedupRatio;
            
            summaryTable.add_row({
                result.category,
                result.operation,
                std::to_string(result.dataSize),
                timeEF.str(),
                timePD.str(),
                speedup.str()
            });
        }
        
        // Print the summary
        std::cout << "\n\n=== EpochFrame vs Pandas Benchmark Summary ===\n\n";
        std::cout << summaryTable << std::endl;
        
        // Print overall stats
        double totalSpeedup = 0.0;
        size_t validComparisons = 0;
        
        for (const auto& result : globalBenchmarkResults) {
            if (result.speedupRatio > 0.0) {
                totalSpeedup += result.speedupRatio;
                validComparisons++;
            }
        }
        
        if (validComparisons > 0) {
            double avgSpeedup = totalSpeedup / validComparisons;
            std::cout << "\nAverage Speedup: " << std::fixed << std::setprecision(2) 
                     << avgSpeedup << "x (higher is better for EpochFrame)" << std::endl;
        }
    }
};

CATCH_REGISTER_LISTENER(GlobalSummaryReporter)

// Main entry point is defined by Catch2 macros above 