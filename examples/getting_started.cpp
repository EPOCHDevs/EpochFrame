#include <epochframe/dataframe.h>
#include <epochframe/series.h>
#include <iostream>
#include <vector>

/**
 * EpochFrame Getting Started Example
 * 
 * This example demonstrates basic usage of EpochFrame DataFrame and Series classes,
 * showing how to create, manipulate, and analyze data similar to pandas in Python.
 */
int main() {
    std::cout << "EpochFrame Getting Started Example" << std::endl;
    std::cout << "==================================" << std::endl;

    // Create a DataFrame from a map of column names to vectors
    epochframe::DataFrame df({
        {"Name", {"Alice", "Bob", "Charlie", "David", "Eva"}},
        {"Age", {25, 30, 35, 40, 45}},
        {"Score", {85.5, 90.0, 78.5, 92.5, 88.0}}
    });
    
    std::cout << "\n1. Created DataFrame:" << std::endl;
    std::cout << df << std::endl;
    
    // Accessing columns
    auto names = df["Name"];
    auto ages = df["Age"];
    
    std::cout << "\n2. Accessing the 'Name' column:" << std::endl;
    std::cout << names << std::endl;
    
    // Basic statistics
    std::cout << "\n3. Basic statistics on 'Age' column:" << std::endl;
    std::cout << "Mean age: " << ages.mean() << std::endl;
    std::cout << "Min age: " << ages.min() << std::endl;
    std::cout << "Max age: " << ages.max() << std::endl;
    std::cout << "Sum of ages: " << ages.sum() << std::endl;
    
    // Filtering data
    auto filtered_df = df[df["Age"] > 30];
    std::cout << "\n4. Filtering records where Age > 30:" << std::endl;
    std::cout << filtered_df << std::endl;
    
    // Selecting multiple columns
    auto subset = df[{"Name", "Score"}];
    std::cout << "\n5. Selecting only 'Name' and 'Score' columns:" << std::endl;
    std::cout << subset << std::endl;
    
    // Adding a new column
    df["Grade"] = df["Score"].apply([](double score) -> std::string {
        if (score >= 90) return "A";
        if (score >= 80) return "B";
        if (score >= 70) return "C";
        if (score >= 60) return "D";
        return "F";
    });
    
    std::cout << "\n6. DataFrame with computed 'Grade' column:" << std::endl;
    std::cout << df << std::endl;
    
    // GroupBy operations
    auto grouped = df.groupby("Grade").mean();
    std::cout << "\n7. Mean values grouped by 'Grade':" << std::endl;
    std::cout << grouped << std::endl;
    
    // Sort the data
    auto sorted_df = df.sort_values("Score", false);  // descending order
    std::cout << "\n8. DataFrame sorted by 'Score' (descending):" << std::endl;
    std::cout << sorted_df << std::endl;
    
    // Demonstration of chained operations
    auto result = df[df["Age"] < 40][{"Name", "Score", "Grade"}].sort_values("Score");
    std::cout << "\n9. Chained operations: filter, select columns, and sort:" << std::endl;
    std::cout << result << std::endl;
    
    return 0;
} 