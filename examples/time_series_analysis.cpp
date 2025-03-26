#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/scalar.h>
#include "date_time/datetime.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include "index/datetime_index.h"
#include <iostream>
#include <vector>
#include <random>

/**
 * EpochFrame Time Series Analysis Example
 * 
 * This example demonstrates how to work with time series data in EpochFrame,
 * including creating date ranges, resampling, rolling operations, and time-based analysis.
 */
int main() {
    std::cout << "EpochFrame Time Series Analysis Example" << std::endl;
    std::cout << "=======================================" << std::endl;

    // Create a date range for the time series (daily data for a month)
    auto start_date = epoch_frame::datetime::parse_date("2023-01-01");
    int num_days = 30;
    
    // Generate daily datetime index
    auto date_range = epoch_frame::factory::offset::date_range(
        start_date, num_days, "D");
    
    // Generate some random stock price data
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<> price_changes(0.0, 1.0);
    
    double initial_price = 100.0;
    std::vector<double> prices;
    prices.reserve(num_days);
    
    double current_price = initial_price;
    for (int i = 0; i < num_days; i++) {
        // Simulate random walk with slight upward bias
        current_price += price_changes(gen) + 0.05;
        if (current_price < 0) current_price = 0;
        prices.push_back(current_price);
    }
    
    // Create a time series DataFrame with stock prices
    epoch_frame::DataFrame stock_data({
        {"Price", prices},
        {"Volume", std::vector<int>(num_days, 100000)} // Constant volume for simplicity
    }, std::make_shared<epoch_frame::DatetimeIndex>(date_range));
    
    std::cout << "\n1. Daily stock price data:" << std::endl;
    std::cout << stock_data << std::endl;
    
    // Calculate daily returns
    auto price_series = stock_data["Price"];
    auto returns = price_series.pct_change();
    stock_data["Return"] = returns;
    
    std::cout << "\n2. Stock data with daily returns:" << std::endl;
    std::cout << stock_data << std::endl;
    
    // Calculate simple statistics
    std::cout << "\n3. Return statistics:" << std::endl;
    std::cout << "Mean return: " << returns.mean() << std::endl;
    std::cout << "Std dev: " << returns.std() << std::endl;
    std::cout << "Min return: " << returns.min() << std::endl;
    std::cout << "Max return: " << returns.max() << std::endl;
    
    // Calculate rolling mean (5-day moving average)
    auto rolling_mean = price_series.rolling(5).mean();
    stock_data["MA5"] = rolling_mean;
    
    std::cout << "\n4. Stock data with 5-day moving average:" << std::endl;
    std::cout << stock_data << std::endl;
    
    // Resample to weekly data
    auto weekly_data = stock_data.resample("W").mean();
    
    std::cout << "\n5. Weekly average price data:" << std::endl;
    std::cout << weekly_data << std::endl;
    
    // Calculate volatility (rolling standard deviation of returns)
    auto volatility = returns.rolling(5).std();
    stock_data["Volatility"] = volatility;
    
    std::cout << "\n6. Stock data with 5-day volatility:" << std::endl;
    std::cout << stock_data << std::endl;
    
    // Demonstrate shifting data (previous day's price)
    auto prev_price = price_series.shift(1);
    stock_data["PrevPrice"] = prev_price;
    
    std::cout << "\n7. Stock data with previous day's price:" << std::endl;
    std::cout << stock_data << std::endl;
    
    // Filter based on date range
    auto mid_month = epoch_frame::datetime::parse_date("2023-01-15");
    auto end_month = epoch_frame::datetime::parse_date("2023-01-30");
    
    auto filtered_dates = stock_data.loc(mid_month, end_month);
    
    std::cout << "\n8. Filtered data from Jan 15 to Jan 30:" << std::endl;
    std::cout << filtered_dates << std::endl;
    
    return 0;
} 