# EpochFrame

![EpochFrame Logo](docs/assets/logo.png)

EpochFrame is a high-performance C++ data manipulation library inspired by Python's pandas, designed for efficient data analysis and transformation. It provides a familiar DataFrame and Series API with C++ performance benefits.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Features

- **High Performance**: Native C++ implementation that outperforms pandas for many operations
- **Familiar API**: Interface designed to be similar to pandas for easy adoption
- **Arrow Integration**: Built on Apache Arrow for efficient memory management and interoperability
- **Time Series Support**: Specialized functionality for time series analysis
- **String Operations**: Comprehensive string manipulation capabilities
- **Extensibility**: Easy to extend with custom functions and operations
- **Memory Efficiency**: Optimized memory usage for large datasets

## Installation

### Dependencies

- C++17 compatible compiler
- CMake 3.18+
- Apache Arrow 19.0+
- [Optional] Python 3.6+ with pandas (for benchmarking)

### Using vcpkg (Recommended)

EpochFrame uses vcpkg for dependency management:

```bash
# Clone the repository
git clone https://github.com/yourusername/epoch_frame.git
cd epoch_frame

# Build with CMake
mkdir build && cd build
cmake ..
make

# Run tests
make test
```

### Manual Installation

If you prefer to manage dependencies manually:

```bash
# Install dependencies
# Ubuntu/Debian
sudo apt-get install libarrow-dev libparquet-dev

# Build EpochFrame
git clone https://github.com/yourusername/epoch_frame.git
cd epoch_frame
mkdir build && cd build
cmake ..
make
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute.

## License

EpochFrame is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by [pandas](https://pandas.pydata.org/)
- Built on [Apache Arrow](https://arrow.apache.org/)
- Benchmark framework uses [Catch2](https://github.com/catchorg/Catch2)
