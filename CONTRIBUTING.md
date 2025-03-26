# Contributing to EpochFrame

Thank you for your interest in contributing to EpochFrame! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Pull Request Process](#pull-request-process)
- [Testing](#testing)
- [Documentation](#documentation)

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** to your local machine
3. **Set up the development environment** as described below
4. **Create a branch** for your changes
5. **Make your changes** and commit them
6. **Push to your fork** and submit a pull request

## Development Setup

### Prerequisites

- C++17 compatible compiler (GCC 7+, Clang 5+, MSVC 2019+)
- CMake 3.18+
- Git
- Python 3.6+ (for benchmarking)

### Setting Up the Development Environment

```bash
# Clone your fork
git clone https://github.com/yourusername/epoch_frame.git
cd epoch_frame

# Add the upstream remote
git remote add upstream https://github.com/originalowner/epoch_frame.git

# Create a branch for your feature
git checkout -b feature/your-feature-name

# Build with tests and benchmarks enabled
mkdir build && cd build
cmake -DBUILD_TESTS=ON -DBUILD_BENCHMARK=ON ..
make
```

## Coding Standards

EpochFrame follows a consistent coding style:

- Use 4 spaces for indentation, not tabs
- Use camelCase for variable and function names
- Use PascalCase for class names
- Use UPPER_CASE for constants and macros
- Include appropriate comments for complex logic
- Follow the [C++ Core Guidelines](https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines) where applicable

### Formatting

We use clang-format to ensure consistent code style. Run it before submitting your changes:

```bash
# Format all files in the repository
clang-format -i include/**/*.h src/**/*.cpp
```

## Pull Request Process

1. **Update the README.md** with details of changes, if applicable
2. **Add tests** for any new features or fixes
3. **Update documentation** for any changed functionality
4. **Ensure all tests pass** by running `ctest` in the build directory
5. **Ensure code follows our style guide** by running clang-format
6. **Submit your pull request** with a clear description of the changes

## Testing

All new features should include appropriate tests:

```bash
# Run all tests
cd build
ctest

# Run a specific test
ctest -R TestName
```

We use Catch2 for unit testing. Test files should be placed in the `test/` directory with a filename matching the pattern `test_*.cpp`.

## Documentation

Documentation is a crucial part of EpochFrame:

- **API Documentation**: Update the header files with clear, descriptive comments
- **Examples**: Add examples for new features in the `examples/` directory
- **User Guide**: Update the relevant sections in the user guide when adding new functionality

## Benchmarking

For performance-critical code, please include benchmarks:

```bash
# Build and run benchmarks
cd build
make epochframe_benchmark
./bin/epochframe_benchmark
```

## License

By contributing, you agree that your contributions will be licensed under the project's [Apache License 2.0](LICENSE).

## Questions?

If you have any questions, please open an issue or reach out to the maintainers. We appreciate your contributions!
