# EpochFrame Documentation

This directory contains the documentation for the EpochFrame project.

## Documentation Structure

- **API Reference**: Detailed documentation of all classes and functions
- **User Guide**: Guides and tutorials for using EpochFrame
- **Examples**: Code examples demonstrating various features
- **Benchmarks**: Performance comparison with pandas
- **Development**: Information for contributors

## Building the Documentation

The documentation is built using Doxygen and Sphinx.

### Prerequisites

- Doxygen (for C++ API documentation)
- Sphinx (for User Guide and integration)
- Breathe (for integrating Doxygen with Sphinx)

### Building

To build the documentation:

```bash
cd docs
doxygen Doxyfile
cd sphinx
make html
```

The documentation will be generated in `docs/sphinx/build/html`.

## Documentation Sections

### API Reference

The API reference is generated from the C++ header files using Doxygen. It provides a detailed reference of all classes, methods, and functions in the EpochFrame library.

### User Guide

The User Guide provides a more narrative approach to using EpochFrame, with step-by-step instructions and examples.

Topics include:

- Getting Started
- Working with DataFrames and Series
- Time Series Analysis
- GroupBy Operations
- Indexing and Selection
- Advanced Features

### Examples

The examples showcase how to use EpochFrame for various tasks, including:

- Basic data manipulation
- Time series analysis
- Financial analysis
- Data visualization
- Integration with other C++ libraries

### Benchmarks

The benchmarks section provides performance comparisons between EpochFrame and pandas for various operations.

### Development

The development section provides information for contributors to the EpochFrame project, including:

- Code style guide
- Development workflow
- Testing guidelines
- Release process 