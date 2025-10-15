# Mini Data Pipeline for LLM-Style Datasets

A lightweight data pipeline that downloads, cleans, filters, and summarizes a small corpus of text in a way that's robust, reproducible, and testable.

## Overview

This project demonstrates a production-quality data pipeline designed for processing text datasets. Built as interview preparation for a role on the data team at Reflection AI, it showcases best practices in data engineering including modularity, testing, and reproducibility.

## Project Structure

```
├── data/
│   └── raw/              # Raw downloaded data
│   └── processed/        # Cleaned and processed data
├── pipeline/
│   ├── __init__.py       # Package initialization
│   ├── io_utils.py       # Data download and I/O operations
│   ├── cleaners.py       # Text cleaning functions
│   ├── filters.py        # Data filtering logic
│   ├── analytics.py      # Summary and analytics functions
│   └── main.py           # Main pipeline orchestration
└── tests/
    └── test_pipeline.py  # Unit and integration tests
```

## Features

- **Download**: Fetch text data from various sources
- **Clean**: Remove noise, normalize text, handle edge cases
- **Filter**: Apply custom filtering criteria to data
- **Analyze**: Generate summaries and statistics
- **Test**: Comprehensive test coverage for reliability
- **Reproduce**: Deterministic pipeline for consistent results

## Requirements

- **Python**: 3.10+
- **Built-in libraries**: `gzip`, `json`, `pathlib`, `logging`
- **Recommended libraries**:
  - `requests` - HTTP requests for downloading data
  - `tqdm` - Progress bars
  - `pytest` - Testing framework
  - `tiktoken` - Token counting for LLM datasets
  - `langdetect` (or `fasttext`) - Language detection
  - `pandas` (optional) - Data manipulation

## Installation

1. Ensure you have Python 3.10 or higher installed:
```bash
python3 --version
```

2. Install required dependencies:
```bash
pip install requests tqdm pytest tiktoken langdetect pandas
```

## Getting Started

Coming soon...

## Usage

Coming soon...

## License

Coming soon...
