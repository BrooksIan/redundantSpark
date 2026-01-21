# Project Structure

This document describes the organization of the Data Deduplication Lab project.

## Directory Structure

```
redundantSpark/
├── README.md                          # Main lab guide (start here!)
├── requirements.txt                   # Python dependencies
├── .gitignore                        # Git ignore rules
│
├── notebooks/                        # Jupyter notebooks for lab exercises
│   ├── 00_Getting_Started.ipynb     # Setup and introduction
│   ├── 01_Basic_Deduplication.ipynb  # Exercise 1
│   ├── 02_Compare_Methods.ipynb      # Exercise 2
│   ├── 03_Approximate_Methods.ipynb   # Exercise 3
│   └── 04_File_Level_Deduplication.ipynb  # Exercise 4
│
├── documents/                        # Documentation
│   ├── CLOUDERA_AI_WORKBENCH.md      # Cloudera AI Workbench guide
│   ├── LOCAL_SETUP.md                # Local/Docker setup guide
│   ├── docker_commands.md             # Docker commands reference
│   ├── lab_guide.md                  # Step-by-step lab tutorial
│   ├── deduplication_guide.md        # Deduplication methods guide
│   ├── advanced_deduplication_methods.md  # Advanced techniques
│   └── results.md                    # Test results and analysis
│
├── examples/                         # Example scripts
│   ├── deduplicate_demo.py           # Demo script
│   └── deduplicate_files_example.py  # File deduplication example
│
├── scripts/                          # Utility scripts
│   └── copy_files_to_containers.sh    # Docker utility script
│
├── tests/                            # Unit tests
│   ├── __init__.py
│   ├── conftest.py                   # Pytest configuration
│   └── test_deduplication.py         # Deduplication tests
│
├── data/                             # Generated data (gitignored)
│   ├── .gitkeep                      # Keeps directory in git
│   ├── redundant_data.csv            # Sample datasets
│   └── duplicatefiles/                # Test duplicate files
│
├── docker-compose.yml                # Docker Compose configuration
├── Dockerfile                        # Docker image definition
│
└── Core Python Scripts (root level):
    ├── deduplicate_spark.py          # Main deduplication module
    ├── bloom_filter_hyperloglog.py    # Approximate methods
    ├── bloom_filter_file_deduplication.py  # File-level deduplication
    ├── generate_dataset.py            # Dataset generator
    └── generate_duplicate_files.py    # Duplicate file generator
```

## File Descriptions

### Core Scripts (Root Level)

These are the main scripts that users will import and use:

- **`deduplicate_spark.py`** - Main deduplication module with all deduplication methods
- **`bloom_filter_hyperloglog.py`** - HyperLogLog and Bloom Filter implementations
- **`bloom_filter_file_deduplication.py`** - File-level deduplication using Bloom Filters
- **`generate_dataset.py`** - Generates CSV datasets with intentional duplicates
- **`generate_duplicate_files.py`** - Generates duplicate files for testing

### Notebooks

Interactive Jupyter notebooks for guided lab exercises:

- **`00_Getting_Started.ipynb`** - Introduction, setup, and data generation
- **`01_Basic_Deduplication.ipynb`** - Exercise 1: Basic exact deduplication
- **`02_Compare_Methods.ipynb`** - Exercise 2: Compare different methods
- **`03_Approximate_Methods.ipynb`** - Exercise 3: HyperLogLog and approximate methods
- **`04_File_Level_Deduplication.ipynb`** - Exercise 4: File-level deduplication

### Examples

Example scripts demonstrating usage:

- **`deduplicate_demo.py`** - Demonstration script
- **`deduplicate_files_example.py`** - File deduplication example

### Scripts

Utility scripts for development and deployment:

- **`copy_files_to_containers.sh`** - Docker utility for copying files to containers

### Documentation

All documentation is in the `documents/` directory:

- **`CLOUDERA_AI_WORKBENCH.md`** - Complete guide for Cloudera AI Workbench
- **`LOCAL_SETUP.md`** - Local development and Docker setup
- **`docker_commands.md`** - Docker commands reference
- **`lab_guide.md`** - Step-by-step lab tutorial
- **`deduplication_guide.md`** - Detailed deduplication methods guide
- **`advanced_deduplication_methods.md`** - Advanced techniques
- **`results.md`** - Test results and performance analysis

### Tests

Unit tests using pytest:

- **`conftest.py`** - Pytest configuration and fixtures
- **`test_deduplication.py`** - Deduplication function tests

### Data

The `data/` directory contains generated files (gitignored):

- Sample CSV datasets
- Duplicate test files
- Output parquet files
- Results from deduplication runs

## Best Practices

1. **Core scripts** stay in root for easy importing
2. **Examples** go in `examples/` directory
3. **Documentation** is organized in `documents/`
4. **Notebooks** are in `notebooks/` for easy access
5. **Generated data** is in `data/` and gitignored
6. **Tests** are in `tests/` following pytest conventions

## For Lab Users

If you're using this for a lab:

1. Start with **`README.md`** for overview
2. Use **`notebooks/00_Getting_Started.ipynb`** to begin
3. Follow the exercise notebooks in order (01-04)
4. Refer to **`documents/CLOUDERA_AI_WORKBENCH.md`** for Cloudera-specific help

## For Developers

If you're developing or contributing:

1. Core scripts are in root for easy imports
2. Add examples to `examples/` directory
3. Add documentation to `documents/` directory
4. Follow existing test patterns in `tests/`
5. Update this file when adding new directories

