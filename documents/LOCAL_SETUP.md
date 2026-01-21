# Local Setup and Docker Guide

This guide covers setting up and running the deduplication project locally or using Docker.

## Overview

This project provides:
- **Dataset Generation**: Scripts to generate CSV files with intentional duplicates and variations
- **File Generation**: Scripts to generate duplicate files for testing file-level deduplication
- **Deduplication Scripts**: Ready-to-use Spark scripts for various deduplication strategies

## Requirements

- **Apache Spark**: 3.3.0 or greater
- **Python**: 3.8+
- **Docker & Docker Compose** (for containerized setup, optional)

## Installation Options

### Option 1: Using Docker Compose (Recommended for Local Development)

This is the easiest way to get started with a full Spark cluster.

#### Start the Spark Cluster

```bash
docker-compose up -d
```

This starts:
- **Spark Master** on port 8080 (Web UI)
- **Spark Worker** on port 8081 (Web UI)
- Both containers connected to the same network

#### View Logs

```bash
docker-compose logs -f
```

#### Stop the Cluster

```bash
docker-compose down
```

#### Access Web UIs

- **Spark Master Web UI**: http://localhost:8080
- **Spark Worker Web UI**: http://localhost:8081

**Note**: The Docker setup uses Apache Spark 4.1.0-preview4. The first time you run `docker-compose up -d`, it will pull the pre-built Spark image.

#### Execute Commands in the Cluster

```bash
# Start PySpark shell connected to the cluster
docker-compose exec spark-master pyspark --master spark://spark-master:7077

# Run Python scripts
docker-compose exec spark-master python generate_dataset.py

# Start Spark shell
docker-compose exec spark-master spark-shell --master spark://spark-master:7077

# Access bash shell
docker-compose exec spark-master bash
```

**📖 Docker Commands Reference**: For a comprehensive guide to all Docker and docker-compose commands used in this project, see [docker_commands.md](docker_commands.md).

### Option 2: Using Docker (Single Container)

Build the Docker image:

```bash
docker build -t redundantspark .
```

Run the container:

```bash
docker run -it --rm -p 8080:8080 -p 8081:8081 -p 7077:7077 redundantspark
```

**Note**: The Docker image includes Apache Spark 3.3.0, so PySpark is available without additional installation.

#### Start Spark Shells

```bash
# Start Spark shell
docker run -it --rm -p 8080:8080 redundantspark spark-shell

# Start PySpark shell
docker run -it --rm -p 8080:8080 redundantspark pyspark

# Run Python scripts
docker run -it --rm -v $(pwd):/app redundantspark python generate_dataset.py
```

### Option 3: Local Installation

For local development without Docker:

#### Install PySpark

```bash
# Install PySpark (matching Spark 3.3.0)
pip install pyspark==3.3.0

# Install other dependencies
pip install -r requirements.txt
```

**Note**: When using Docker, PySpark is already included in the Spark image, so you don't need to install it separately.

## Usage

### Generate Sample Data

#### Generate CSV Dataset with Duplicates

```bash
# Generate 1000 records with ~10% duplicates
python generate_dataset.py 1000 redundant_data.csv

# Generate large dataset
python generate_dataset.py 100000 redundant_data_large.csv
```

#### Generate Duplicate Files

```bash
# Generate 25 files with 90% unique (22 unique, 3 duplicates)
python generate_duplicate_files.py 25 0.9 data/duplicatefiles

# Generate 100 files with 80% unique
python generate_duplicate_files.py 100 0.8 data/duplicatefiles

# Custom output directory
python generate_duplicate_files.py 50 0.9 /path/to/output
```

The script creates files with random names where:
- Files have random alphanumeric filenames
- Content is randomly generated text
- Duplicate files have the same content but different filenames
- Summary shows which files are duplicates

### Run Deduplication

#### Using Docker Compose

```bash
# Run deduplication script
docker-compose exec spark-master python deduplicate_spark.py redundant_data.csv output.parquet exact
```

#### Using Docker

```bash
docker run -it --rm -v $(pwd):/app redundantspark python deduplicate_spark.py redundant_data.csv output.parquet exact
```

#### Local Usage

```bash
# Exact duplicate removal (fastest)
python deduplicate_spark.py redundant_data.csv output.parquet exact

# Window-based deduplication (keeps first record)
python deduplicate_spark.py redundant_data.csv output.parquet window

# Normalized deduplication (handles case/whitespace)
python deduplicate_spark.py redundant_data.csv output.parquet normalized

# Fuzzy matching with Levenshtein (expensive, for small datasets)
python deduplicate_spark.py redundant_data.csv output.parquet fuzzy_levenshtein

# Fuzzy matching with FuzzyWuzzy (expensive, for small datasets)
python deduplicate_spark.py redundant_data.csv output.parquet fuzzy_fuzzywuzzy

# Locality-Sensitive Hashing (scalable fuzzy matching)
python deduplicate_spark.py redundant_data.csv output.parquet lsh

# Checksum-based deduplication (MD5 hash)
python deduplicate_spark.py redundant_data.csv output.parquet checksum_md5

# Checksum-based deduplication (SHA-256 hash)
python deduplicate_spark.py redundant_data.csv output.parquet checksum_sha256

# Spark built-in hash function (fastest hash-based method)
python deduplicate_spark.py redundant_data.csv output.parquet spark_hash

# Partitioned hash-based deduplication (for large datasets)
python deduplicate_spark.py redundant_data.csv output.parquet partitioned_hash
```

### Bloom Filters and HyperLogLog

For approximate duplicate detection and distinct count estimation:

```bash
# Run Bloom Filter and HyperLogLog demonstrations
python bloom_filter_hyperloglog.py
```

This script demonstrates:
- **Bloom Filters**: Memory-efficient approximate duplicate detection
- **HyperLogLog**: Fast distinct count estimation with minimal memory
- **Method Comparison**: Compares approximate vs exact methods
- **Incremental Deduplication**: Using Bloom Filters for streaming scenarios

## Available Deduplication Methods

| Method | Description | Use Case |
|--------|-------------|----------|
| `exact` | Simple exact duplicate removal | Fastest, removes exact duplicates |
| `window` | Window-based deduplication (keeps first/last) | When you need to keep specific records |
| `normalized` | Normalize then deduplicate | Handles case/whitespace variations |
| `fuzzy_levenshtein` | Fuzzy matching with Levenshtein distance | Small datasets, expensive |
| `fuzzy_fuzzywuzzy` | Fuzzy matching with FuzzyWuzzy | Small datasets, expensive |
| `lsh` | Locality-Sensitive Hashing | Scalable fuzzy matching |
| `checksum_md5` | MD5 hash-based deduplication | Content-based deduplication |
| `checksum_sha256` | SHA-256 hash-based deduplication | More secure hash-based deduplication |
| `spark_hash` | Spark built-in hash function | Fastest hash-based method |
| `partitioned_hash` | Hash-based partitioned deduplication | Large-scale hash-based deduplication |

## Best Practices

### For Exact Duplicates

- Use `dropDuplicates()` - simplest and fastest
- Use `spark_hash` method for hash-based deduplication (very fast)
- Partition by key columns for better performance

### For Checksum-Based Deduplication

- Use `spark_hash` for fastest hash-based method (uses Spark's built-in MD5)
- Use `checksum_md5` or `checksum_sha256` for content-based deduplication
- Use `partitioned_hash` for large-scale hash-based deduplication

### For Fuzzy Duplicates

- Use LSH (Locality-Sensitive Hashing) for large datasets
- Use Levenshtein/FuzzyWuzzy only for small datasets (< 100K records)
- Normalize data (case, whitespace) before deduplication

### For File-Level Deduplication

- Use the `file_level_deduplication()` function to detect duplicate files by content hash
- Useful for detecting duplicate files in storage systems

### For Approximate Methods

- Use **Bloom Filters** for memory-efficient duplicate detection (allows small false positive rate)
- Use **HyperLogLog** for fast distinct count estimation (uses minimal memory)
- Both methods are much faster and use less memory than exact methods
- Ideal for monitoring, dashboards, and large-scale streaming scenarios

### Performance Tips

- Cache intermediate results if reused
- Partition data appropriately
- Use window functions when you need to keep specific records
- Write results in efficient formats (Parquet, Delta)
- Hash-based methods are faster than column-based comparisons for exact duplicates

## Documentation

**Getting Started:**
- [Lab Guide](documents/lab_guide.md) - Step-by-step tutorial teaching Python and Spark for deduplication

**Reference Guides:**
- [docker_commands.md](docker_commands.md) - Complete reference for all Docker and docker-compose commands
- [deduplication_guide.md](documents/deduplication_guide.md) - Detailed explanation of each deduplication method
- [advanced_deduplication_methods.md](documents/advanced_deduplication_methods.md) - Advanced techniques including:
  - Checksum/hash-based deduplication methods
  - File-level duplicate detection
  - Bloom filters and HyperLogLog
  - Delta Lake deduplication
  - Advanced techniques for large-scale deduplication

## Test Results

This project includes comprehensive test results demonstrating the effectiveness of various deduplication methods:

### Record-Level Deduplication Results

See [results.md](documents/results.md) for detailed test results including:

- **Test Environment**: Spark 4.1.0-preview4 cluster configuration
- **Dataset Size**: 410,000 records across multiple files
- **Methods Tested**: 
  - Exact deduplication (95.91% duplicate removal rate)
  - Spark hash deduplication (5.27% duplicate removal rate)
- **Performance Metrics**: Processing times, cluster utilization, and scalability results
- **Comparison Analysis**: Side-by-side comparison of different deduplication strategies

**Key Findings:**
- Exact method removed 393,250 duplicates from 410,000 records (95.91% removal rate)
- Spark hash method processed records faster with 5.27% removal rate
- Both methods executed successfully on large datasets without errors

### File-Level Deduplication Results

See [file_deduplication_results.md](documents/file_deduplication_results.md) for file duplicate detection results:

- **Files Analyzed**: 25 files in test directory
- **Results**: Identified 3 duplicate groups (6 duplicate files)
- **Unique Files**: 22 unique files identified
- **Space Efficiency**: 12% duplicate rate detected
- **Methodology**: MD5 hash-based content comparison using Spark

**Key Findings:**
- Successfully identified all duplicate files by content hash
- Efficient distributed processing of file-level deduplication
- Demonstrated scalability for larger file collections

### Approximate Methods Results

See [bloom_filter_hyperloglog_results.md](documents/bloom_filter_hyperloglog_results.md) for test results on approximate duplicate detection methods:

- **HyperLogLog Distinct Count Estimation**: Tested with different RSD (Relative Standard Deviation) values
  - RSD=0.01: 1.02% error rate, ~1.12s processing time
  - RSD=0.05: ~1-2% error rate, ~0.2s processing time (recommended)
  - RSD=0.1: ~1-2% error rate, ~0.1s processing time
- **Memory Efficiency**: HyperLogLog uses only ~1.5KB per counter regardless of dataset size
- **Performance**: For large datasets, HyperLogLog can be faster than exact methods
- **Use Cases**: Ideal for monitoring, dashboards, and memory-constrained environments

**Key Findings:**
- HyperLogLog provides accurate approximate counts (1-2% error) with minimal memory
- RSD=0.05 provides the best balance of speed and accuracy
- Approximate methods scale better than exact methods for very large datasets
- Suitable for real-time analytics where approximate results are acceptable

These results demonstrate the effectiveness of Spark-based deduplication for both record-level and file-level duplicate detection, as well as approximate methods for memory-efficient distinct counting in production environments.

