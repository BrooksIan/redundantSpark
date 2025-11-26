# redundantSpark

A project for generating redundant datasets and demonstrating Apache Spark best practices for deduplication and handling redundant data.

## Overview

This project provides:
- **Dataset Generation**: Scripts to generate CSV files with intentional duplicates and variations
- **File Generation**: Scripts to generate duplicate files for testing file-level deduplication
- **Deduplication Guide**: Comprehensive guide on Apache Spark deduplication techniques
- **Deduplication Scripts**: Ready-to-use Spark scripts for various deduplication strategies

## Requirements

- **Apache Spark**: 3.3.0 or greater
- **Python**: 3.8+
- **Docker & Docker Compose** (for containerized setup)

## Getting Started

This project uses Apache Spark 3.3.0 with Hadoop 3. The easiest way to get started is using Docker Compose.

## Installation

### Using Docker Compose (Recommended)

Start the Spark cluster (master + worker):
```bash
docker-compose up -d
```

View logs:
```bash
docker-compose logs -f
```

Stop the cluster:
```bash
docker-compose down
```

Access the Spark Master Web UI at: http://localhost:8080  
Access the Spark Worker Web UI at: http://localhost:8081

**Note**: The Docker setup uses Apache Spark 4.1.0-preview4. The first time you run `docker-compose up -d`, it will pull the pre-built Spark image.

**📖 Docker Commands Reference**: For a comprehensive guide to all Docker and docker-compose commands used in this project, see [docker_commands.md](docker_commands.md). This includes commands for managing containers, running Spark jobs, troubleshooting, and more.

### Using Docker

Build the Docker image:
```bash
docker build -t redundantspark .
```

Run the container:
```bash
docker run -it --rm -p 8080:8080 -p 8081:8081 -p 7077:7077 redundantspark
```

**Note**: The Docker image includes Apache Spark 3.3.0, so PySpark is available without additional installation.

### Local Installation

For local development, you'll need to install PySpark separately:

```bash
# Install PySpark (matching Spark 3.3.0)
pip install pyspark==3.3.0

# Install other dependencies
pip install -r requirements.txt
```

**Note**: When using Docker, PySpark is already included in the Spark image, so you don't need to install it separately.

## Usage

### Using Docker Compose

Execute commands in the Spark cluster:
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

### Using Docker

Start a Spark shell:
```bash
docker run -it --rm -p 8080:8080 redundantspark spark-shell
```

Start a PySpark shell:
```bash
docker run -it --rm -p 8080:8080 redundantspark pyspark
```

Run Python scripts:
```bash
docker run -it --rm -v $(pwd):/app redundantspark python generate_dataset.py
```

### Local Usage

```bash
# Generate redundant dataset (CSV with duplicate records)
python generate_dataset.py 1000 redundant_data.csv

# Generate duplicate files (for testing file-level deduplication)
python generate_duplicate_files.py 25 0.9 data/duplicatefiles

# Deduplicate using Spark
python deduplicate_spark.py redundant_data.csv output.parquet exact
```

## Data Generation

### Generate CSV Dataset with Duplicates

```bash
# Generate 1000 records with ~10% duplicates
python generate_dataset.py 1000 redundant_data.csv

# Generate large dataset
python generate_dataset.py 100000 redundant_data_large.csv
```

### Generate Duplicate Files

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

## Deduplication

### Quick Start

The project includes a comprehensive deduplication script with multiple strategies:

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

### Documentation

**Getting Started:**
- [Lab Guide](documents/lab_guide.md) - **Start here!** Step-by-step tutorial teaching Python and Spark for deduplication

**Reference Guides:**
- [docker_commands.md](docker_commands.md) - **Docker Commands Helper** - Complete reference for all Docker and docker-compose commands used in this project
- [deduplication_guide.md](documents/deduplication_guide.md) - Detailed explanation of each deduplication method
- [advanced_deduplication_methods.md](documents/advanced_deduplication_methods.md) - Advanced techniques including:
  - Checksum/hash-based deduplication methods
  - File-level duplicate detection
  - Bloom filters and HyperLogLog
  - Delta Lake deduplication
  - Advanced techniques for large-scale deduplication

### Best Practices Summary

**For exact duplicates:**
- Use `dropDuplicates()` - simplest and fastest
- Use `spark_hash` method for hash-based deduplication (very fast)
- Partition by key columns for better performance

**For checksum-based deduplication:**
- Use `spark_hash` for fastest hash-based method (uses Spark's built-in MD5)
- Use `checksum_md5` or `checksum_sha256` for content-based deduplication
- Use `partitioned_hash` for large-scale hash-based deduplication

**For fuzzy duplicates:**
- Use LSH (Locality-Sensitive Hashing) for large datasets
- Use Levenshtein/FuzzyWuzzy only for small datasets (< 100K records)
- Normalize data (case, whitespace) before deduplication

**For file-level deduplication:**
- Use the `file_level_deduplication()` function to detect duplicate files by content hash
- Useful for detecting duplicate files in storage systems

**For approximate methods (when exact accuracy isn't critical):**
- Use **Bloom Filters** for memory-efficient duplicate detection (allows small false positive rate)
- Use **HyperLogLog** for fast distinct count estimation (uses minimal memory)
- Both methods are much faster and use less memory than exact methods
- Ideal for monitoring, dashboards, and large-scale streaming scenarios

**Performance tips:**
- Cache intermediate results if reused
- Partition data appropriately
- Use window functions when you need to keep specific records
- Write results in efficient formats (Parquet, Delta)
- Hash-based methods are faster than column-based comparisons for exact duplicates

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


