# Lab Guide: Data Deduplication with Python and Apache Spark

## Table of Contents
1. [Introduction](#introduction)
2. [Documentation Index](#documentation-index)
3. [Prerequisites](#prerequisites)
4. [Lab Setup](#lab-setup)
5. [Part 1: Understanding Duplicate Data](#part-1-understanding-duplicate-data)
6. [Part 2: Record-Level Deduplication](#part-2-record-level-deduplication)
7. [Part 3: File-Level Duplicate Detection](#part-3-file-level-duplicate-detection)
8. [Part 4: Advanced Techniques](#part-4-advanced-techniques)
9. [Exercises](#exercises)
10. [Best Practices](#best-practices)

---

## Documentation Index

This lab guide is part of a comprehensive documentation set. The following markdown files are available in the `documents/` directory:

- **[lab_guide.md](lab_guide.md)** - This comprehensive lab guide covering all aspects of data deduplication with Python and Apache Spark
- **[deduplication_guide.md](deduplication_guide.md)** - Detailed explanations of deduplication methods and techniques
- **[advanced_deduplication_methods.md](advanced_deduplication_methods.md)** - Advanced deduplication techniques including fuzzy matching and similarity algorithms
- **[bloom_filter_hyperloglog_results.md](bloom_filter_hyperloglog_results.md)** - Detailed test results and analysis for HyperLogLog distinct count estimation and Bloom Filter implementations
- **[bloom_filter_file_deduplication_results.md](bloom_filter_file_deduplication_results.md)** - Results and analysis of Bloom Filter-based file deduplication methods
- **[file_deduplication_results.md](file_deduplication_results.md)** - Results and analysis of file-level duplicate detection methods
- **[results.md](results.md)** - Summary of test results and performance benchmarks across different deduplication methods

---

## Introduction

This lab guide will teach you how to identify and remove duplicate data using Python and Apache Spark. You'll learn:

- How to generate test datasets with duplicates
- Different types of duplicates (exact, fuzzy, normalized)
- Record-level deduplication techniques
- File-level duplicate detection
- Performance optimization strategies

### Learning Objectives

By the end of this lab, you will be able to:
- Generate test datasets with controlled duplicates
- Use Spark to deduplicate records using multiple strategies
- Detect duplicate files by content hash
- Choose the appropriate deduplication method for your use case
- Optimize Spark jobs for better performance

---

## Prerequisites

Before starting this lab, you should have:
- Basic Python programming knowledge
- Understanding of DataFrames and SQL concepts
- Familiarity with command-line interface
- Docker installed (for containerized Spark setup)

---

## Lab Setup

### Option 1: Using Docker Compose (Recommended)

1. **Start the Spark cluster:**
   ```bash
   docker-compose up -d
   ```

2. **Verify the cluster is running:**
   ```bash
   docker-compose ps
   ```

3. **Access the Spark Master Web UI:**
   Open http://localhost:8080 in your browser

4. **Execute commands in the cluster:**
   ```bash
   docker-compose exec spark-master bash
   ```

5. **Copy files to containers (if needed):**
   ```bash
   # Copy a Python script to the container
   docker cp your_script.py spark-master:/app/
   
   # Copy data directory
   docker cp data spark-master:/app/
   
   # Or use the provided script
   ./copy_files_to_containers.sh
   ```

**📖 Docker Commands Reference**: For a comprehensive guide to all Docker and docker-compose commands, troubleshooting tips, and advanced usage, see [docker_commands.md](../docker_commands.md). This reference includes:
- All container management commands
- Running Spark jobs with spark-submit
- Accessing containers and debugging
- Common workflows and troubleshooting
- Port mappings and environment variables

**💡 Working with Files in Docker:**
- Scripts should be placed in `/app/` directory inside containers
- Data files should be in `/app/data/` directory
- When creating scripts locally, copy them to containers using `docker cp`
- The `copy_files_to_containers.sh` script automates copying common files

### Option 2: Local Setup

1. **Install PySpark:**
   ```bash
   pip install pyspark==3.3.0
   pip install -r requirements.txt
   ```

2. **Set up Spark environment:**
   ```bash
   export SPARK_HOME=/path/to/spark
   export PATH=$PATH:$SPARK_HOME/bin
   ```

---

## Part 1: Understanding Duplicate Data

### Exercise 1.1: Generate a Test Dataset

Let's start by creating a dataset with known duplicates.

**Task:** Generate a CSV file with 1000 records containing duplicates.

```bash
# In Docker container (files will be in /app/ directory)
docker-compose exec spark-master python /app/generate_dataset.py 1000 /app/data/redundant_data.csv

# Or locally
python generate_dataset.py 1000 redundant_data.csv
```

**Note:** Make sure the `data/` directory exists. In Docker, create it with:
```bash
docker-compose exec spark-master mkdir -p /app/data
```

**What to observe:**
- The script creates records with exact duplicates
- Some records have variations (case differences, whitespace, etc.)
- Check the output file to see the duplicate patterns

**Expected output:**
```
Generating 1000 records with duplicates and variations...
Generated 1000 records and saved to redundant_data.csv
Expected unique records: ~500
```

### Exercise 1.2: Inspect the Data

Let's examine the data to understand the duplicate patterns.

**Create a Python script `inspect_data.py`:**

```python
import pandas as pd
import os

# Read the CSV file
# Use /app/data/ in Docker, or current directory locally
csv_path = '/app/data/redundant_data.csv' if os.path.exists('/app/data/redundant_data.csv') else 'redundant_data.csv'
df = pd.read_csv(csv_path)

print(f"Total records: {len(df)}")
print(f"\nFirst 10 records:")
print(df.head(10))

print(f"\nDuplicate records (exact):")
duplicates = df[df.duplicated()]
print(f"Number of exact duplicates: {len(duplicates)}")

print(f"\nSample duplicates:")
print(duplicates.head(5))
```

**Run the script:**
```bash
# In Docker container
docker-compose exec spark-master python /app/inspect_data.py

# Or locally
python inspect_data.py
```

**Note:** If you created the script locally, copy it to Docker first:
```bash
docker cp inspect_data.py spark-master:/app/
```

**Questions to consider:**
1. How many exact duplicates are there?
2. What patterns do you notice in the duplicate records?
3. Are there any near-duplicates (similar but not identical)?

---

## Part 2: Record-Level Deduplication

**💡 Note:** For the exercises in this section, you'll create Python scripts. You can create them:
- **Locally** on your machine, then copy to Docker: `docker cp script_name.py spark-master:/app/`
- **Inside Docker** by accessing the container: `docker-compose exec spark-master bash`, then create files in `/app/`

### Exercise 2.1: Exact Duplicate Removal

**Objective:** Remove exact duplicate records using Spark.

**Create a script `dedupe_exact.py`:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create Spark session
spark = SparkSession.builder \
    .appName("ExactDeduplication") \
    .getOrCreate()

# Read the CSV file
# Use /app/data/ in Docker, or current directory locally
import os
csv_path = '/app/data/redundant_data.csv' if os.path.exists('/app/data/redundant_data.csv') else 'redundant_data.csv'
df = spark.read.csv(csv_path, header=True, inferSchema=True)

print(f"Original record count: {df.count()}")

# Remove exact duplicates (all columns)
df_deduped = df.dropDuplicates()

print(f"Deduplicated record count: {df_deduped.count()}")

# Show sample results
print("\nSample deduplicated records:")
df_deduped.show(10, truncate=False)

# Save results
# Use /app/data/ in Docker, or current directory locally
output_path = '/app/data/deduplicated_exact.parquet' if os.path.exists('/app/data') else 'deduplicated_exact.parquet'
df_deduped.write.mode('overwrite').parquet(output_path)

spark.stop()
```

**Run the script:**
```bash
# In Docker (scripts should be in /app/ directory)
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/dedupe_exact.py

# Or locally
spark-submit dedupe_exact.py
```

**Note:** If you created the script inside the Docker container, make sure it's in `/app/` directory. You can copy files to the container using:
```bash
docker cp dedupe_exact.py spark-master:/app/
```

**Expected results:**
- Original count: ~1000 records
- Deduplicated count: ~500-600 records
- Exact duplicates removed

### Exercise 2.2: Column-Based Deduplication

**Objective:** Remove duplicates based on specific columns (e.g., email).

**Create a script `dedupe_by_email.py`:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EmailDeduplication") \
    .getOrCreate()

df = spark.read.csv('redundant_data.csv', header=True, inferSchema=True)

print(f"Original records: {df.count()}")

# Remove duplicates based on email column only
df_deduped = df.dropDuplicates(subset=['email'])

print(f"Unique emails: {df_deduped.count()}")

# Show records with same email but different names
print("\nRecords grouped by email:")
df.groupBy('email').count().filter('count > 1').show(10)

spark.stop()
```

**Questions:**
1. How many unique email addresses are there?
2. What happens when you deduplicate by email vs. all columns?

### Exercise 2.3: Normalized Deduplication

**Objective:** Handle duplicates with case/whitespace variations.

**Create a script `dedupe_normalized.py`:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("NormalizedDeduplication") \
    .getOrCreate()

df = spark.read.csv('redundant_data.csv', header=True, inferSchema=True)

print(f"Original records: {df.count()}")

# Normalize data: lowercase, trim whitespace
df_normalized = df.withColumn('name_normalized', 
    F.trim(F.lower(F.col('name')))) \
    .withColumn('email_normalized', 
    F.trim(F.lower(F.col('email')))) \
    .withColumn('address_normalized', 
    F.trim(F.lower(F.col('address'))))

# Remove duplicates on normalized columns
df_deduped = df_normalized.dropDuplicates(
    subset=['name_normalized', 'email_normalized', 'address_normalized']
)

print(f"Normalized deduplicated records: {df_deduped.count()}")

# Show examples of normalized duplicates
print("\nNormalized duplicate examples:")
df_normalized.groupBy('email_normalized').count() \
    .filter('count > 1').show(5)

spark.stop()
```

**Key concepts:**
- Normalization removes case sensitivity
- Trimming removes whitespace differences
- Useful for handling data entry variations

### Exercise 2.4: Window-Based Deduplication

**Objective:** Keep the first record when duplicates are found.

**Create a script `dedupe_window.py`:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("WindowDeduplication") \
    .getOrCreate()

df = spark.read.csv('redundant_data.csv', header=True, inferSchema=True)

# Create window partitioned by email, ordered by id
window = Window.partitionBy('email').orderBy('id')

# Add row number
df_with_rownum = df.withColumn('row_num', F.row_number().over(window))

# Keep only first record for each email
df_deduped = df_with_rownum.filter(F.col('row_num') == 1).drop('row_num')

print(f"Original: {df.count()}")
print(f"Deduplicated: {df_deduped.count()}")

# Show which records were kept
print("\nKept records (first occurrence):")
df_deduped.show(10)

spark.stop()
```

**Use cases:**
- When you need to keep a specific record (first, last, most recent)
- When you need to preserve metadata about which record was kept

---

## Part 3: File-Level Duplicate Detection

### Exercise 3.1: Generate Duplicate Files

**Objective:** Create test files with known duplicates.

```bash
# In Docker container
docker-compose exec spark-master python /app/generate_duplicate_files.py 25 0.9 /app/data/duplicatefiles

# Or locally
python generate_duplicate_files.py 25 0.9 data/duplicatefiles
```

**Note:** Make sure the output directory exists. In Docker:
```bash
docker-compose exec spark-master mkdir -p /app/data/duplicatefiles
```

**What happens:**
- Creates files with random names
- Some files have identical content (duplicates)
- Summary shows which files are duplicates

### Exercise 3.2: Detect Duplicate Files by Hash

**Objective:** Find duplicate files using content hashing.

**Create a script `find_duplicate_files.py`:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import hashlib
import os

def calculate_file_hash(file_path):
    """Calculate MD5 hash of file content."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"Error hashing {file_path}: {e}")
        return None

spark = SparkSession.builder \
    .appName("DuplicateFileDetection") \
    .getOrCreate()

# Directory containing files
# Use /app/data/duplicatefiles in Docker, or data/duplicatefiles locally
file_dir = '/app/data/duplicatefiles' if os.path.exists('/app/data/duplicatefiles') else 'data/duplicatefiles'

# Get list of files
files = [os.path.join(file_dir, f) for f in os.listdir(file_dir) 
         if os.path.isfile(os.path.join(file_dir, f))]

# Calculate hashes
file_data = []
for file_path in files:
    file_hash = calculate_file_hash(file_path)
    if file_hash is not None:  # Only add files that were successfully hashed
        file_size = os.path.getsize(file_path)
        file_data.append({
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'file_hash': file_hash,
            'file_size': file_size
        })

# Create DataFrame
df = spark.createDataFrame(file_data)

print(f"Total files: {df.count()}")

# Find duplicate files (same hash)
duplicates = df.groupBy('file_hash', 'file_size') \
    .agg(F.collect_list('file_name').alias('duplicate_files'),
         F.count('*').alias('duplicate_count')) \
    .filter('duplicate_count > 1')

print(f"\nDuplicate file groups: {duplicates.count()}")
print("\nDuplicate files:")
duplicates.select('duplicate_files', 'file_size', 'duplicate_count') \
    .show(truncate=False)

# Show unique files
unique = df.groupBy('file_hash').count().filter('count == 1')
print(f"\nUnique files: {unique.count()}")

spark.stop()
```

**Key concepts:**
- Hash functions create unique fingerprints for file content
- Files with same hash have identical content
- Efficient for large-scale duplicate detection

### Exercise 3.3: Using Spark for Large-Scale File Deduplication

**Objective:** Use Spark to process many files in parallel.

**Create a script `spark_file_dedupe.py`:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import hashlib
import os

def hash_file(file_path):
    """Calculate file hash - Spark UDF compatible."""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except:
        return None

spark = SparkSession.builder \
    .appName("SparkFileDeduplication") \
    .getOrCreate()

# Get all files recursively
# Use /app/data/duplicatefiles in Docker, or data/duplicatefiles locally
file_dir = '/app/data/duplicatefiles' if os.path.exists('/app/data/duplicatefiles') else 'data/duplicatefiles'
files = []
for root, dirs, filenames in os.walk(file_dir):
    for filename in filenames:
        file_path = os.path.join(root, filename)
        files.append(file_path)

# Create RDD of file paths
files_rdd = spark.sparkContext.parallelize(files)

# Calculate hashes in parallel
file_hashes = files_rdd.map(lambda f: {
    'file_path': f,
    'file_name': os.path.basename(f),
    'file_hash': hash_file(f),
    'file_size': os.path.getsize(f) if os.path.exists(f) else 0
}).filter(lambda x: x['file_hash'] is not None)

# Convert to DataFrame
schema = StructType([
    StructField('file_path', StringType(), True),
    StructField('file_name', StringType(), True),
    StructField('file_hash', StringType(), True),
    StructField('file_size', LongType(), True)
])

df = spark.createDataFrame(file_hashes, schema)

# Find duplicates
duplicates = df.groupBy('file_hash') \
    .agg(F.collect_list('file_name').alias('duplicate_files'),
         F.count('*').alias('count'),
         F.first('file_size').alias('file_size')) \
    .filter('count > 1') \
    .orderBy(F.desc('count'))

print("Duplicate file groups:")
duplicates.select('duplicate_files', 'file_size', 'count').show(truncate=False)

spark.stop()
```

**Benefits:**
- Parallel processing of many files
- Scalable to large file systems
- Efficient use of Spark cluster resources

---

## Part 4: Advanced Techniques

### Exercise 4.1: Hash-Based Deduplication

**Objective:** Use hash functions for fast duplicate detection.

**Create a script `dedupe_hash.py`:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder \
    .appName("HashDeduplication") \
    .getOrCreate()

# Read the CSV file
# Use /app/data/ in Docker, or current directory locally
csv_path = '/app/data/redundant_data.csv' if os.path.exists('/app/data/redundant_data.csv') else 'redundant_data.csv'
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Create hash of all columns
df_with_hash = df.withColumn('record_hash', 
    F.md5(F.concat_ws('|', 
        F.col('id'), 
        F.col('name'), 
        F.col('email'), 
        F.col('address')
    ))
)

print(f"Original: {df.count()}")

# Remove duplicates by hash
df_deduped = df_with_hash.dropDuplicates(subset=['record_hash']).drop('record_hash')

print(f"Deduplicated: {df_deduped.count()}")

spark.stop()
```

**Advantages:**
- Fast comparison (hash vs. column comparison)
- Works well for large datasets
- Can partition by hash for better performance

### Exercise 4.2: Using the Comprehensive Deduplication Script

**Objective:** Use the provided `deduplicate_spark.py` script with different strategies.

```bash
# In Docker (scripts should be in /app/ directory)
# Exact duplicates
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/deduplicate_spark.py /app/data/redundant_data.csv /app/data/output_exact.parquet exact

# Normalized (case/whitespace insensitive)
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/deduplicate_spark.py /app/data/redundant_data.csv /app/data/output_normalized.parquet normalized

# Hash-based (fast)
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/deduplicate_spark.py /app/data/redundant_data.csv /app/data/output_hash.parquet spark_hash

# Window-based (keeps first)
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/deduplicate_spark.py /app/data/redundant_data.csv /app/data/output_window.parquet window

# Or locally
python deduplicate_spark.py redundant_data.csv output.parquet exact
python deduplicate_spark.py redundant_data.csv output.parquet normalized
python deduplicate_spark.py redundant_data.csv output.parquet spark_hash
python deduplicate_spark.py redundant_data.csv output.parquet window
```

**Compare results:**
```python
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("CompareResults").getOrCreate()

# Use /app/data/ in Docker, or current directory locally
base_path = '/app/data/' if os.path.exists('/app/data') else ''

exact = spark.read.parquet(f'{base_path}output_exact.parquet')
normalized = spark.read.parquet(f'{base_path}output_normalized.parquet')
hash_based = spark.read.parquet(f'{base_path}output_hash.parquet')

print(f"Exact method: {exact.count()} records")
print(f"Normalized method: {normalized.count()} records")
print(f"Hash method: {hash_based.count()} records")

spark.stop()
```

### Exercise 4.3: Approximate Methods - Bloom Filters and HyperLogLog

**Objective:** Learn about approximate duplicate detection and distinct counting methods that use minimal memory.

#### Understanding Approximate Methods

When working with very large datasets or memory-constrained environments, approximate methods can be more efficient than exact deduplication:

- **HyperLogLog**: Estimates distinct counts with minimal memory
- **Bloom Filter**: Probabilistic data structure for approximate duplicate detection

**When to use:**
- Memory is constrained
- Exact accuracy is not required
- Fast approximate results are acceptable
- Real-time monitoring and dashboards

#### Exercise 4.3.1: HyperLogLog for Distinct Count Estimation

**Create a script `hyperloglog_demo.py`:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("HyperLogLogDemo") \
    .getOrCreate()

# Read the CSV file
# Use /app/data/ in Docker, or current directory locally
import os
csv_path = '/app/data/redundant_data.csv' if os.path.exists('/app/data/redundant_data.csv') else 'redundant_data.csv'
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Exact distinct count (baseline)
exact_count = df.select('email').distinct().count()
print(f"Exact distinct emails: {exact_count}")

# HyperLogLog with different RSD (Relative Standard Deviation) values
# Lower RSD = higher accuracy but more memory
# Higher RSD = lower accuracy but less memory

# RSD = 0.01 (1% error, high accuracy)
hll_001 = df.agg(F.approx_count_distinct('email', 0.01).alias('count')).collect()[0]['count']
print(f"HyperLogLog (RSD=0.01): {hll_001}")
print(f"Error: {abs(exact_count - hll_001) / exact_count * 100:.2f}%")

# RSD = 0.05 (5% error, balanced)
hll_005 = df.agg(F.approx_count_distinct('email', 0.05).alias('count')).collect()[0]['count']
print(f"HyperLogLog (RSD=0.05): {hll_005}")
print(f"Error: {abs(exact_count - hll_005) / exact_count * 100:.2f}%")

# RSD = 0.1 (10% error, fastest)
hll_01 = df.agg(F.approx_count_distinct('email', 0.1).alias('count')).collect()[0]['count']
print(f"HyperLogLog (RSD=0.1): {hll_01}")
print(f"Error: {abs(exact_count - hll_01) / exact_count * 100:.2f}%")

spark.stop()
```

**Key concepts:**
- RSD (Relative Standard Deviation) controls accuracy vs. memory trade-off
- Lower RSD = more accurate but uses more memory
- Higher RSD = less accurate but uses less memory
- Recommended: RSD=0.05 for most use cases

#### Exercise 4.3.2: Using the Bloom Filter Script

**Run the provided Bloom Filter and HyperLogLog demonstration:**

```bash
# In Docker (script should be in /app/ directory)
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /app/bloom_filter_hyperloglog.py

# Or locally
spark-submit bloom_filter_hyperloglog.py
```

**What the script demonstrates:**
- HyperLogLog distinct count estimation with different RSD values
- Comparison with exact distinct count
- Incremental deduplication pattern
- Performance and memory usage comparison

**Expected output:**
```
Exact distinct count: 6277
HyperLogLog (RSD=0.01): 6213
Error: 1.02%

HyperLogLog (RSD=0.05): ~6200+
Error: ~1-2%

HyperLogLog (RSD=0.1): ~6200+
Error: ~1-2%
```

#### Understanding the Results

**Performance Comparison (from test results):**

| Method | Processing Time | Memory Usage | Error Rate | Use Case |
|--------|----------------|--------------|------------|----------|
| Exact `distinct()` | 0.27s | High | 0% | When exact count needed |
| HyperLogLog (RSD=0.01) | 1.12s | Very Low (~1.5KB) | ~1% | High accuracy needed |
| HyperLogLog (RSD=0.05) | ~0.2s | Very Low (~1.5KB) | ~1-2% | **Recommended** |
| HyperLogLog (RSD=0.1) | ~0.1s | Very Low (~1.5KB) | ~1-2% | Monitoring/dashboards |

**Key findings:**
- HyperLogLog uses minimal memory (~1.5KB per counter) regardless of dataset size
- For large datasets, HyperLogLog can be faster than exact methods
- RSD=0.05 provides the best balance of speed and accuracy
- Error rates are typically 1-2% even with higher RSD values

**When to use HyperLogLog:**
- Monitoring and dashboards (approximate counts are sufficient)
- Large datasets where exact distinct count is expensive
- Memory-constrained environments
- Real-time analytics where speed is critical

**When NOT to use HyperLogLog:**
- When exact counts are required (compliance, audit)
- Small datasets (exact method is fast enough)
- When error tolerance is zero

#### Exercise 4.3.3: Incremental Deduplication Pattern

**Objective:** Learn how to check new data against existing data for duplicates.

**Create a script `incremental_dedupe.py`:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IncrementalDeduplication") \
    .getOrCreate()

# Existing data (already processed)
# Use /app/data/ in Docker, or current directory locally
import os
base_path = '/app/data/' if os.path.exists('/app/data') else ''
existing_df = spark.read.csv(f'{base_path}redundant_data.csv', header=True, inferSchema=True)

# New data to check (create new_data.csv first or use existing file)
new_df = spark.read.csv(f'{base_path}new_data.csv', header=True, inferSchema=True)

# Find records in new data that don't exist in existing data
# (Left Anti Join - keeps only records from left that don't match right)
unique_new_records = new_df.join(
    existing_df.select('email').distinct(),
    on='email',
    how='left_anti'
)

print(f"New records: {new_df.count()}")
print(f"Unique new records (not in existing): {unique_new_records.count()}")
print(f"Duplicates found: {new_df.count() - unique_new_records.count()}")

# Show sample unique records
unique_new_records.show(10)

spark.stop()
```

**Use cases:**
- Streaming data deduplication
- Incremental data processing
- Checking new records against existing database
- Preventing duplicate inserts

**Note:** For true Bloom Filter implementation, you would use a probabilistic data structure. In Spark, left anti join provides exact duplicate detection. For approximate Bloom Filter, consider using the `pybloom_live` library.

---

## Exercises

### Exercise A: Deduplicate Customer Data

**Scenario:** You have a customer database with potential duplicates. Some customers have:
- Same email but different names (typos)
- Same name but different email formats
- Case variations in names

**Task:**
1. Generate a dataset with 5000 customer records
2. Apply normalized deduplication
3. Report how many duplicates were found
4. Save the deduplicated data

**Solution template:**
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("CustomerDeduplication").getOrCreate()

# Your code here

spark.stop()
```

### Exercise B: Find Duplicate Images

**Scenario:** You have a directory of image files. Some images are exact duplicates.

**Task:**
1. Create a script that finds duplicate images by hash
2. Group duplicates together
3. Report total storage that could be saved by removing duplicates

**Hint:** Use the file hashing approach from Part 3.

### Exercise C: Streaming Deduplication

**Scenario:** You're receiving data in real-time and need to deduplicate as it arrives.

**Task:**
1. Simulate streaming data (read CSV in batches)
2. Use a Bloom Filter or hash set to track seen records
3. Filter out duplicates in real-time

**Advanced:** Implement using Spark Streaming.

---

## Best Practices

### 1. Choose the Right Method

- **Exact duplicates:** Use `dropDuplicates()` - fastest
- **Case/whitespace variations:** Normalize first, then deduplicate
- **Large datasets:** Use hash-based methods
- **Need to keep specific records:** Use window functions

### 2. Performance Optimization

```python
# Cache intermediate results if reused
df.cache()

# Partition data appropriately
df.repartition(10, 'email')

# Use column pruning
df.select('id', 'email', 'name')

# Write in efficient formats
df.write.parquet('output.parquet')  # Better than CSV
```

### 3. Data Quality Checks

```python
# Check for nulls before deduplication
df.filter(F.col('email').isNull()).count()

# Validate data types
df.printSchema()

# Check for empty strings
df.filter(F.trim(F.col('email')) == '').count()
```

### 4. Monitoring and Logging

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

original_count = df.count()
logger.info(f"Original records: {original_count}")

deduped_count = df_deduped.count()
logger.info(f"Deduplicated records: {deduped_count}")
logger.info(f"Duplicates removed: {original_count - deduped_count}")
```

### 5. Testing Your Deduplication

```python
# Verify no duplicates remain
assert df_deduped.count() == df_deduped.dropDuplicates().count()

# Check specific columns
assert df_deduped.select('email').distinct().count() == df_deduped.count()

# Sample verification
sample = df_deduped.sample(0.1)
assert sample.count() == sample.dropDuplicates().count()
```

---

## Detailed Results and Expected Outputs

This section provides detailed expected results for each exercise to help you verify your implementations and understand what successful execution looks like.

### Part 1: Understanding Duplicate Data

#### Exercise 1.1: Generate Test Dataset - Expected Results

**Command:**
```bash
python generate_dataset.py 1000 redundant_data.csv
```

**Expected Output:**
```
Generating 1000 records with duplicates and variations...
Generated 1000 records and saved to redundant_data.csv
Expected unique records: ~500
```

**File Verification:**
- File created: `redundant_data.csv`
- Total records: 1000
- Columns: `id`, `name`, `email`, `address`
- Contains intentional duplicates and variations

#### Exercise 1.2: Inspect Data - Expected Results

**Sample Output:**
```
Total records: 1000

First 10 records:
      id            name                    email                    address
0  ID0001      John Smith    john.smith@gmail.com  1234 Main St, New York, NY
1  ID0002      Jane Doe      jane.doe@yahoo.com    5678 Oak Ave, Los Angeles, CA
...

Duplicate records (exact):
Number of exact duplicates: 250

Sample duplicates:
      id            name                    email                    address
5  ID0001      John Smith    john.smith@gmail.com  1234 Main St, New York, NY
12 ID0002      Jane Doe      jane.doe@yahoo.com    5678 Oak Ave, Los Angeles, CA
...
```

**Key Observations:**
- Approximately 50% of records are duplicates
- Duplicates are exact copies of earlier records
- Some records have variations (case, whitespace)

---

### Part 2: Record-Level Deduplication

#### Exercise 2.1: Exact Duplicate Removal - Expected Results

**Sample Spark Output:**
```
Original record count: 1000
Deduplicated record count: 500

Sample deduplicated records:
+------+---------------+---------------------------+---------------------------------+
|id    |name           |email                      |address                          |
+------+---------------+---------------------------+---------------------------------+
|ID0001|John Smith     |john.smith@gmail.com       |1234 Main St, New York, NY       |
|ID0002|Jane Doe       |jane.doe@yahoo.com         |5678 Oak Ave, Los Angeles, CA    |
|ID0003|Michael Johnson|michael.johnson@hotmail.com|9012 Park Blvd, Chicago, IL      |
...
+------+---------------+---------------------------+---------------------------------+
```

**Performance Metrics:**
- Processing time: ~2-5 seconds (depending on cluster)
- Duplicates removed: ~500 records
- Deduplication rate: ~50%

**Output Files:**
- `deduplicated_exact.parquet/` directory created
- Contains deduplicated data in Parquet format

#### Exercise 2.2: Column-Based Deduplication - Expected Results

**Sample Output:**
```
Original records: 1000
Unique emails: 750

Records grouped by email:
+---------------------------+-----+
|email                      |count|
+---------------------------+-----+
|john.smith@gmail.com       |2    |
|jane.doe@yahoo.com         |3    |
|michael.johnson@hotmail.com|1    |
...
+---------------------------+-----+
```

**Key Differences:**
- Deduplicating by email only removes records with identical emails
- Different names with same email are considered duplicates
- More records retained compared to all-column deduplication

#### Exercise 2.3: Normalized Deduplication - Expected Results

**Sample Output:**
```
Original records: 1000
Normalized deduplicated records: 450

Normalized duplicate examples:
+---------------------------+-----+
|email_normalized           |count|
+---------------------------+-----+
|john.smith@gmail.com       |3    |
|JANE.DOE@YAHOO.COM         |2    |
|michael.johnson@hotmail.com|2    |
...
+---------------------------+-----+
```

**What This Shows:**
- Normalization catches case variations (e.g., "John" vs "JOHN")
- Trimming removes whitespace differences
- More duplicates found than exact method
- Lower final count indicates more aggressive deduplication

#### Exercise 2.4: Window-Based Deduplication - Expected Results

**Sample Output:**
```
Original: 1000
Deduplicated: 500

Kept records (first occurrence):
+------+---------------+---------------------------+---------------------------------+
|id    |name           |email                      |address                          |
+------+---------------+---------------------------+---------------------------------+
|ID0001|John Smith     |john.smith@gmail.com       |1234 Main St, New York, NY       |
|ID0002|Jane Doe       |jane.doe@yahoo.com         |5678 Oak Ave, Los Angeles, CA    |
...
+------+---------------+---------------------------+---------------------------------+
```

**Row Number Column (before filtering):**
```
+------+---------------+---------------------------+-----+
|id    |email          |name                       |row_num|
+------+---------------+---------------------------+-----+
|ID0001|john.smith@gmail.com|John Smith            |1    |
|ID0005|john.smith@gmail.com|John Smith            |2    |
|ID0012|john.smith@gmail.com|John Smith            |3    |
...
+------+---------------+---------------------------+-----+
```

**Key Points:**
- `row_num = 1` indicates the first occurrence (kept)
- `row_num > 1` indicates duplicates (filtered out)
- Preserves the first record for each email group

---

### Part 3: File-Level Duplicate Detection

#### Exercise 3.1: Generate Duplicate Files - Expected Results

**Command:**
```bash
python generate_duplicate_files.py 25 0.9 data/duplicatefiles
```

**Expected Output:**
```
Generating 25 files with 90% unique content...
Created 25 files in data/duplicatefiles/
Unique files: 22
Duplicate groups: 3
Duplicate files: 6

Duplicate Groups:
Group 1:
  - gQUUChMXTTvM.txt (484 bytes)
  - NyJlbhi3rPbL.txt (484 bytes)
  Content hash: b02a71a2e9fbe1ee6c85b17a241c1490

Group 2:
  - qHvlF6fjlIVt.txt (797 bytes)
  - xjY4evTc8xdu.txt (797 bytes)
  Content hash: 7d60a2ca941faca9aed0428d355e8502

Group 3:
  - BScs1pTAfSlm.txt (981 bytes)
  - KlQIyFuTyVLm.txt (981 bytes)
  Content hash: 678357c80f95d25dcf6b87d048b44006
```

#### Exercise 3.2: Detect Duplicate Files - Expected Results

**Sample Output:**
```
Total files: 25

Duplicate file groups: 3

Duplicate files:
+--------------------------------+--------+----------------+
|duplicate_files                |file_size|duplicate_count|
+--------------------------------+--------+----------------+
|[gQUUChMXTTvM.txt, NyJlbhi3rPbL.txt]|484    |2              |
|[qHvlF6fjlIVt.txt, xjY4evTc8xdu.txt]|797    |2              |
|[BScs1pTAfSlm.txt, KlQIyFuTyVLm.txt]|981    |2              |
+--------------------------------+--------+----------------+

Unique files: 22
```

**Verification:**
- 3 duplicate groups identified
- 6 duplicate files total (3 pairs)
- 22 unique files
- All files with same hash have identical content

#### Exercise 3.3: Spark File Deduplication - Expected Results

**Sample Output:**
```
Duplicate file groups:
+--------------------------------+--------+-----+
|duplicate_files                |file_size|count|
+--------------------------------+--------+-----+
|[gQUUChMXTTvM.txt, NyJlbhi3rPbL.txt]|484    |2    |
|[qHvlF6fjlIVt.txt, xjY4evTc8xdu.txt]|797    |2    |
|[BScs1pTAfSlm.txt, KlQIyFuTyVLm.txt]|981    |2    |
+--------------------------------+--------+-----+
```

**Performance:**
- Parallel processing of all files
- Faster than sequential processing for large directories
- Scalable to thousands of files

---

### Part 4: Advanced Techniques

#### Exercise 4.1: Hash-Based Deduplication - Expected Results

**Sample Output:**
```
Original: 1000
Deduplicated: 500

Sample records with hash:
+------+---------------+---------------------------+--------------------------------+----------------------------------+
|id    |name           |email                      |address                         |record_hash                       |
+------+---------------+---------------------------+--------------------------------+----------------------------------+
|ID0001|John Smith     |john.smith@gmail.com       |1234 Main St, New York, NY      |a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6|
|ID0002|Jane Doe       |jane.doe@yahoo.com         |5678 Oak Ave, Los Angeles, CA   |b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7|
...
+------+---------------+---------------------------+--------------------------------+----------------------------------+
```

**Performance Comparison:**
- Hash method: ~3-4 seconds
- Exact method: ~2-5 seconds
- Hash method scales better for very large datasets

#### Exercise 4.2: Comprehensive Script Results - Expected Comparison

**Results Summary Table:**

| Method | Input Records | Output Records | Duplicates Removed | Processing Time | Use Case |
|--------|--------------|---------------|-------------------|-----------------|----------|
| `exact` | 1000 | ~500 | ~500 (50%) | ~2-5s | Remove identical records |
| `normalized` | 1000 | ~450 | ~550 (55%) | ~3-6s | Handle case/whitespace |
| `spark_hash` | 1000 | ~950 | ~50 (5%) | ~2-4s | Fast hash-based dedup |
| `window` | 1000 | ~500 | ~500 (50%) | ~4-7s | Keep first/last record |

**Sample Comparison Output:**
```
Exact method: 500 records
Normalized method: 450 records
Hash method: 950 records
Window method: 500 records
```

**Key Insights:**
- Normalized method finds most duplicates (handles variations)
- Hash method is fastest but less aggressive
- Exact and window methods produce similar results
- Choose method based on your specific needs

#### Exercise 4.3: Bloom Filter and HyperLogLog - Expected Results

**HyperLogLog Distinct Count Estimation:**

**Sample Output:**
```
Exact distinct emails: 6277
HyperLogLog (RSD=0.01): 6213
Error: 1.02%

HyperLogLog (RSD=0.05): ~6200+
Error: ~1-2%

HyperLogLog (RSD=0.1): ~6200+
Error: ~1-2%
```

**Performance Comparison (from test results on 410,000 records):**

| Method | Processing Time | Memory Usage | Error Rate | Distinct Count |
|--------|----------------|--------------|------------|----------------|
| Exact `distinct()` | 0.27s | High | 0% | 6,277 |
| HyperLogLog (RSD=0.01) | 1.12s | Very Low (~1.5KB) | 1.02% | 6,213 |
| HyperLogLog (RSD=0.05) | ~0.2s | Very Low (~1.5KB) | ~1-2% | ~6,200+ |
| HyperLogLog (RSD=0.1) | ~0.1s | Very Low (~1.5KB) | ~1-2% | ~6,200+ |

**Key Observations:**
- HyperLogLog uses minimal memory (~1.5KB per counter) regardless of dataset size
- For large datasets, HyperLogLog (RSD=0.05) can be faster than exact method
- Error rates are typically 1-2% even with higher RSD values
- RSD=0.05 provides the best balance of speed and accuracy

**Incremental Deduplication Results:**

**Sample Output:**
```
New records: 10000
Unique new records (not in existing): 8500
Duplicates found: 1500

Sample unique records:
+------+--------------+--------------------------+---------------------------+
|id    |name          |email                     |address                    |
+------+--------------+--------------------------+---------------------------+
|ID9642|sarah jackson |SARAH.JACKSON@COMPANY.COM |8843 Main St New York AZ   |
|ID9582|Mike Moore    |michaelmoore@outlookcom   |4036 Oak Ave Phoenix AZ    |
...
+------+--------------+--------------------------+---------------------------+
```

**Performance Metrics:**
- Left anti join provides exact duplicate detection (no false positives)
- Efficient for checking new data against existing large datasets
- Scalable to millions of records

**When to Use Each Method:**

**Use HyperLogLog when:**
- Monitoring and dashboards (approximate counts sufficient)
- Large datasets where exact distinct count is expensive
- Memory-constrained environments
- Real-time analytics where speed is critical

**Use Exact Methods when:**
- Exact counts are required (compliance, audit)
- Small to medium datasets (exact method is fast enough)
- Final deduplication after approximate pre-filtering

**Use Incremental Deduplication when:**
- Processing streaming data
- Checking new records against existing database
- Preventing duplicate inserts
- Memory-efficient duplicate detection

**See [bloom_filter_hyperloglog_results.md](bloom_filter_hyperloglog_results.md) for detailed test results and analysis.**

---

### Exercise Results

#### Exercise A: Customer Data Deduplication - Expected Results

**Sample Output:**
```
Generating 5000 customer records...
Generated 5000 records with duplicates and variations

Original customer records: 5000
After normalized deduplication: 2250
Duplicates found: 2750 (55%)

Sample deduplicated customers:
+------+----------------+---------------------------+---------------------------------+
|cust_id|name            |email                      |address                          |
+------+----------------+---------------------------+---------------------------------+
|C001  |John Smith       |john.smith@gmail.com       |123 Main St, New York, NY        |
|C002  |Jane Doe         |jane.doe@yahoo.com         |456 Oak Ave, Los Angeles, CA     |
...
+------+----------------+---------------------------+---------------------------------+

Report:
- Total duplicates removed: 2750
- Unique customers: 2250
- Deduplication rate: 55%
- Output saved to: deduplicated_customers.parquet
```

#### Exercise B: Duplicate Images - Expected Results

**Sample Output:**
```
Scanning directory: images/
Total files: 150
Processing files...

Duplicate image groups: 12
Duplicate files: 24

Storage Analysis:
- Total storage: 2.5 GB
- Duplicate storage: 450 MB
- Potential savings: 450 MB (18%)

Duplicate Groups:
Group 1 (3 files, 45 MB):
  - photo1.jpg
  - photo1_copy.jpg
  - IMG_001.jpg
  Hash: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6

Group 2 (2 files, 12 MB):
  - image.png
  - image_duplicate.png
  Hash: b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7
...
```

---

### Performance Benchmarks

#### Small Dataset (1,000 records)

| Method | Time (seconds) | Memory (MB) | Duplicates Removed |
|--------|---------------|-------------|-------------------|
| Exact | 2-5 | 50-100 | 50% |
| Normalized | 3-6 | 60-120 | 55% |
| Hash | 2-4 | 40-80 | 5% |
| Window | 4-7 | 70-140 | 50% |

#### Medium Dataset (10,000 records)

| Method | Time (seconds) | Memory (MB) | Duplicates Removed |
|--------|---------------|-------------|-------------------|
| Exact | 5-10 | 200-400 | 50% |
| Normalized | 8-15 | 250-500 | 55% |
| Hash | 4-8 | 150-300 | 5% |
| Window | 10-20 | 300-600 | 50% |

#### Large Dataset (100,000+ records)

| Method | Time (seconds) | Memory (MB) | Duplicates Removed |
|--------|---------------|-------------|-------------------|
| Exact | 15-30 | 500-1000 | 50% |
| Normalized | 25-50 | 600-1200 | 55% |
| Hash | 10-20 | 400-800 | 5% |
| Window | 30-60 | 700-1400 | 50% |

*Note: Performance varies based on cluster configuration, data characteristics, and network conditions.*

---

### Verification Checklist

After completing each exercise, verify your results:

**Exercise 1.1-1.2:**
- [ ] CSV file created with 1000 records
- [ ] Can identify duplicate patterns in data
- [ ] Understand difference between exact and near-duplicates

**Exercise 2.1:**
- [ ] Exact deduplication reduces records by ~50%
- [ ] Parquet file created successfully
- [ ] No exact duplicates remain in output

**Exercise 2.2:**
- [ ] Email-based deduplication works correctly
- [ ] Can see groups of records with same email
- [ ] More records retained than all-column dedup

**Exercise 2.3:**
- [ ] Normalization handles case differences
- [ ] More duplicates found than exact method
- [ ] Lower final count than exact method

**Exercise 2.4:**
- [ ] Window function adds row numbers correctly
- [ ] First record kept for each group
- [ ] Duplicates filtered out properly

**Exercise 3.1-3.3:**
- [ ] Duplicate files generated successfully
- [ ] Hash-based detection finds all duplicates
- [ ] Spark processes files in parallel

**Exercise 4.1-4.2:**
- [ ] Hash-based deduplication works
- [ ] Can compare different methods
- [ ] Understand performance trade-offs

**Exercise 4.3:**
- [ ] HyperLogLog distinct count estimation works
- [ ] Understand RSD parameter and accuracy trade-offs
- [ ] Can compare HyperLogLog with exact distinct count
- [ ] Incremental deduplication pattern implemented
- [ ] Understand when to use approximate vs. exact methods

---

### Common Output Patterns

#### Successful Execution Pattern

```
Original records: 1000
Processing...
Deduplicated records: 500
Duplicates removed: 500 (50.0%)
Output saved to: output.parquet
```

#### Error Pattern (to avoid)

```
Original records: 1000
Processing...
Error: NullPointerException
```

**Solution:** Check for null values before deduplication:
```python
df = df.filter(F.col('email').isNotNull())
```

#### Performance Warning Pattern

```
WARN: Large number of partitions (200). Consider coalescing.
```

**Solution:** Reduce partitions:
```python
df = df.coalesce(10)
```

---

## Summary

In this lab, you learned:

1. **Data Generation:** How to create test datasets with controlled duplicates
2. **Exact Deduplication:** Removing identical records
3. **Normalized Deduplication:** Handling case and whitespace variations
4. **Window Functions:** Keeping specific records when duplicates exist
5. **File-Level Detection:** Finding duplicate files by content hash
6. **Hash-Based Methods:** Fast duplicate detection for large datasets
7. **Approximate Methods:** HyperLogLog for distinct count estimation and Bloom Filters for approximate duplicate detection
8. **Performance Optimization:** Best practices for Spark jobs

### Next Steps

- Experiment with different deduplication strategies on your own data
- Try combining multiple methods (e.g., normalize then hash)
- Explore fuzzy matching for near-duplicates (see `advanced_deduplication_methods.md`)
- Test HyperLogLog with different RSD values on your datasets
- Implement incremental deduplication for streaming scenarios
- Review detailed results in [bloom_filter_hyperloglog_results.md](bloom_filter_hyperloglog_results.md)

### Additional Resources

- [deduplication_guide.md](deduplication_guide.md) - Detailed method explanations
- [advanced_deduplication_methods.md](advanced_deduplication_methods.md) - Advanced techniques
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

---

## Troubleshooting

### Common Issues

**Issue:** Spark job runs slowly
- **Solution:** Increase partitions, cache intermediate results, use hash-based methods

**Issue:** Out of memory errors
- **Solution:** Reduce partition size, use disk-based joins, increase executor memory

**Issue:** Duplicates not being removed
- **Solution:** Check for whitespace, case differences, null values, data types

**Issue:** Docker containers not starting
- **Solution:** Check ports are available, increase Docker memory allocation

---

**Lab Completion Checklist:**
- [ ] Generated test dataset with duplicates
- [ ] Performed exact deduplication
- [ ] Implemented normalized deduplication
- [ ] Used window functions for deduplication
- [ ] Detected duplicate files by hash
- [ ] Compared different deduplication methods
- [ ] Completed at least one exercise
- [ ] Reviewed best practices

**Congratulations!** You've completed the Data Deduplication Lab Guide. 🎉

