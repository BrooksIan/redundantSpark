# Data Deduplication Lab with Apache Spark

A hands-on lab for learning data deduplication techniques using Apache Spark in Cloudera AI Workbench.

## Lab Overview

This lab teaches you how to:
- **Identify and remove duplicate records** from datasets using various Spark techniques
- **Compare different deduplication methods** and understand when to use each
- **Work with large datasets** efficiently using distributed processing
- **Use approximate methods** (Bloom Filters, HyperLogLog) for memory-efficient duplicate detection
- **Perform file-level deduplication** to find duplicate files by content

## Prerequisites

- Access to **Cloudera AI Workbench** (or similar Spark environment)
- Basic Python knowledge
- Familiarity with data processing concepts

## Quick Start

### Step 1: Upload Files to Cloudera AI Workbench

Upload the following files to your Cloudera AI Workbench project:

**Required Core Scripts (in project root):**
- `deduplicate_spark.py` - Main deduplication script
- `generate_dataset.py` - Dataset generation script
- `bloom_filter_hyperloglog.py` - Approximate methods demonstration
- `bloom_filter_file_deduplication.py` - File-level deduplication

**Recommended:**
- `notebooks/` directory - Jupyter notebooks for guided exercises (recommended way to learn)
- `examples/` directory - Example scripts for reference

**Important**: The Python scripts must be in the **project root directory** (same level as the `notebooks/` folder). The notebooks will automatically add the project root to the Python path.

### Step 1a: Using Jupyter Notebooks (Recommended)

The easiest way to get started is using the provided Jupyter notebooks:

1. **Start with**: `notebooks/00_Getting_Started.ipynb` - Setup and introduction
2. **Then complete**: 
   - `notebooks/01_Basic_Deduplication.ipynb` - Exercise 1
   - `notebooks/02_Compare_Methods.ipynb` - Exercise 2
   - `notebooks/03_Approximate_Methods.ipynb` - Exercise 3
   - `notebooks/04_File_Level_Deduplication.ipynb` - Exercise 4

The notebooks provide step-by-step guidance with code cells you can run directly.

### Step 2: Generate Sample Data

```python
# Generate a dataset with duplicates
python generate_dataset.py 1000 data/redundant_data.csv
```

### Step 3: Run Deduplication

```python
from deduplicate_spark import create_spark_session, process_file_spark

# Create Spark session (automatically configured for Cloudera)
spark = create_spark_session("DeduplicationLab")

# Process the file using exact deduplication
stats = process_file_spark(
    spark,
    "data/redundant_data.csv",
    output_dir=None,  # Uses /tmp/results in Cloudera
    method='exact'
)

spark.stop()
```

## Lab Exercises

### Exercise 1: Basic Deduplication

**Objective**: Remove exact duplicates from a dataset

```python
from deduplicate_spark import create_spark_session, process_file_spark

spark = create_spark_session("Exercise1")

# Generate data
# python generate_dataset.py 1000 data/exercise1.csv

# Deduplicate using exact method
stats = process_file_spark(spark, "data/exercise1.csv", method='exact')

print(f"Original records: {stats['original_count']}")
print(f"Unique records: {stats['unique_count']}")
print(f"Duplicates removed: {stats['duplicates_removed']}")

spark.stop()
```

**Questions to Answer**:
- How many duplicates were found?
- What percentage of records were duplicates?
- Where are the results saved?

### Exercise 2: Compare Deduplication Methods

**Objective**: Compare different deduplication strategies

```python
from deduplicate_spark import create_spark_session, process_file_spark

spark = create_spark_session("Exercise2")

methods = ['exact', 'normalized', 'spark_hash']

for method in methods:
    print(f"\n{'='*70}")
    print(f"Testing method: {method}")
    print('='*70)
    stats = process_file_spark(spark, "data/redundant_data.csv", method=method)
    print(f"Deduplication rate: {stats['deduplication_rate']:.2f}%")

spark.stop()
```

**Questions to Answer**:
- Which method removed the most duplicates?
- Which method was fastest?
- When would you use each method?

### Exercise 3: Approximate Methods

**Objective**: Use HyperLogLog for fast distinct count estimation

```python
from bloom_filter_hyperloglog import create_spark_session, hyperloglog_distinct_count
from pyspark.sql import SparkSession

spark = create_spark_session("Exercise3")

# Read your data
df = spark.read.csv("data/redundant_data.csv", header=True, inferSchema=True)

# Estimate distinct count using HyperLogLog
result = hyperloglog_distinct_count(df, column='email', rsd=0.05)

print(f"Approximate distinct emails: {result['approx_distinct']:,}")
print(f"Exact distinct emails: {result['exact_distinct']:,}")
print(f"Error: {result['error_percent']:.2f}%")

spark.stop()
```

**Questions to Answer**:
- How accurate is the approximation?
- How much faster is it than exact counting?
- When would approximate methods be useful?

### Exercise 4: File-Level Deduplication

**Objective**: Find duplicate files by content hash

```python
from deduplicate_spark import create_spark_session, deduplicate_files
import glob
import os

spark = create_spark_session("Exercise4")

# Find files to analyze
file_paths = glob.glob("data/duplicatefiles/*")
file_paths = [f for f in file_paths if os.path.isfile(f)]

# Deduplicate files
deduplicate_files(spark, file_paths, output_dir=None)

spark.stop()
```

**Questions to Answer**:
- How many duplicate files were found?
- What is the total space that could be saved?
- How does file deduplication differ from record deduplication?

## Available Deduplication Methods

| Method | Description | Use Case |
|--------|-------------|----------|
| `exact` | Simple exact duplicate removal | Fastest, removes exact duplicates |
| `normalized` | Normalize then deduplicate | Handles case/whitespace variations |
| `spark_hash` | Hash-based deduplication | Very fast for exact duplicates |
| `checksum_md5` | MD5 hash-based | Content-based deduplication |
| `checksum_sha256` | SHA-256 hash-based | More secure hash-based deduplication |
| `window` | Window-based (keep first/last) | When you need to keep specific records |
| `lsh` | Locality-Sensitive Hashing | Scalable fuzzy matching |

## Lab Resources

### Documentation

- **[Cloudera AI Workbench Guide](documents/CLOUDERA_AI_WORKBENCH.md)** - Complete guide for using this lab in Cloudera AI Workbench
- **[Lab Guide](documents/lab_guide.md)** - Step-by-step tutorial teaching Python and Spark
- **[Deduplication Guide](documents/deduplication_guide.md)** - Detailed explanation of each method
- **[Advanced Methods](documents/advanced_deduplication_methods.md)** - Advanced techniques and best practices
- **[Project Structure](documents/PROJECT_STRUCTURE.md)** - Project organization and file descriptions

### For Local/Docker Setup

If you're not using Cloudera AI Workbench, see:
- **[Local Setup Guide](documents/LOCAL_SETUP.md)** - Setting up locally or with Docker
- **[Docker Commands Reference](documents/docker_commands.md)** - Complete Docker reference

## Output Locations

In Cloudera AI Workbench:
- **Results are saved to**: `/tmp/results/` (accessible by all Spark executors)
- Files are automatically created with appropriate permissions
- Use absolute paths if you need a specific location

## Troubleshooting

### Common Issues

**"A secret key must be specified"**
- The code automatically handles this in Cloudera environments
- If you see this error, ensure you're using the latest version of `deduplicate_spark.py`

**"Mkdirs failed"**
- Results are automatically written to `/tmp/results` in Cloudera
- This path is accessible by all Spark executors

**File not found errors**
- Ensure data files are accessible from the Spark driver
- In Cloudera, you may need to use HDFS paths: `/user/username/data/file.csv`

See [CLOUDERA_AI_WORKBENCH.md](documents/CLOUDERA_AI_WORKBENCH.md) for detailed troubleshooting.

## Learning Objectives

By completing this lab, you will:

✅ Understand different deduplication strategies and when to use each  
✅ Be able to process large datasets efficiently with Spark  
✅ Know how to use approximate methods for memory-efficient operations  
✅ Understand file-level vs record-level deduplication  
✅ Be able to optimize Spark jobs for deduplication tasks  

## Next Steps

1. Complete all exercises above
2. Experiment with different dataset sizes
3. Try combining multiple deduplication methods
4. Explore the advanced methods in `documents/advanced_deduplication_methods.md`
5. Review test results in `documents/results.md` to see performance comparisons

## Support

For issues specific to:
- **Cloudera AI Workbench**: See [CLOUDERA_AI_WORKBENCH.md](documents/CLOUDERA_AI_WORKBENCH.md)
- **Local/Docker setup**: See [LOCAL_SETUP.md](documents/LOCAL_SETUP.md)
- **General questions**: Review the documentation in the `documents/` directory
