# Advanced Apache Spark Duplicate Detection Methods

This document covers additional methods for duplicate detection in Apache Spark, including checksum-based, file-level, and other advanced techniques.

## 1. Checksum/Hash-Based Deduplication

### Method 1: Content-Based Hashing (MD5, SHA-256)

**Best for:** Detecting exact duplicate records regardless of column order or detecting duplicate file contents.

#### Row-Level Content Hashing

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib
import json

def compute_row_hash(*cols):
    """Compute MD5 hash of row content."""
    row_str = json.dumps([str(c) if c is not None else "" for c in cols], sort_keys=True)
    return hashlib.md5(row_str.encode()).hexdigest()

# Register as UDF
hash_udf = F.udf(compute_row_hash, StringType())

# Add hash column
df_with_hash = df.withColumn(
    'content_hash',
    hash_udf(F.col('name'), F.col('email'), F.col('address'))
)

# Remove duplicates based on hash
df_unique = df_with_hash.dropDuplicates(subset=['content_hash']).drop('content_hash')
```

#### Using Spark's Built-in Hash Functions

```python
from pyspark.sql import functions as F

# Create hash from multiple columns
df_with_hash = df.withColumn(
    'row_hash',
    F.md5(F.concat_ws('|', F.col('name'), F.col('email'), F.col('address')))
)

# Or use hash() function (faster, but may have collisions)
df_with_hash = df.withColumn(
    'row_hash',
    F.hash(F.col('name'), F.col('email'), F.col('address'))
)

# Deduplicate based on hash
df_unique = df_with_hash.dropDuplicates(subset=['row_hash']).drop('row_hash')
```

**Advantages:**
- Fast comparison (hash comparison vs. full row comparison)
- Works across different schemas if content is the same
- Can detect duplicates even if column order differs

**Use Cases:**
- Data migration deduplication
- Cross-database duplicate detection
- Detecting duplicate content in different formats

### Method 2: SHA-256 for Cryptographic-Grade Deduplication

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib

def sha256_hash(*cols):
    """Compute SHA-256 hash (more secure, less collisions)."""
    row_str = '|'.join([str(c) if c is not None else "" for c in cols])
    return hashlib.sha256(row_str.encode()).hexdigest()

sha256_udf = F.udf(sha256_hash, StringType())

df_with_hash = df.withColumn(
    'sha256_hash',
    sha256_udf(F.col('name'), F.col('email'), F.col('address'))
)

df_unique = df_with_hash.dropDuplicates(subset=['sha256_hash']).drop('sha256_hash')
```

## 2. File-Level Deduplication

### Method 1: Detecting Duplicate Files by Content

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib
import os

def compute_file_hash(file_path):
    """Compute MD5 hash of file content."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        return None

# For local files
def deduplicate_files_by_content(spark, file_paths):
    """Deduplicate files based on content hash."""
    file_data = []
    for file_path in file_paths:
        if os.path.exists(file_path):
            file_hash = compute_file_hash(file_path)
            file_data.append({
                'file_path': file_path,
                'file_name': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path),
                'content_hash': file_hash
            })
    
    df_files = spark.createDataFrame(file_data)
    
    # Group by content hash to find duplicates
    duplicate_files = df_files.groupBy('content_hash').agg(
        F.collect_list('file_path').alias('duplicate_paths'),
        F.count('*').alias('duplicate_count')
    ).filter(F.col('duplicate_count') > 1)
    
    # Keep one file per hash (e.g., keep the first alphabetically)
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy('content_hash').orderBy('file_path')
    df_unique_files = df_files.withColumn('row_num', F.row_number().over(window_spec)) \
                               .filter(F.col('row_num') == 1) \
                               .drop('row_num')
    
    return df_unique_files, duplicate_files
```

### Method 2: Using Spark to Read and Hash File Contents

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib

def hash_file_content(content):
    """Hash file content."""
    if content:
        return hashlib.md5(content.encode()).hexdigest()
    return None

hash_udf = F.udf(hash_file_content, StringType())

# Read files as text
df_files = spark.read.text("path/to/files/*")

# Add file path and hash
df_files_with_hash = df_files.withColumn(
    'file_path',
    F.input_file_name()
).withColumn(
    'content_hash',
    hash_udf(F.col('value'))
)

# Group by hash to find duplicates
duplicate_files = df_files_with_hash.groupBy('content_hash').agg(
    F.collect_list('file_path').alias('file_paths'),
    F.count('*').alias('count')
).filter(F.col('count') > 1)
```

### Method 3: Deduplicating Files in Distributed Storage (S3, HDFS)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import boto3  # For S3

def list_files_s3(bucket, prefix=''):
    """List files in S3 bucket."""
    s3 = boto3.client('s3')
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            files.append({
                'file_path': f"s3://{bucket}/{obj['Key']}",
                'file_name': obj['Key'],
                'file_size': obj['Size'],
                'etag': obj['ETag'].strip('"')  # S3 ETag can be used as content hash
            })
    return files

# Create DataFrame from S3 file list
df_s3_files = spark.createDataFrame(list_files_s3('my-bucket', 'data/'))

# Deduplicate by ETag (S3's content hash)
df_unique_files = df_s3_files.dropDuplicates(subset=['etag'])
```

## 3. Bloom Filters for Approximate Duplicate Detection

**Best for:** Memory-efficient duplicate detection when you can tolerate false positives.

> **Note:** See `bloom_filter_hyperloglog.py` for a complete working example with both Bloom Filters and HyperLogLog.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from pybloom_live import BloomFilter

# Create Bloom filter
bloom = BloomFilter(capacity=1000000, error_rate=0.001)

# For small datasets, you can broadcast the bloom filter
def check_bloom_filter(value):
    """Check if value exists in bloom filter."""
    return value in bloom

# For large datasets, use Spark's built-in bloom filter
from pyspark.sql.functions import bloom_filter

# Create bloom filter from existing data
existing_df = spark.read.parquet("existing_data.parquet")
existing_keys = existing_df.select('email').distinct().rdd.map(lambda r: r[0]).collect()

# Add to bloom filter
for key in existing_keys:
    bloom.add(key)

# Check new data against bloom filter
new_df = spark.read.parquet("new_data.parquet")

def is_duplicate_udf(email):
    return email in bloom if email else False

is_duplicate = F.udf(is_duplicate_udf, BooleanType())

# Filter out potential duplicates
df_unique = new_df.filter(~is_duplicate(F.col('email')))
```

**Usage:**
```bash
# Run the complete Bloom Filter and HyperLogLog demonstration
python bloom_filter_hyperloglog.py
```

## 4. HyperLogLog for Distinct Count Estimation

**Best for:** Estimating distinct counts without storing all values (useful for monitoring).

> **Note:** See `bloom_filter_hyperloglog.py` for a complete working example with performance comparisons.

```python
from pyspark.sql import functions as F

# Estimate distinct count using HyperLogLog
distinct_count = df.agg(
    F.approx_count_distinct('email', rsd=0.05).alias('approx_distinct_emails')
).collect()[0]['approx_distinct_emails']

# Compare with exact count
exact_count = df.select('email').distinct().count()

print(f"Approximate distinct: {distinct_count}")
print(f"Exact distinct: {exact_count}")
```

**Usage:**
```bash
# Run the complete Bloom Filter and HyperLogLog demonstration
python bloom_filter_hyperloglog.py
```

The script includes:
- HyperLogLog distinct count estimation with different RSD values
- Performance comparison between exact and approximate methods
- Memory usage analysis
- Practical examples for real-world scenarios

## 5. Delta Lake Built-in Deduplication

**Best for:** When using Delta Lake format, which provides ACID transactions and built-in deduplication.

```python
from delta.tables import DeltaTable

# Write data to Delta Lake
df.write.format("delta").mode("append").save("delta_table_path")

# Deduplicate using Delta Lake merge
delta_table = DeltaTable.forPath(spark, "delta_table_path")

# Merge new data, avoiding duplicates
new_df = spark.read.parquet("new_data.parquet")

delta_table.alias("target").merge(
    new_df.alias("source"),
    "target.email = source.email"
).whenNotMatchedInsertAll().execute()

# Or use Delta Lake's deduplicate function
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "delta_table_path")

# Deduplicate based on key columns
delta_table.toDF().dropDuplicates(['email', 'name']).write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("delta_table_path")
```

## 6. Partitioned Hash-Based Deduplication

**Best for:** Large-scale deduplication with efficient partitioning.

```python
from pyspark.sql import functions as F

# Create hash-based partition key
df_with_partition = df.withColumn(
    'partition_key',
    F.abs(F.hash(F.col('email'))) % 100  # Partition into 100 buckets
)

# Repartition by hash
df_partitioned = df_with_partition.repartition(100, 'partition_key')

# Deduplicate within each partition
df_unique = df_partitioned.dropDuplicates(subset=['email']).drop('partition_key')
```

## 7. Incremental Deduplication with Checksums

**Best for:** Streaming or incremental data processing.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib

def compute_checksum(*cols):
    """Compute checksum for incremental deduplication."""
    row_str = '|'.join([str(c) if c is not None else "" for c in cols])
    return hashlib.md5(row_str.encode()).hexdigest()

checksum_udf = F.udf(compute_checksum, StringType())

# Existing data with checksums
existing_df = spark.read.parquet("existing_data.parquet")
existing_checksums = existing_df.select('checksum').distinct().rdd.map(lambda r: r[0]).collect()
existing_checksums_set = set(existing_checksums)

# New data
new_df = spark.read.parquet("new_data.parquet")
new_df_with_checksum = new_df.withColumn(
    'checksum',
    checksum_udf(F.col('name'), F.col('email'), F.col('address'))
)

# Filter out records that already exist
def is_new_udf(checksum):
    return checksum not in existing_checksums_set if checksum else True

is_new = F.udf(is_new_udf, BooleanType())
df_new_only = new_df_with_checksum.filter(is_new(F.col('checksum')))

# Append new records
df_new_only.write.mode('append').parquet("existing_data.parquet")
```

## 8. Content-Based File Deduplication with Spark

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField
import hashlib

def hash_file_content_udf(content):
    """Hash file content."""
    if content:
        return hashlib.sha256(content.encode()).hexdigest()
    return None

hash_udf = F.udf(hash_file_content_udf, StringType())

# Read all files as binary/text
df_files = spark.read.format("binaryFile").load("path/to/files/*")

# Compute content hash
df_files_with_hash = df_files.withColumn(
    'content_hash', hash_udf(F.col('content'))
).withColumn(
    'file_name', F.input_file_name()
)

# Find duplicate files
duplicate_groups = df_files_with_hash.groupBy('content_hash').agg(
    F.collect_list('file_name').alias('duplicate_files'),
    F.count('*').alias('duplicate_count')
).filter(F.col('duplicate_count') > 1)

# Keep one file per content hash
window_spec = Window.partitionBy('content_hash').orderBy('file_name')
df_unique_files = df_files_with_hash.withColumn(
    'row_num', F.row_number().over(window_spec)
).filter(F.col('row_num') == 1).drop('row_num', 'content_hash')
```

## Performance Comparison

| Method | Use Case | Performance | Memory | Accuracy |
|--------|----------|------------|--------|----------|
| Content Hash (MD5) | Exact duplicates | Fast | Low | 100% |
| Content Hash (SHA-256) | Secure deduplication | Medium | Low | 100% |
| File-level Hash | Duplicate files | Medium | Low | 100% |
| Bloom Filter | Approximate detection | Very Fast | Very Low | ~99.9% |
| HyperLogLog | Distinct estimation | Very Fast | Very Low | ~95% |
| Delta Lake | ACID deduplication | Fast | Medium | 100% |
| Partitioned Hash | Large-scale | Fast | Medium | 100% |

## Best Practices

1. **For exact duplicates:** Use MD5/SHA-256 content hashing - fastest and most accurate
2. **For file deduplication:** Compute file content hashes and group by hash
3. **For streaming:** Use incremental checksum-based deduplication
4. **For approximate detection:** Use Bloom Filters when memory is constrained
5. **For Delta Lake users:** Leverage built-in deduplication features
6. **For large datasets:** Combine hashing with partitioning for optimal performance

## Security Considerations

- **MD5:** Fast but has known collision vulnerabilities (fine for deduplication, not for security)
- **SHA-256:** Cryptographically secure, recommended for sensitive data
- **File ETags:** S3 ETags can be used as content hashes for S3 files

