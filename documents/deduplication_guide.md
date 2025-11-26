# Apache Spark Best Practices for Handling Redundant Data

## Overview

Apache Spark provides several powerful techniques for handling redundant data, from simple exact duplicate removal to sophisticated fuzzy matching. This guide covers the best practices for deduplication in Spark.

## 1. Exact Duplicate Removal

### Method 1: `dropDuplicates()` (Recommended for Simple Cases)

**Best for:** Removing exact duplicates across all columns or specific columns.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Deduplication").getOrCreate()

# Read the redundant dataset
# Use /app/data/ in Docker, or current directory locally
import os
csv_path = "/app/data/redundant_data.csv" if os.path.exists("/app/data/redundant_data.csv") else "redundant_data.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Remove duplicates across all columns
df_unique = df.dropDuplicates()

# Remove duplicates based on specific columns (keep first occurrence)
df_unique = df.dropDuplicates(subset=['name', 'email'])

# Remove duplicates and keep the record with the latest ID
from pyspark.sql import functions as F
df_unique = df.dropDuplicates(subset=['name', 'email']).orderBy('id')
```

**Performance Tips:**
- Use `dropDuplicates()` on specific columns rather than all columns when possible
- Partition data appropriately before deduplication
- Cache intermediate results if reusing

### Method 2: Window Functions (For Keeping Specific Records)

**Best for:** When you need to keep a specific record from duplicates (e.g., latest, highest priority).

```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Define window specification
window_spec = Window.partitionBy('name', 'email').orderBy(F.desc('id'))

# Add row number and filter
df_unique = df.withColumn('row_num', F.row_number().over(window_spec)) \
              .filter(F.col('row_num') == 1) \
              .drop('row_num')
```

**When to use:**
- Need to keep the "best" record from duplicates
- Want to preserve metadata (e.g., keep record with most complete data)
- Need deterministic selection criteria

## 2. Fuzzy Duplicate Detection

### Method 1: Using Levenshtein Distance

**Best for:** Detecting similar but not identical records (typos, variations).

```python
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import Levenshtein

# Register UDF for Levenshtein distance
def levenshtein_udf(s1, s2):
    return Levenshtein.distance(s1 or "", s2 or "")

spark.udf.register("levenshtein_distance", levenshtein_udf, IntegerType())

# Self-join to find similar records
df_with_similarity = df.alias("a").join(
    df.alias("b"),
    (F.expr("levenshtein_distance(a.name, b.name) <= 2") |
     F.expr("levenshtein_distance(a.email, b.email) <= 2")),
    "inner"
).select(
    F.col("a.id").alias("id1"),
    F.col("a.name").alias("name1"),
    F.col("b.id").alias("id2"),
    F.col("b.name").alias("name2"),
    F.expr("levenshtein_distance(a.name, b.name)").alias("name_distance"),
    F.expr("levenshtein_distance(a.email, b.email)").alias("email_distance")
).filter(F.col("id1") != F.col("id2"))
```

**Performance Warning:** Self-joins can be expensive. Use with caution on large datasets.

### Method 2: Using FuzzyWuzzy (Better for String Similarity)

```python
from fuzzywuzzy import fuzz
from pyspark.sql.types import IntegerType

def fuzzy_ratio_udf(s1, s2):
    return fuzz.ratio(s1 or "", s2 or "")

spark.udf.register("fuzzy_ratio", fuzzy_ratio_udf, IntegerType())

# Find records with high similarity
df_similar = df.alias("a").join(
    df.alias("b"),
    F.expr("fuzzy_ratio(a.name, b.name) >= 85"),
    "inner"
).filter(F.col("a.id") != F.col("b.id"))
```

### Method 3: Locality-Sensitive Hashing (LSH) - Scalable Approach

**Best for:** Large-scale fuzzy deduplication (millions/billions of records).

```python
from pyspark.ml.feature import MinHashLSH, HashingTF, Tokenizer
from pyspark.ml import Pipeline

# Tokenize and hash the name field
tokenizer = Tokenizer(inputCol="name", outputCol="tokens")
hashingTF = HashingTF(inputCol="tokens", outputCol="features", numFeatures=1024)
minhash = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)

pipeline = Pipeline(stages=[tokenizer, hashingTF, minhash])
model = pipeline.fit(df)

# Transform data
df_hashed = model.transform(df)

# Find approximate duplicates
df_duplicates = model.stages[-1].approxSimilarityJoin(
    df_hashed, df_hashed, threshold=0.3, distCol="distance"
).filter(F.col("datasetA.id") != F.col("datasetB.id"))
```

**Advantages:**
- Scales to very large datasets
- Much faster than pairwise comparisons
- Built into Spark MLlib

## 3. Partitioning Strategies for Deduplication

### Strategy 1: Partition by Key Columns

```python
# Partition by columns used for deduplication
df_partitioned = df.repartition("name", "email")

# Then deduplicate within partitions
df_unique = df_partitioned.dropDuplicates(subset=['name', 'email'])
```

### Strategy 2: Bucketing (For Repeated Deduplication)

```python
# Write bucketed data (if writing to Hive/Delta)
df.write \
  .bucketBy(100, "name", "email") \
  .sortBy("id") \
  .saveAsTable("bucketed_data")

# Read and deduplicate
df = spark.table("bucketed_data")
df_unique = df.dropDuplicates(subset=['name', 'email'])
```

## 4. Performance Optimization Techniques

### 1. Broadcast Join for Small Reference Data

```python
# If you have a small list of known duplicates
known_duplicates = spark.createDataFrame([...], schema)
df_unique = df.join(
    F.broadcast(known_duplicates),
    on="id",
    how="left_anti"  # Keep only records NOT in duplicates
)
```

### 2. Caching Intermediate Results

```python
# Cache if you'll reuse the deduplicated data
df_unique = df.dropDuplicates()
df_unique.cache()
df_unique.count()  # Trigger caching

# Use cached data for multiple operations
df_unique.write.parquet("deduplicated_data.parquet")
```

### 3. Incremental Deduplication

```python
# For streaming or incremental updates
from pyspark.sql import functions as F

# Union new data with existing
df_all = existing_df.union(new_df)

# Deduplicate and keep latest
window_spec = Window.partitionBy('name', 'email').orderBy(F.desc('timestamp'))
df_latest = df_all.withColumn('row_num', F.row_number().over(window_spec)) \
                  .filter(F.col('row_num') == 1) \
                  .drop('row_num')
```

## 5. Complete Deduplication Pipeline Example

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import MinHashLSH, HashingTF, Tokenizer
from pyspark.ml import Pipeline

def deduplicate_data(spark, input_path, output_path, method='exact'):
    """
    Comprehensive deduplication pipeline.
    
    Args:
        spark: SparkSession
        input_path: Path to input CSV
        output_path: Path to output parquet
        method: 'exact', 'fuzzy', or 'lsh'
    """
    # Read data
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    if method == 'exact':
        # Step 1: Remove exact duplicates
        df_step1 = df.dropDuplicates(subset=['name', 'email'])
        
        # Step 2: Keep record with most complete data
        window_spec = Window.partitionBy('name', 'email').orderBy(
            F.desc(F.length('address')),
            F.desc('id')
        )
        df_unique = df_step1.withColumn('row_num', F.row_number().over(window_spec)) \
                            .filter(F.col('row_num') == 1) \
                            .drop('row_num')
    
    elif method == 'fuzzy':
        # Use LSH for fuzzy matching
        tokenizer = Tokenizer(inputCol="name", outputCol="tokens")
        hashingTF = HashingTF(inputCol="tokens", outputCol="features", numFeatures=1024)
        minhash = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
        
        pipeline = Pipeline(stages=[tokenizer, hashingTF, minhash])
        model = pipeline.fit(df)
        df_hashed = model.transform(df)
        
        # Find and merge similar records
        similar_pairs = model.stages[-1].approxSimilarityJoin(
            df_hashed, df_hashed, threshold=0.3, distCol="distance"
        ).filter(F.col("datasetA.id") != F.col("datasetB.id"))
        
        # Keep one record from each similar pair (implementation depends on business logic)
        df_unique = df_hashed.dropDuplicates(subset=['name', 'email'])
    
# Write results
# Use /app/data/ in Docker, or current directory locally
output_path = '/app/data/deduplicated_data.parquet' if os.path.exists('/app/data') else 'deduplicated_data.parquet'
df_unique.write.mode('overwrite').parquet(output_path)
    
    print(f"Original records: {df.count()}")
    print(f"Unique records: {df_unique.count()}")
    print(f"Duplicates removed: {df.count() - df_unique.count()}")
    
    return df_unique

# Usage
spark = SparkSession.builder.appName("DeduplicationPipeline").getOrCreate()
df_unique = deduplicate_data(
    spark, 
    "redundant_data.csv", 
    "deduplicated_data.parquet",
    method='exact'
)
```

## 6. Best Practices Summary

### ✅ DO:
- Use `dropDuplicates()` for exact duplicates (simplest and fastest)
- Partition data by key columns before deduplication
- Use window functions when you need to keep specific records
- Use LSH for large-scale fuzzy matching
- Cache intermediate results if reused
- Write deduplicated data in efficient formats (Parquet, Delta)

### ❌ DON'T:
- Don't use self-joins for fuzzy matching on large datasets
- Don't deduplicate without partitioning on large datasets
- Don't forget to handle NULL values in key columns
- Don't deduplicate without understanding your data distribution
- Don't use expensive UDFs without optimization

## 7. Handling Edge Cases

### NULL Values
```python
# Replace NULLs before deduplication
df_clean = df.fillna({
    'name': '',
    'email': '',
    'address': ''
})

df_unique = df_clean.dropDuplicates(subset=['name', 'email'])
```

### Case Sensitivity
```python
# Normalize case before deduplication
df_normalized = df.withColumn('name_lower', F.lower(F.col('name'))) \
                  .withColumn('email_lower', F.lower(F.col('email')))

df_unique = df_normalized.dropDuplicates(subset=['name_lower', 'email_lower'])
```

### Whitespace
```python
# Trim whitespace
df_trimmed = df.withColumn('name', F.trim(F.col('name'))) \
               .withColumn('email', F.trim(F.col('email')))

df_unique = df_trimmed.dropDuplicates(subset=['name', 'email'])
```

## 8. Monitoring and Validation

```python
# Validate deduplication results
def validate_deduplication(original_df, deduplicated_df, key_columns):
    original_count = original_df.count()
    unique_count = deduplicated_df.count()
    duplicates_removed = original_count - unique_count
    
    # Check for remaining duplicates
    remaining_duplicates = deduplicated_df.groupBy(key_columns).count() \
                                          .filter(F.col('count') > 1)
    
    print(f"Original records: {original_count}")
    print(f"Unique records: {unique_count}")
    print(f"Duplicates removed: {duplicates_removed}")
    print(f"Remaining duplicate groups: {remaining_duplicates.count()}")
    
    return {
        'original_count': original_count,
        'unique_count': unique_count,
        'duplicates_removed': duplicates_removed,
        'deduplication_rate': duplicates_removed / original_count * 100
    }
```

