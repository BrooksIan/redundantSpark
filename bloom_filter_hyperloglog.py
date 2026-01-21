"""
Apache Spark script demonstrating Bloom Filters and HyperLogLog for approximate duplicate detection.

Bloom Filters: Memory-efficient probabilistic data structure for approximate duplicate detection
HyperLogLog: Algorithm for estimating distinct counts with minimal memory

Both are useful when:
- Memory is constrained
- Exact accuracy is not required
- Fast approximate results are acceptable
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import sys
import os


def create_spark_session(app_name="BloomFilterHyperLogLog", master_url=None):
    """
    Create and configure Spark session.
    
    Supports multiple environments:
    - Cloudera AI Workbench: Uses existing SparkSession if available, or creates with minimal config
    - Docker/Standalone: Configures for standalone Spark cluster
    - Local: Uses local mode
    """
    import os
    
    # Check if running in Cloudera AI Workbench
    is_cloudera = (
        os.getenv("CDSW_PROJECT_ID") is not None or
        os.getenv("CDSW_APP_ID") is not None or
        os.getenv("CLOUDERA_AI_WORKBENCH") is not None or
        os.path.exists("/var/lib/cdsw") or
        os.path.exists("/home/cdsw")
    )
    
    # In Cloudera AI Workbench, try to use existing SparkSession first
    if is_cloudera:
        try:
            from pyspark.sql import SparkSession
            existing_spark = SparkSession.getActiveSession()
            if existing_spark is not None:
                print(f"Using existing SparkSession in Cloudera AI Workbench")
                return existing_spark
        except:
            pass
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Determine master URL
    master = master_url or os.getenv("SPARK_MASTER")
    
    # Cloudera AI Workbench: Don't set master, let platform manage it
    if is_cloudera:
        pass  # Platform manages configuration
    elif not master and os.path.exists("/opt/spark"):  # Running in Spark Docker container
        master = "spark://spark-master:7077"
    
    # Connect to Spark master if specified (not in Cloudera)
    if master and not is_cloudera:
        builder = builder.master(master)
        # Add authentication secret if connecting to a standalone cluster
        if master.startswith("spark://"):
            auth_secret = os.getenv("SPARK_AUTHENTICATE_SECRET", "spark-secret-key")
            builder = builder.config("spark.authenticate.secret", auth_secret)
            builder = builder.config("spark.authenticate", "true")
    
    return builder.getOrCreate()


def bloom_filter_deduplication_exact(spark, existing_df, new_df, key_column='email', error_rate=0.01):
    """
    Use Bloom Filter for approximate duplicate detection (exact implementation).
    
    This method collects existing keys into a Bloom Filter and checks new data against it.
    Best for: Small to medium existing datasets, large new datasets.
    
    Args:
        spark: SparkSession
        existing_df: DataFrame with existing data
        new_df: DataFrame with new data to check
        key_column: Column to use for duplicate detection
        error_rate: False positive rate (0.01 = 1% false positives)
    
    Returns:
        DataFrame with potential duplicates filtered out
    """
    try:
        from pybloom_live import BloomFilter
        
        print(f"\n{'='*70}")
        print("BLOOM FILTER DEDUPLICATION (Exact Implementation)")
        print(f"{'='*70}")
        
        # Get distinct keys from existing data
        print(f"Collecting distinct {key_column} values from existing data...")
        existing_keys = existing_df.select(key_column).distinct().rdd.map(lambda r: r[0]).collect()
        num_existing = len(existing_keys)
        
        print(f"Found {num_existing:,} distinct {key_column} values")
        
        # Create Bloom Filter with appropriate capacity
        # Capacity should be larger than expected number of unique values
        capacity = int(num_existing * 1.2)  # 20% buffer
        print(f"Creating Bloom Filter with capacity: {capacity:,}, error rate: {error_rate}")
        
        bloom = BloomFilter(capacity=capacity, error_rate=error_rate)
        
        # Add existing keys to Bloom Filter
        print("Adding existing keys to Bloom Filter...")
        for key in existing_keys:
            if key is not None:
                bloom.add(str(key))
        
        print(f"Bloom Filter created. Size: ~{bloom.num_bits_m / 8 / 1024:.2f} KB")
        
        # Broadcast Bloom Filter (for small filters)
        if bloom.num_bits_m < 10 * 1024 * 1024:  # Less than 10MB
            print("Broadcasting Bloom Filter to all nodes...")
            bloom_broadcast = spark.sparkContext.broadcast(bloom)
            
            def check_bloom_filter_udf(value):
                """Check if value exists in Bloom Filter."""
                if value is None:
                    return False
                return str(value) in bloom_broadcast.value
            
            check_udf = F.udf(check_bloom_filter_udf, BooleanType())
            
            # Filter new data
            print(f"Checking {new_df.count():,} new records against Bloom Filter...")
            df_unique = new_df.filter(~check_udf(F.col(key_column)))
            
            # Count results
            new_count = new_df.count()
            unique_count = df_unique.count()
            potential_duplicates = new_count - unique_count
            
            print(f"\nResults:")
            print(f"  New records:           {new_count:,}")
            print(f"  Potential duplicates:  {potential_duplicates:,}")
            print(f"  Unique records:       {unique_count:,}")
            print(f"  Note: ~{error_rate*100:.2f}% false positive rate possible")
            
            return df_unique
        else:
            print("Bloom Filter too large to broadcast. Consider using Spark's built-in bloom filter.")
            return None
            
    except ImportError:
        print("Warning: pybloom_live not installed.")
        print("Install with: pip install pybloom-live")
        print("\nFalling back to Spark's built-in approximate methods...")
        return None
    except Exception as e:
        print(f"Error in Bloom Filter deduplication: {e}")
        import traceback
        traceback.print_exc()
        return None


def bloom_filter_deduplication_spark(spark, existing_df, new_df, key_column='email', 
                                     expected_items=None, fpp=0.01):
    """
    Use Spark's built-in Bloom Filter for approximate duplicate detection.
    
    This uses Spark's native bloom filter which is more scalable.
    Best for: Large datasets where exact Bloom Filter is too large.
    
    Args:
        spark: SparkSession
        existing_df: DataFrame with existing data
        new_df: DataFrame with new data to check
        key_column: Column to use for duplicate detection
        expected_items: Expected number of items (if None, calculated from existing data)
        fpp: False positive probability (default 0.01 = 1%)
    
    Returns:
        DataFrame with potential duplicates filtered out
    """
    print(f"\n{'='*70}")
    print("BLOOM FILTER DEDUPLICATION (Spark Built-in)")
    print(f"{'='*70}")
    
    # Calculate expected items if not provided
    if expected_items is None:
        print(f"Calculating expected number of distinct {key_column} values...")
        expected_items = existing_df.select(key_column).distinct().count()
    
    print(f"Expected distinct items: {expected_items:,}")
    print(f"False positive probability: {fpp}")
    
    # Create Bloom Filter from existing data
    print(f"Creating Bloom Filter from existing data...")
    existing_keys = existing_df.select(key_column).distinct()
    
    # Use Spark's bloom filter (available in Spark 3.0+)
    try:
        # For Spark 3.0+, we can use bloom filter aggregation
        # Create a bloom filter using aggregate function
        from pyspark.sql.functions import bloom_filter
        
        # Create bloom filter
        bloom_df = existing_keys.agg(
            bloom_filter(key_column, expected_items, fpp).alias('bloom_filter')
        )
        
        # Get the bloom filter (this is a bit array)
        bloom_result = bloom_df.collect()[0]['bloom_filter']
        
        print(f"Bloom Filter created successfully")
        
        # For checking, we'd need to use the bloom filter in a join or UDF
        # Since Spark's bloom_filter is primarily for aggregation, we'll use a different approach
        # Check if keys exist using a left anti join (more efficient)
        print(f"Using left anti join for duplicate detection...")
        df_unique = new_df.join(
            existing_keys.alias('existing'),
            new_df[key_column] == F.col(f'existing.{key_column}'),
            'left_anti'
        )
        
        new_count = new_df.count()
        unique_count = df_unique.count()
        potential_duplicates = new_count - unique_count
        
        print(f"\nResults:")
        print(f"  New records:           {new_count:,}")
        print(f"  Duplicates found:      {potential_duplicates:,}")
        print(f"  Unique records:        {unique_count:,}")
        
        return df_unique
        
    except Exception as e:
        print(f"Error: {e}")
        print("Using alternative method: left anti join")
        # Fallback to left anti join
        existing_keys = existing_df.select(key_column).distinct()
        df_unique = new_df.join(
            existing_keys.alias('existing'),
            new_df[key_column] == F.col(f'existing.{key_column}'),
            'left_anti'
        )
        return df_unique


def hyperloglog_distinct_count(df, column='email', rsd=0.05):
    """
    Estimate distinct count using HyperLogLog algorithm.
    
    HyperLogLog provides approximate distinct counts with minimal memory.
    Best for: Monitoring, dashboards, when exact count is not critical.
    
    Args:
        df: Input DataFrame
        column: Column to count distinct values for
        rsd: Relative standard deviation (0.05 = 5% error, lower = more accurate but more memory)
    
    Returns:
        Dictionary with approximate and exact counts
    """
    print(f"\n{'='*70}")
    print("HYPERLOGLOG DISTINCT COUNT ESTIMATION")
    print(f"{'='*70}")
    
    print(f"Estimating distinct count for column: {column}")
    print(f"Relative standard deviation: {rsd} ({rsd*100}% error)")
    
    # Approximate distinct count using HyperLogLog
    print("Calculating approximate distinct count...")
    approx_result = df.agg(
        F.approx_count_distinct(column, rsd=rsd).alias('approx_distinct')
    ).collect()[0]
    
    approx_count = approx_result['approx_distinct']
    
    # Exact distinct count for comparison
    print("Calculating exact distinct count (for comparison)...")
    exact_count = df.select(column).distinct().count()
    
    # Calculate error
    error = abs(approx_count - exact_count)
    error_percent = (error / exact_count * 100) if exact_count > 0 else 0
    
    print(f"\nResults:")
    print(f"  Approximate distinct:    {approx_count:,}")
    print(f"  Exact distinct:          {exact_count:,}")
    print(f"  Error:                   {error:,} ({error_percent:.2f}%)")
    print(f"  Memory saved:            Significant (HyperLogLog uses ~1.5KB per counter)")
    
    return {
        'approx_distinct': approx_count,
        'exact_distinct': exact_count,
        'error': error,
        'error_percent': error_percent,
        'rsd': rsd
    }


def compare_methods(spark, df, key_column='email'):
    """
    Compare different methods for duplicate detection and distinct counting.
    
    Args:
        spark: SparkSession
        df: Input DataFrame
        key_column: Column to analyze
    """
    print(f"\n{'='*70}")
    print("METHOD COMPARISON")
    print(f"{'='*70}")
    
    total_count = df.count()
    print(f"Total records: {total_count:,}")
    print(f"Analyzing column: {key_column}\n")
    
    # Method 1: Exact distinct count
    print("1. Exact distinct count (baseline):")
    import time
    start = time.time()
    exact_count = df.select(key_column).distinct().count()
    exact_time = time.time() - start
    print(f"   Result: {exact_count:,} distinct values")
    print(f"   Time: {exact_time:.2f} seconds")
    print(f"   Memory: High (requires full distinct operation)\n")
    
    # Method 2: HyperLogLog with different RSD values
    print("2. HyperLogLog (approximate):")
    rsd_values = [0.01, 0.05, 0.1]
    for rsd in rsd_values:
        start = time.time()
        result = hyperloglog_distinct_count(df, key_column, rsd=rsd)
        hll_time = time.time() - start
        print(f"   RSD={rsd}: {result['approx_distinct']:,} (error: {result['error_percent']:.2f}%)")
        print(f"   Time: {hll_time:.2f} seconds")
        print(f"   Memory: Very Low (~1.5KB per counter)\n")
    
    # Method 3: Approximate count distinct (Spark's default)
    print("3. Spark's approx_count_distinct (default RSD=0.05):")
    start = time.time()
    approx_default = df.agg(F.approx_count_distinct(key_column)).collect()[0][0]
    approx_time = time.time() - start
    error_default = abs(approx_default - exact_count) / exact_count * 100
    print(f"   Result: {approx_default:,} distinct values")
    print(f"   Error: {error_default:.2f}%")
    print(f"   Time: {approx_time:.2f} seconds\n")
    
    # Summary
    print("="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Exact method:     {exact_count:,} distinct ({exact_time:.2f}s)")
    print(f"HyperLogLog:      ~{approx_default:,} distinct ({approx_time:.2f}s, {error_default:.2f}% error)")
    print(f"\nHyperLogLog is {exact_time/approx_time:.1f}x faster with minimal memory usage")
    print("="*70)


def incremental_deduplication_example(spark, existing_path, new_path, key_column='email'):
    """
    Example of incremental deduplication using Bloom Filter.
    
    This demonstrates a common use case: checking new data against existing data.
    
    Args:
        spark: SparkSession
        existing_path: Path to existing data (CSV or Parquet)
        new_path: Path to new data (CSV or Parquet)
        key_column: Column to use for duplicate detection
    """
    print(f"\n{'='*70}")
    print("INCREMENTAL DEDUPLICATION EXAMPLE")
    print(f"{'='*70}")
    
    # Read data
    print(f"Reading existing data from: {existing_path}")
    if existing_path.endswith('.csv'):
        existing_df = spark.read.csv(existing_path, header=True, inferSchema=True)
    else:
        existing_df = spark.read.parquet(existing_path)
    
    print(f"Reading new data from: {new_path}")
    if new_path.endswith('.csv'):
        new_df = spark.read.csv(new_path, header=True, inferSchema=True)
    else:
        new_df = spark.read.parquet(new_path)
    
    existing_count = existing_df.count()
    new_count = new_df.count()
    
    print(f"\nExisting records: {existing_count:,}")
    print(f"New records: {new_count:,}")
    
    # Try Bloom Filter method
    print("\n" + "-"*70)
    print("Using Bloom Filter for duplicate detection...")
    df_unique_bloom = bloom_filter_deduplication_spark(
        spark, existing_df, new_df, key_column=key_column
    )
    
    if df_unique_bloom:
        print("\n" + "-"*70)
        print("Sample of unique records (first 5):")
        df_unique_bloom.show(5, truncate=False)


def main():
    """Main function to run Bloom Filter and HyperLogLog examples."""
    import glob
    
    spark = create_spark_session("BloomFilterHyperLogLogDemo")
    
    # Find data files
    data_dir = "data"
    csv_files = []
    
    if os.path.exists(data_dir):
        csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
        csv_files = [f for f in csv_files if not os.path.basename(f).startswith("deduplicated_")]
    
    if not csv_files:
        print("No CSV files found in data/ directory!")
        print("Please generate data first: python generate_dataset.py 1000 data/test_data.csv")
        spark.stop()
        return
    
    # Use first available CSV file
    input_file = csv_files[0]
    print(f"Using data file: {input_file}")
    
    # Read data
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    total_count = df.count()
    print(f"Loaded {total_count:,} records")
    
    # Example 1: HyperLogLog distinct count estimation
    print("\n" + "="*70)
    print("EXAMPLE 1: HyperLogLog Distinct Count Estimation")
    print("="*70)
    hyperloglog_distinct_count(df, column='email', rsd=0.05)
    
    # Example 2: Compare methods
    print("\n" + "="*70)
    print("EXAMPLE 2: Method Comparison")
    print("="*70)
    compare_methods(spark, df, key_column='email')
    
    # Example 3: Incremental deduplication (if we have multiple files)
    if len(csv_files) > 1:
        print("\n" + "="*70)
        print("EXAMPLE 3: Incremental Deduplication with Bloom Filter")
        print("="*70)
        existing_file = csv_files[0]
        new_file = csv_files[1] if len(csv_files) > 1 else csv_files[0]
        incremental_deduplication_example(spark, existing_file, new_file, key_column='email')
    else:
        # Split data for demonstration
        print("\n" + "="*70)
        print("EXAMPLE 3: Incremental Deduplication with Bloom Filter")
        print("="*70)
        print("Splitting data into 'existing' and 'new' for demonstration...")
        
        # Split data 70/30
        existing_df, new_df = df.randomSplit([0.7, 0.3], seed=42)
        existing_count = existing_df.count()
        new_count = new_df.count()
        
        print(f"Existing records: {existing_count:,}")
        print(f"New records: {new_count:,}")
        
        # Use Bloom Filter
        df_unique = bloom_filter_deduplication_spark(
            spark, existing_df, new_df, key_column='email'
        )
        
        if df_unique:
            print("\nSample of unique records (first 5):")
            df_unique.show(5, truncate=False)
    
    print("\n" + "="*70)
    print("DEMONSTRATION COMPLETE")
    print("="*70)
    print("\nKey Takeaways:")
    print("  - HyperLogLog: Fast approximate distinct counts with minimal memory")
    print("  - Bloom Filter: Memory-efficient duplicate detection (allows false positives)")
    print("  - Use approximate methods when exact accuracy is not critical")
    print("  - Both methods scale well to very large datasets")
    print("="*70 + "\n")
    
    spark.stop()


if __name__ == "__main__":
    main()

