"""
Apache Spark script demonstrating Bloom Filters and HyperLogLog for file-level deduplication.

This script shows how to use approximate methods to:
1. Estimate the number of unique files (HyperLogLog)
2. Quickly check if a file is a duplicate before computing full hash (Bloom Filter)
3. Efficiently deduplicate large file collections
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType
import sys
import os
import glob
import hashlib


def create_spark_session(app_name="BloomFilterFileDeduplication", master_url=None):
    """Create and configure Spark session."""
    import os
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Connect to Spark master if running in Docker or master_url is provided
    if master_url:
        builder = builder.master(master_url)
    elif os.getenv("SPARK_MASTER"):
        builder = builder.master(os.getenv("SPARK_MASTER"))
    elif os.path.exists("/opt/spark"):  # Running in Spark Docker container
        builder = builder.master("spark://spark-master:7077")
    
    return builder.getOrCreate()


def compute_file_hash(file_path, hash_type='md5'):
    """
    Compute hash of file content.
    
    Args:
        file_path: Path to file
        hash_type: 'md5' or 'sha256'
    
    Returns:
        Hex digest of file hash
    """
    if hash_type == 'md5':
        hasher = hashlib.md5()
    elif hash_type == 'sha256':
        hasher = hashlib.sha256()
    else:
        raise ValueError(f"Unsupported hash type: {hash_type}")
    
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception as e:
        print(f"Error hashing file {file_path}: {e}")
        return None


def hyperloglog_file_count(spark, file_paths, hash_type='md5'):
    """
    Estimate the number of unique files using HyperLogLog.
    
    This is useful when you have many files and want a quick estimate
    without computing all hashes.
    
    Args:
        spark: SparkSession
        file_paths: List of file paths
        hash_type: Hash algorithm to use
    
    Returns:
        Dictionary with approximate and exact counts
    """
    print(f"\n{'='*70}")
    print("HYPERLOGLOG FILE COUNT ESTIMATION")
    print(f"{'='*70}")
    
    # Create DataFrame with file paths
    file_data = []
    for file_path in file_paths:
        if os.path.exists(file_path) and os.path.isfile(file_path):
            file_data.append({
                'file_path': file_path,
                'file_name': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path)
            })
    
    if not file_data:
        print("No valid files found!")
        return None
    
    df_files = spark.createDataFrame(file_data)
    total_files = len(file_data)
    
    print(f"Total files to analyze: {total_files:,}")
    print(f"Computing file hashes (this may take a while for large files)...")
    
    # Compute hashes (this is the expensive part)
    def hash_file_udf(file_path):
        return compute_file_hash(file_path, hash_type)
    
    hash_udf = F.udf(hash_file_udf, StringType())
    df_with_hash = df_files.withColumn('file_hash', hash_udf(F.col('file_path')))
    
    # Approximate distinct count using HyperLogLog
    print("Calculating approximate distinct file count...")
    approx_result = df_with_hash.agg(
        F.approx_count_distinct('file_hash', rsd=0.05).alias('approx_unique_files')
    ).collect()[0]
    
    approx_count = approx_result['approx_unique_files']
    
    # Exact distinct count for comparison
    print("Calculating exact distinct file count...")
    exact_count = df_with_hash.select('file_hash').distinct().count()
    
    # Calculate error
    error = abs(approx_count - exact_count)
    error_percent = (error / exact_count * 100) if exact_count > 0 else 0
    
    print(f"\nResults:")
    print(f"  Total files:              {total_files:,}")
    print(f"  Approximate unique files:  {approx_count:,}")
    print(f"  Exact unique files:        {exact_count:,}")
    print(f"  Duplicate files:           {total_files - exact_count:,}")
    print(f"  Error:                     {error:,} ({error_percent:.2f}%)")
    print(f"  Memory saved:              Significant (HyperLogLog uses ~1.5KB)")
    
    return {
        'total_files': total_files,
        'approx_unique': approx_count,
        'exact_unique': exact_count,
        'duplicates': total_files - exact_count,
        'error': error,
        'error_percent': error_percent
    }


def bloom_filter_file_deduplication(spark, existing_files, new_files, hash_type='md5'):
    """
    Use Bloom Filter approach for file deduplication.
    
    This method is efficient when you have a large set of existing files
    and want to quickly check if new files are duplicates.
    
    Args:
        spark: SparkSession
        existing_files: List of existing file paths
        new_files: List of new file paths to check
        hash_type: Hash algorithm to use
    
    Returns:
        DataFrame with unique new files (duplicates filtered out)
    """
    print(f"\n{'='*70}")
    print("BLOOM FILTER FILE DEDUPLICATION")
    print(f"{'='*70}")
    
    # Compute hashes for existing files
    print(f"Computing hashes for {len(existing_files):,} existing files...")
    existing_hashes = set()
    for file_path in existing_files:
        if os.path.exists(file_path) and os.path.isfile(file_path):
            file_hash = compute_file_hash(file_path, hash_type)
            if file_hash:
                existing_hashes.add(file_hash)
    
    print(f"Found {len(existing_hashes):,} unique existing file hashes")
    
    # For large sets, we'd use a Bloom Filter here
    # For now, we'll use a broadcast set (works for moderate sizes)
    if len(existing_hashes) > 100000:
        print("Warning: Large number of existing files. Consider using Bloom Filter library.")
    
    # Broadcast existing hashes
    existing_hashes_broadcast = spark.sparkContext.broadcast(existing_hashes)
    
    # Process new files
    print(f"Checking {len(new_files):,} new files against existing files...")
    
    new_file_data = []
    for file_path in new_files:
        if os.path.exists(file_path) and os.path.isfile(file_path):
            file_hash = compute_file_hash(file_path, hash_type)
            if file_hash:
                new_file_data.append({
                    'file_path': file_path,
                    'file_name': os.path.basename(file_path),
                    'file_size': os.path.getsize(file_path),
                    'file_hash': file_hash
                })
    
    if not new_file_data:
        print("No valid new files found!")
        return None
    
    df_new_files = spark.createDataFrame(new_file_data)
    
    # Check if hash exists in existing files
    def is_duplicate_udf(file_hash):
        if file_hash is None:
            return False
        return file_hash in existing_hashes_broadcast.value
    
    is_duplicate = F.udf(is_duplicate_udf, BooleanType())
    df_new_files = df_new_files.withColumn('is_duplicate', is_duplicate(F.col('file_hash')))
    
    # Filter out duplicates
    df_unique_new = df_new_files.filter(~F.col('is_duplicate'))
    
    total_new = df_new_files.count()
    unique_new = df_unique_new.count()
    duplicates = total_new - unique_new
    
    print(f"\nResults:")
    print(f"  New files checked:         {total_new:,}")
    print(f"  Duplicate files found:     {duplicates:,}")
    print(f"  Unique new files:          {unique_new:,}")
    
    return df_unique_new.select('file_path', 'file_name', 'file_size', 'file_hash')


def exact_file_deduplication(spark, file_paths, hash_type='md5', output_dir="data"):
    """
    Exact file deduplication using full hash computation.
    
    This is the baseline method that computes all hashes and finds exact duplicates.
    
    Args:
        spark: SparkSession
        file_paths: List of file paths
        hash_type: Hash algorithm to use
        output_dir: Directory to save results
    
    Returns:
        Tuple of (unique_files_df, duplicate_groups_df)
    """
    print(f"\n{'='*70}")
    print("EXACT FILE DEDUPLICATION")
    print(f"{'='*70}")
    
    print(f"Processing {len(file_paths):,} files...")
    print("Computing file hashes (this may take a while)...")
    
    file_data = []
    for file_path in file_paths:
        if os.path.exists(file_path) and os.path.isfile(file_path):
            try:
                file_hash = compute_file_hash(file_path, hash_type)
                if file_hash:
                    file_data.append({
                        'file_path': file_path,
                        'file_name': os.path.basename(file_path),
                        'file_size': os.path.getsize(file_path),
                        'file_hash': file_hash
                    })
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                continue
    
    if not file_data:
        print("No valid files found!")
        return None, None
    
    df_files = spark.createDataFrame(file_data)
    
    # Group by hash to find duplicates
    duplicate_groups = df_files.groupBy('file_hash').agg(
        F.collect_list('file_path').alias('duplicate_paths'),
        F.collect_list('file_name').alias('duplicate_names'),
        F.count('*').alias('duplicate_count'),
        F.sum('file_size').alias('total_size')
    ).filter(F.col('duplicate_count') > 1)
    
    # Keep one file per hash (keep first alphabetically)
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy('file_hash').orderBy('file_path')
    df_unique_files = df_files.withColumn('row_num', F.row_number().over(window_spec)) \
                              .filter(F.col('row_num') == 1) \
                              .drop('row_num')
    
    total_files = df_files.count()
    unique_count = df_unique_files.count()
    duplicate_count = duplicate_groups.count()
    duplicates_removed = total_files - unique_count
    
    print(f"\nResults:")
    print(f"  Total files:               {total_files:,}")
    print(f"  Unique files:               {unique_count:,}")
    print(f"  Duplicate groups:          {duplicate_count:,}")
    print(f"  Duplicate files removed:    {duplicates_removed:,}")
    
    # Calculate space savings
    if duplicate_count > 0:
        duplicate_stats = duplicate_groups.agg(
            F.sum('total_size').alias('total_duplicate_size'),
            F.sum(F.col('duplicate_count') - 1).alias('total_duplicate_files')
        ).collect()[0]
        
        total_duplicate_size = duplicate_stats['total_duplicate_size']
        total_duplicate_files = duplicate_stats['total_duplicate_files']
        
        # Calculate space that could be saved (keep one copy, remove rest)
        space_savings = total_duplicate_size - (total_duplicate_size / (duplicate_count + 1))
        
        print(f"  Total duplicate size:      {total_duplicate_size:,} bytes")
        print(f"  Potential space savings:    {space_savings:,.0f} bytes ({space_savings/1024/1024:.2f} MB)")
    
    # Save results
    if output_dir:
        output_path = os.path.join(output_dir, "file_deduplication_results.parquet")
        print(f"\nSaving results to: {output_path}")
        df_unique_files.write.mode('overwrite').parquet(output_path)
    
    return df_unique_files, duplicate_groups


def compare_file_deduplication_methods(spark, file_paths):
    """
    Compare different methods for file deduplication.
    
    Args:
        spark: SparkSession
        file_paths: List of file paths
    """
    print(f"\n{'='*70}")
    print("FILE DEDUPLICATION METHOD COMPARISON")
    print(f"{'='*70}")
    
    import time
    
    # Method 1: HyperLogLog (approximate count)
    print("\n1. HyperLogLog (approximate unique file count):")
    start = time.time()
    hll_result = hyperloglog_file_count(spark, file_paths)
    hll_time = time.time() - start
    if hll_result:
        print(f"   Time: {hll_time:.2f} seconds")
        print(f"   Memory: Very Low")
    
    # Method 2: Exact deduplication
    print("\n2. Exact File Deduplication (baseline):")
    start = time.time()
    unique_files, duplicate_groups = exact_file_deduplication(spark, file_paths)
    exact_time = time.time() - start
    if unique_files:
        exact_count = unique_files.count()
        print(f"   Unique files: {exact_count:,}")
        print(f"   Time: {exact_time:.2f} seconds")
        print(f"   Memory: High (requires all hashes)")
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    if hll_result and unique_files:
        print(f"HyperLogLog: ~{hll_result['approx_unique']:,} unique files ({hll_time:.2f}s)")
        print(f"Exact method: {exact_count:,} unique files ({exact_time:.2f}s)")
        if exact_time > 0:
            print(f"\nHyperLogLog is {exact_time/hll_time:.1f}x faster for estimation")
    print("="*70)


def main():
    """Main function to run file deduplication examples."""
    spark = create_spark_session("BloomFilterFileDeduplication")
    
    # Find files in duplicatefiles directory
    duplicate_files_dir = "data/duplicatefiles"
    
    if not os.path.exists(duplicate_files_dir):
        print(f"Directory {duplicate_files_dir} does not exist!")
        print("Please run: python generate_duplicate_files.py 25 0.9")
        spark.stop()
        return
    
    # Get all files
    file_paths = glob.glob(os.path.join(duplicate_files_dir, "*"))
    file_paths = [f for f in file_paths if os.path.isfile(f)]
    
    if not file_paths:
        print(f"No files found in {duplicate_files_dir}!")
        spark.stop()
        return
    
    print(f"Found {len(file_paths)} files to analyze")
    
    # Example 1: HyperLogLog file count estimation
    print("\n" + "="*70)
    print("EXAMPLE 1: HyperLogLog File Count Estimation")
    print("="*70)
    hyperloglog_file_count(spark, file_paths)
    
    # Example 2: Exact file deduplication
    print("\n" + "="*70)
    print("EXAMPLE 2: Exact File Deduplication")
    print("="*70)
    unique_files, duplicate_groups = exact_file_deduplication(spark, file_paths)
    
    if unique_files and duplicate_groups:
        print("\nSample duplicate groups:")
        duplicate_groups.select(
            'file_hash',
            'duplicate_names',
            'duplicate_count',
            F.round('total_size', 2).alias('total_size_bytes')
        ).show(5, truncate=False)
        
        print("\nSample unique files:")
        unique_files.select('file_name', 'file_size', 'file_hash').show(5, truncate=False)
    
    # Example 3: Incremental deduplication (if we have enough files)
    if len(file_paths) > 5:
        print("\n" + "="*70)
        print("EXAMPLE 3: Incremental File Deduplication (Bloom Filter approach)")
        print("="*70)
        
        # Split files into "existing" and "new"
        split_point = len(file_paths) // 2
        existing_files = file_paths[:split_point]
        new_files = file_paths[split_point:]
        
        print(f"Existing files: {len(existing_files)}")
        print(f"New files to check: {len(new_files)}")
        
        unique_new = bloom_filter_file_deduplication(spark, existing_files, new_files)
        
        if unique_new:
            print("\nSample of unique new files (first 5):")
            unique_new.show(5, truncate=False)
    
    # Example 4: Method comparison
    print("\n" + "="*70)
    print("EXAMPLE 4: Method Comparison")
    print("="*70)
    compare_file_deduplication_methods(spark, file_paths)
    
    print("\n" + "="*70)
    print("DEMONSTRATION COMPLETE")
    print("="*70)
    print("\nKey Takeaways:")
    print("  - HyperLogLog: Fast approximate file count estimation")
    print("  - Bloom Filter: Efficient duplicate checking for incremental deduplication")
    print("  - Exact hashing: Accurate but slower for large file collections")
    print("  - Use approximate methods for quick estimates, exact methods for final deduplication")
    print("="*70 + "\n")
    
    spark.stop()


if __name__ == "__main__":
    main()

