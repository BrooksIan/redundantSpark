"""
Apache Spark script for deduplicating redundant data.
Demonstrates multiple deduplication strategies.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType
import sys
import hashlib


def create_spark_session(app_name="Deduplication", master_url=None):
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


def exact_deduplication(df, key_columns=None):
    """
    Remove exact duplicates using dropDuplicates().
    
    Args:
        df: Input DataFrame
        key_columns: List of columns to use for deduplication (None = all columns)
    
    Returns:
        Deduplicated DataFrame
    """
    if key_columns:
        return df.dropDuplicates(subset=key_columns)
    else:
        return df.dropDuplicates()


def window_based_deduplication(df, key_columns, order_by='id', keep='first'):
    """
    Remove duplicates using window functions, keeping specific records.
    
    Args:
        df: Input DataFrame
        key_columns: List of columns to partition by
        order_by: Column to order by (determines which record to keep)
        keep: 'first' or 'last' - which record to keep
    
    Returns:
        Deduplicated DataFrame
    """
    if keep == 'last':
        window_spec = Window.partitionBy(key_columns).orderBy(F.desc(order_by))
    else:
        window_spec = Window.partitionBy(key_columns).orderBy(F.col(order_by))
    
    return df.withColumn('row_num', F.row_number().over(window_spec)) \
             .filter(F.col('row_num') == 1) \
             .drop('row_num')


def normalize_data(df):
    """
    Normalize data before deduplication (handle NULLs, case, whitespace).
    
    Args:
        df: Input DataFrame
    
    Returns:
        Normalized DataFrame with additional normalized columns
    """
    return df.withColumn('name_normalized', F.trim(F.lower(F.coalesce(F.col('name'), F.lit(''))))) \
            .withColumn('email_normalized', F.trim(F.lower(F.coalesce(F.col('email'), F.lit(''))))) \
            .withColumn('address_normalized', F.trim(F.lower(F.coalesce(F.col('address'), F.lit('')))))


def fuzzy_deduplication_levenshtein(df, threshold=2):
    """
    Find fuzzy duplicates using Levenshtein distance.
    Note: This is expensive for large datasets - use LSH for production.
    
    Args:
        df: Input DataFrame
        threshold: Maximum Levenshtein distance to consider as duplicate
    
    Returns:
        DataFrame with similarity scores
    """
    try:
        import Levenshtein
        
        def levenshtein_udf(s1, s2):
            return Levenshtein.distance(str(s1) if s1 else "", str(s2) if s2 else "")
        
        spark = df.sql_ctx.sparkSession
        spark.udf.register("levenshtein_distance", levenshtein_udf, IntegerType())
        
        # Normalize first
        df_norm = normalize_data(df)
        
        # Self-join to find similar records (expensive!)
        similar = df_norm.alias("a").join(
            df_norm.alias("b"),
            (F.expr(f"levenshtein_distance(a.name_normalized, b.name_normalized) <= {threshold}") |
             F.expr(f"levenshtein_distance(a.email_normalized, b.email_normalized) <= {threshold}")),
            "inner"
        ).select(
            F.col("a.id").alias("id1"),
            F.col("a.name").alias("name1"),
            F.col("a.email").alias("email1"),
            F.col("b.id").alias("id2"),
            F.col("b.name").alias("name2"),
            F.col("b.email").alias("email2"),
            F.expr("levenshtein_distance(a.name_normalized, b.name_normalized)").alias("name_distance"),
            F.expr("levenshtein_distance(a.email_normalized, b.email_normalized)").alias("email_distance")
        ).filter(F.col("id1") < F.col("id2"))  # Avoid duplicate pairs
        
        return similar
        
    except ImportError:
        print("Warning: python-Levenshtein not installed. Install with: pip install python-Levenshtein")
        return None


def fuzzy_deduplication_fuzzywuzzy(df, threshold=85):
    """
    Find fuzzy duplicates using FuzzyWuzzy.
    Note: This is expensive for large datasets - use LSH for production.
    
    Args:
        df: Input DataFrame
        threshold: Minimum similarity score (0-100)
    
    Returns:
        DataFrame with similarity scores
    """
    try:
        from fuzzywuzzy import fuzz
        
        def fuzzy_ratio_udf(s1, s2):
            return fuzz.ratio(str(s1) if s1 else "", str(s2) if s2 else "")
        
        spark = df.sql_ctx.sparkSession
        spark.udf.register("fuzzy_ratio", fuzzy_ratio_udf, IntegerType())
        
        # Normalize first
        df_norm = normalize_data(df)
        
        # Self-join to find similar records
        similar = df_norm.alias("a").join(
            df_norm.alias("b"),
            F.expr(f"fuzzy_ratio(a.name_normalized, b.name_normalized) >= {threshold}"),
            "inner"
        ).select(
            F.col("a.id").alias("id1"),
            F.col("a.name").alias("name1"),
            F.col("b.id").alias("id2"),
            F.col("b.name").alias("name2"),
            F.expr("fuzzy_ratio(a.name_normalized, b.name_normalized)").alias("similarity_score")
        ).filter(F.col("id1") < F.col("id2"))
        
        return similar
        
    except ImportError:
        print("Warning: fuzzywuzzy not installed. Install with: pip install fuzzywuzzy")
        return None


def lsh_deduplication(df, threshold=0.3):
    """
    Use Locality-Sensitive Hashing (LSH) for scalable fuzzy deduplication.
    
    Args:
        df: Input DataFrame
        threshold: Similarity threshold (0.0 to 1.0)
    
    Returns:
        DataFrame with similar record pairs
    """
    try:
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
        similar_pairs = model.stages[-1].approxSimilarityJoin(
            df_hashed, df_hashed, threshold=threshold, distCol="distance"
        ).filter(F.col("datasetA.id") != F.col("datasetB.id"))
        
        return similar_pairs.select(
            F.col("datasetA.id").alias("id1"),
            F.col("datasetA.name").alias("name1"),
            F.col("datasetB.id").alias("id2"),
            F.col("datasetB.name").alias("name2"),
            F.col("distance")
        )
        
    except Exception as e:
        print(f"Error in LSH deduplication: {e}")
        return None


def checksum_based_deduplication(df, columns=None, hash_type='md5'):
    """
    Deduplicate using content-based checksums/hashes.
    
    Args:
        df: Input DataFrame
        columns: List of columns to include in hash (None = all columns)
        hash_type: 'md5' or 'sha256'
    
    Returns:
        Deduplicated DataFrame
    """
    if columns is None:
        columns = df.columns
    
    def compute_hash(*cols):
        """Compute hash of row content."""
        row_str = '|'.join([str(c) if c is not None else "" for c in cols])
        if hash_type == 'sha256':
            return hashlib.sha256(row_str.encode()).hexdigest()
        else:  # md5
            return hashlib.md5(row_str.encode()).hexdigest()
    
    hash_udf = F.udf(compute_hash, StringType())
    
    # Create hash column
    df_with_hash = df.withColumn(
        'content_hash',
        hash_udf(*[F.col(col) for col in columns])
    )
    
    # Remove duplicates based on hash
    df_unique = df_with_hash.dropDuplicates(subset=['content_hash']).drop('content_hash')
    
    return df_unique


def spark_hash_deduplication(df, columns=None):
    """
    Deduplicate using Spark's built-in hash functions (faster than UDF).
    
    Args:
        df: Input DataFrame
        columns: List of columns to include in hash (None = all columns)
    
    Returns:
        Deduplicated DataFrame
    """
    if columns is None:
        columns = df.columns
    
    # Use Spark's md5 function (faster than UDF)
    df_with_hash = df.withColumn(
        'row_hash',
        F.md5(F.concat_ws('|', *[F.col(col) for col in columns]))
    )
    
    # Deduplicate based on hash
    df_unique = df_with_hash.dropDuplicates(subset=['row_hash']).drop('row_hash')
    
    return df_unique


def partitioned_hash_deduplication(df, key_columns, num_partitions=100):
    """
    Deduplicate using hash-based partitioning for better performance on large datasets.
    
    Args:
        df: Input DataFrame
        key_columns: Columns to use for deduplication
        num_partitions: Number of partitions to create
    
    Returns:
        Deduplicated DataFrame
    """
    # Create hash-based partition key
    df_with_partition = df.withColumn(
        'partition_key',
        F.abs(F.hash(*[F.col(col) for col in key_columns])) % num_partitions
    )
    
    # Repartition by hash
    df_partitioned = df_with_partition.repartition(num_partitions, 'partition_key')
    
    # Deduplicate within each partition
    df_unique = df_partitioned.dropDuplicates(subset=key_columns).drop('partition_key')
    
    return df_unique


def file_level_deduplication(spark, file_paths, output_dir="data"):
    """
    Detect and deduplicate files based on content hash.
    
    Args:
        spark: SparkSession
        file_paths: List of file paths to check
        output_dir: Directory to save results
    
    Returns:
        Tuple of (unique_files_df, duplicate_groups_df)
    """
    import os
    
    file_data = []
    for file_path in file_paths:
        if os.path.exists(file_path):
            try:
                # Compute file content hash
                hash_md5 = hashlib.md5()
                with open(file_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
                file_hash = hash_md5.hexdigest()
                
                file_data.append({
                    'file_path': file_path,
                    'file_name': os.path.basename(file_path),
                    'file_size': os.path.getsize(file_path),
                    'content_hash': file_hash
                })
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
                continue
    
    if not file_data:
        print("No valid files found to process")
        return None, None
    
    df_files = spark.createDataFrame(file_data)
    
    # Group by content hash to find duplicates
    duplicate_groups = df_files.groupBy('content_hash').agg(
        F.collect_list('file_path').alias('duplicate_paths'),
        F.count('*').alias('duplicate_count')
    ).filter(F.col('duplicate_count') > 1)
    
    # Keep one file per hash (keep first alphabetically)
    window_spec = Window.partitionBy('content_hash').orderBy('file_path')
    df_unique_files = df_files.withColumn('row_num', F.row_number().over(window_spec)) \
                              .filter(F.col('row_num') == 1) \
                              .drop('row_num')
    
    return df_unique_files, duplicate_groups


def validate_deduplication(original_df, deduplicated_df, key_columns):
    """
    Validate deduplication results and print statistics.
    
    Args:
        original_df: Original DataFrame
        deduplicated_df: Deduplicated DataFrame
        key_columns: Columns used for deduplication
    """
    original_count = original_df.count()
    unique_count = deduplicated_df.count()
    duplicates_removed = original_count - unique_count
    
    # Check for remaining duplicates
    remaining_duplicates = deduplicated_df.groupBy(key_columns).count() \
                                          .filter(F.col('count') > 1)
    remaining_count = remaining_duplicates.count()
    
    print("\n" + "="*60)
    print("DEDUPLICATION RESULTS")
    print("="*60)
    print(f"Original records:        {original_count:,}")
    print(f"Unique records:          {unique_count:,}")
    print(f"Duplicates removed:      {duplicates_removed:,}")
    print(f"Deduplication rate:      {duplicates_removed/original_count*100:.2f}%")
    print(f"Remaining duplicate groups: {remaining_count}")
    print("="*60 + "\n")
    
    return {
        'original_count': original_count,
        'unique_count': unique_count,
        'duplicates_removed': duplicates_removed,
        'deduplication_rate': duplicates_removed / original_count * 100
    }


def process_file_spark(spark, input_path, output_dir="data", method='exact'):
    """Process a single file with Spark and return statistics."""
    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_path = os.path.join(output_dir, f"deduplicated_{base_name}.parquet")
    
    print(f"\n{'='*70}")
    print(f"Processing: {input_path}")
    print(f"Deduplication method: {method}")
    print(f"{'='*70}")
    
    # Read data
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    original_count = df.count()
    print(f"Loaded {original_count:,} records")
    
    # Perform deduplication based on method
    if method == 'exact':
        # Simple exact deduplication
        df_unique = exact_deduplication(df, key_columns=['name', 'email'])
        
    elif method == 'window':
        # Window-based deduplication (keeps first record by ID)
        df_unique = window_based_deduplication(
            df, 
            key_columns=['name', 'email'],
            order_by='id',
            keep='first'
        )
        
    elif method == 'normalized':
        # Normalize then deduplicate
        df_norm = normalize_data(df)
        df_unique = exact_deduplication(
            df_norm, 
            key_columns=['name_normalized', 'email_normalized']
        ).select('id', 'name', 'email', 'address')  # Drop normalized columns
        
    elif method == 'fuzzy_levenshtein':
        # Find fuzzy duplicates (expensive!)
        print("Warning: Fuzzy deduplication with Levenshtein is expensive for large datasets")
        similar_pairs = fuzzy_deduplication_levenshtein(df, threshold=2)
        if similar_pairs:
            print(f"Found {similar_pairs.count()} similar record pairs")
            similar_pairs.show(20, truncate=False)
        # For actual deduplication, you'd need to merge these pairs
        df_unique = exact_deduplication(df, key_columns=['name', 'email'])
        
    elif method == 'fuzzy_fuzzywuzzy':
        # Find fuzzy duplicates using FuzzyWuzzy
        print("Warning: Fuzzy deduplication with FuzzyWuzzy is expensive for large datasets")
        similar_pairs = fuzzy_deduplication_fuzzywuzzy(df, threshold=85)
        if similar_pairs:
            print(f"Found {similar_pairs.count()} similar record pairs")
            similar_pairs.show(20, truncate=False)
        df_unique = exact_deduplication(df, key_columns=['name', 'email'])
        
    elif method == 'lsh':
        # Use LSH for scalable fuzzy matching
        print("Using LSH for fuzzy deduplication...")
        similar_pairs = lsh_deduplication(df, threshold=0.3)
        if similar_pairs:
            print(f"Found {similar_pairs.count()} similar record pairs")
            similar_pairs.show(20, truncate=False)
        # For actual deduplication, merge similar pairs
        df_unique = exact_deduplication(df, key_columns=['name', 'email'])
        
    elif method == 'checksum_md5':
        # Checksum-based deduplication using MD5
        print("Using MD5 checksum-based deduplication...")
        df_unique = checksum_based_deduplication(df, columns=['name', 'email', 'address'], hash_type='md5')
        
    elif method == 'checksum_sha256':
        # Checksum-based deduplication using SHA-256
        print("Using SHA-256 checksum-based deduplication...")
        df_unique = checksum_based_deduplication(df, columns=['name', 'email', 'address'], hash_type='sha256')
        
    elif method == 'spark_hash':
        # Using Spark's built-in hash function (faster)
        print("Using Spark built-in hash function for deduplication...")
        df_unique = spark_hash_deduplication(df, columns=['name', 'email', 'address'])
        
    elif method == 'partitioned_hash':
        # Hash-based partitioned deduplication
        print("Using partitioned hash-based deduplication...")
        df_unique = partitioned_hash_deduplication(df, key_columns=['name', 'email'], num_partitions=100)
        
    else:
        print(f"Unknown method: {method}")
        print("Available methods: exact, window, normalized, fuzzy_levenshtein, fuzzy_fuzzywuzzy, lsh, checksum_md5, checksum_sha256, spark_hash, partitioned_hash")
        return None
    
    # Validate results
    stats = validate_deduplication(df, df_unique, ['name', 'email'])
    
    # Write output
    print(f"Writing deduplicated data to: {output_path}")
    df_unique.write.mode('overwrite').parquet(output_path)
    print("Done!")
    
    # Show sample results
    print("\nSample of deduplicated data:")
    df_unique.show(5, truncate=False)
    
    return stats


def deduplicate_files(spark, file_paths, output_dir="data"):
    """
    Deduplicate files based on content hash.
    
    Args:
        spark: SparkSession
        file_paths: List of file paths or directory path
        output_dir: Directory to save results
    """
    import os
    import glob
    
    # If single path provided and it's a directory, get all files
    if len(file_paths) == 1 and os.path.isdir(file_paths[0]):
        file_paths = glob.glob(os.path.join(file_paths[0], "*"))
        file_paths = [f for f in file_paths if os.path.isfile(f)]
    
    print(f"\n{'='*70}")
    print(f"File-Level Deduplication")
    print(f"Processing {len(file_paths)} file(s)")
    print(f"{'='*70}")
    
    unique_files, duplicate_groups = file_level_deduplication(spark, file_paths, output_dir)
    
    if unique_files is None:
        print("No files processed")
        return
    
    unique_count = unique_files.count()
    duplicate_count = duplicate_groups.count() if duplicate_groups else 0
    
    print(f"\nResults:")
    print(f"  Total files processed: {len(file_paths)}")
    print(f"  Unique files: {unique_count}")
    print(f"  Duplicate groups found: {duplicate_count}")
    
    if duplicate_count > 0:
        print(f"\nDuplicate file groups:")
        duplicate_groups.show(20, truncate=False)
    
    # Save results
    output_path = os.path.join(output_dir, "file_deduplication_results.parquet")
    unique_files.write.mode('overwrite').parquet(output_path)
    print(f"\nResults saved to: {output_path}")


def main():
    """Main function to run deduplication."""
    import os
    import glob
    
    # Available methods
    available_methods = ['exact', 'window', 'normalized', 'fuzzy_levenshtein', 'fuzzy_fuzzywuzzy', 
                        'lsh', 'checksum_md5', 'checksum_sha256', 'spark_hash', 'partitioned_hash']
    
    # Determine input files and method
    if len(sys.argv) > 1 and sys.argv[1] not in available_methods:
        # Files specified as arguments
        input_files = [f for f in sys.argv[1:] if not f.startswith('-')]
        method = sys.argv[-1] if sys.argv[-1] in available_methods else 'exact'
    else:
        # Process all CSV files in data/ directory
        data_dir = "data"
        if os.path.exists(data_dir):
            input_files = glob.glob(os.path.join(data_dir, "*.csv"))
            # Exclude already deduplicated files
            input_files = [f for f in input_files if not os.path.basename(f).startswith("deduplicated_")]
        else:
            # Fallback to current directory
            input_files = glob.glob("*.csv")
            input_files = [f for f in input_files if not f.startswith("deduplicated_")]
        
        # Check for method argument
        method = 'exact'
        if len(sys.argv) > 1:
            if sys.argv[1] in available_methods:
                method = sys.argv[1]
    
    if not input_files:
        print("Usage: python deduplicate_spark.py [file1.csv] [file2.csv] ... [method]")
        print("   OR: python deduplicate_spark.py [method]  (processes all CSV files in data/)")
        print("\nAvailable methods:")
        print("  - exact: Simple exact duplicate removal")
        print("  - window: Window-based deduplication (keeps first/last)")
        print("  - normalized: Normalize then deduplicate")
        print("  - fuzzy_levenshtein: Fuzzy matching with Levenshtein distance")
        print("  - fuzzy_fuzzywuzzy: Fuzzy matching with FuzzyWuzzy")
        print("  - lsh: Locality-Sensitive Hashing for scalable fuzzy matching")
        print("  - checksum_md5: Content-based deduplication using MD5 hash")
        print("  - checksum_sha256: Content-based deduplication using SHA-256 hash")
        print("  - spark_hash: Using Spark's built-in hash function (fastest)")
        print("  - partitioned_hash: Hash-based partitioned deduplication")
        print("\nNo CSV files found to process!")
        sys.exit(1)
    
    # Create Spark session (reuse for all files)
    spark = create_spark_session("RedundantDataDeduplication")
    
    print(f"Found {len(input_files)} file(s) to process:")
    for f in input_files:
        print(f"  - {f}")
    print(f"\nUsing deduplication method: {method}")
    
    # Process each file
    all_stats = []
    for input_path in input_files:
        try:
            stats = process_file_spark(spark, input_path, output_dir="data", method=method)
            if stats:
                stats['filename'] = os.path.basename(input_path)
                all_stats.append(stats)
        except Exception as e:
            print(f"\nError processing {input_path}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Summary across all files
    if len(all_stats) > 1:
        print("\n" + "="*70)
        print("SUMMARY ACROSS ALL FILES")
        print("="*70)
        total_original = sum(s['original_count'] for s in all_stats)
        total_unique = sum(s['unique_count'] for s in all_stats)
        total_removed = sum(s['duplicates_removed'] for s in all_stats)
        
        print(f"\nTotal files processed:     {len(all_stats)}")
        print(f"Total original records:    {total_original:,}")
        print(f"Total unique records:       {total_unique:,}")
        print(f"Total duplicates removed:   {total_removed:,}")
        print(f"Overall deduplication rate: {total_removed/total_original*100:.2f}%")
        
        print("\nPer-file breakdown:")
        for stats in all_stats:
            print(f"  {stats['filename']:30} {stats['original_count']:>8,} -> {stats['unique_count']:>8,} "
                  f"({stats['deduplication_rate']:>5.2f}% removed)")
        print("="*70 + "\n")
    
    spark.stop()


if __name__ == "__main__":
    main()

