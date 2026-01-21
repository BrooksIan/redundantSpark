# Cloudera AI Workbench Configuration Guide

This guide explains the adjustments needed to run the deduplication scripts in Cloudera AI Workbench.

## Automatic Detection

The code now automatically detects Cloudera AI Workbench environment by checking for:
- `CDSW_PROJECT_ID` environment variable
- `CDSW_APP_ID` environment variable
- `CLOUDERA_AI_WORKBENCH` environment variable
- Presence of `/var/lib/cdsw` or `/home/cdsw` directories

When detected, the code will:
1. **Use existing SparkSession**: If Cloudera AI Workbench provides a pre-configured SparkSession, it will be reused
2. **Skip master URL configuration**: The platform manages the Spark master URL
3. **Skip authentication config**: Cloudera uses Kerberos/enterprise authentication, not `spark.authenticate.secret`

## Manual Usage Options

### Option 1: Use Pre-configured SparkSession (Recommended)

In Cloudera AI Workbench, you can often use the pre-configured `spark` session directly:

```python
# Instead of: spark = create_spark_session("MyApp")
# Just use: spark (if available in your environment)

# Or check if it exists:
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession() or create_spark_session("MyApp")
```

### Option 2: Use create_spark_session Function

The function will automatically detect Cloudera AI Workbench and configure appropriately:

```python
from deduplicate_spark import create_spark_session

spark = create_spark_session("DeduplicationApp")
# Will automatically use existing session or create with minimal config
```

## File Path Adjustments

### Local Files vs Distributed Storage

Cloudera AI Workbench typically uses distributed storage (HDFS, S3, etc.). Adjust file paths accordingly:

**Before (local files):**
```python
input_path = "data/redundant_data.csv"
```

**After (HDFS):**
```python
# In Cloudera, HDFS is typically the default filesystem, so use paths without protocol:
input_path = "/user/username/data/redundant_data.csv"  # Recommended - uses default FS
# Or with explicit protocol (if you know the namenode):
input_path = "hdfs://namenode:8020/user/username/data/redundant_data.csv"
```

**After (S3):**
```python
input_path = "s3a://bucket-name/data/redundant_data.csv"
```

### Reading Files

The code should work as-is, but ensure paths are correct:

```python
# Example: Reading from HDFS
df = spark.read.csv("hdfs:///user/username/data/redundant_data.csv", header=True, inferSchema=True)

# Example: Reading from S3
df = spark.read.csv("s3a://my-bucket/data/redundant_data.csv", header=True, inferSchema=True)
```

## Dependencies

Ensure all required Python packages are installed in your Cloudera AI Workbench environment:

```bash
# Install dependencies
pip install fuzzywuzzy python-Levenshtein rapidfuzz

# Optional: For Bloom Filter support
pip install pybloom-live
```

## Environment Variables

If you need to override behavior, you can set:

```bash
# Force a specific Spark master (usually not needed in Cloudera)
export SPARK_MASTER="yarn"

# Disable Cloudera detection (if needed)
unset CDSW_PROJECT_ID
```

## Output Path Handling

The code now automatically writes to **local filesystem** by default in Cloudera AI Workbench:

- **Default output directory**: `/tmp/results` (accessible by all Spark executors)
- **Relative paths** (e.g., `"data"`, `"results"`) are converted to `/tmp/results` or `/tmp/{path}`
- **Absolute paths in /tmp** (e.g., `"/tmp/results"`) are used as-is
- **Paths in /home** are converted to `/tmp` to ensure executors can access them
- **HDFS/S3 paths** with explicit protocols (e.g., `"hdfs://namenode:8020/path"`, `"s3a://bucket/path"`) are supported if needed

**Important**: In distributed Spark, executors run on different nodes. Using `/tmp/results` ensures all executors can write to the same path on their respective nodes.

### Example:

```python
# Default: writes to /tmp/results (accessible by all executors)
stats = process_file_spark(
    spark, 
    "/user/username/data/input.csv",  # Can read from HDFS
    output_dir=None,  # Defaults to '/tmp/results'
    method='exact'
)

# Explicitly specify output directory (will be converted to /tmp/results)
stats = process_file_spark(
    spark, 
    "/user/username/data/input.csv",
    output_dir="results",  # Will become /tmp/results
    method='exact'
)

# Or use absolute /tmp path
stats = process_file_spark(
    spark, 
    "/user/username/data/input.csv",
    output_dir="/tmp/my_results",  # Accessible by all executors
    method='exact'
)

# If you need HDFS output (with explicit protocol and namenode)
stats = process_file_spark(
    spark, 
    "/user/username/data/input.csv",
    output_dir="hdfs://namenode:8020/user/username/output",  # Explicit HDFS
    method='exact'
)
```

## Common Issues and Solutions

### Issue: "Mkdirs failed to create file:/home/cdsw/results/..." or "file:/user/cdsw/data/..."

**Solution**: This error occurs in distributed Spark when executors try to write to paths that don't exist on their nodes. The code now:
- **Defaults to `/tmp/results`** in Cloudera AI Workbench (accessible by all executors)
- Converts `/home` paths to `/tmp` automatically
- Creates directories with appropriate permissions

If you still see this error:
1. Use default: `output_dir=None` (defaults to `/tmp/results`)
2. Or explicitly use `/tmp`: `output_dir="/tmp/results"`
3. Avoid `/home` paths in distributed mode - they're automatically converted to `/tmp`
4. If you need HDFS, use explicit protocol with namenode: `output_dir="hdfs://namenode:8020/user/username/data"`

### Issue: "Incomplete HDFS URI, no host: hdfs:///user/..."

**Solution**: This error occurs when using `hdfs://` protocol without a namenode host. The code now:
- **Defaults to local filesystem** (`results/` directory) to avoid this issue
- If you need HDFS, use explicit protocol with namenode: `hdfs://namenode:8020/path`
- Or use paths without protocol if HDFS is default FS: `/user/username/data` (but local is recommended)

### Issue: "Cannot find SparkSession"

**Solution**: Ensure you're running in a Cloudera AI Workbench session that has Spark enabled. The platform should provide a SparkSession automatically.

### Issue: File not found errors

**Solution**: 
- Check that file paths use the correct protocol (hdfs://, s3a://, etc.)
- Verify file permissions in HDFS/S3
- Use absolute paths instead of relative paths

### Issue: Authentication errors

**Solution**: 
- Cloudera uses Kerberos authentication, which is handled automatically by the platform
- Ensure you're properly authenticated to the cluster
- Check that your user has permissions to access the data

### Issue: Memory/resource errors

**Solution**: 
- Adjust Spark configuration through Cloudera AI Workbench UI
- Or set Spark configs in your code:
```python
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.executor.cores", "2")
```

## Example: Running in Cloudera AI Workbench

```python
from deduplicate_spark import create_spark_session, process_file_spark

# Create or get SparkSession (automatically detects Cloudera)
spark = create_spark_session("DeduplicationApp")

# Process file from HDFS, write to /tmp/results (accessible by all executors)
input_path = "/user/username/data/redundant_data.csv"
stats = process_file_spark(
    spark, 
    input_path, 
    output_dir=None,  # Defaults to /tmp/results
    method='exact'
)

# Results will be written to: /tmp/results/deduplicated_redundant_data.parquet
# Note: Files are written on each executor node's /tmp/results directory
# You may need to collect results from executor nodes or use HDFS for shared access

# Or use S3
input_path = "s3a://my-bucket/data/redundant_data.csv"
stats = process_file_spark(
    spark, 
    input_path, 
    output_dir="s3a://my-bucket/data/output",
    method='exact'
)

spark.stop()
```

## Testing Detection

To verify Cloudera AI Workbench detection is working:

```python
import os

is_cloudera = (
    os.getenv("CDSW_PROJECT_ID") is not None or
    os.getenv("CDSW_APP_ID") is not None or
    os.path.exists("/var/lib/cdsw") or
    os.path.exists("/home/cdsw")
)

print(f"Cloudera AI Workbench detected: {is_cloudera}")
```

## Additional Notes

- **Kerberos**: Cloudera environments use Kerberos for authentication. This is handled automatically by the platform.
- **YARN**: Cloudera typically uses YARN as the resource manager. The code will work with YARN without additional configuration.
- **Spark Configuration**: Most Spark configurations are managed by Cloudera Manager. Only set configs that are specific to your application.
- **Logging**: Check Cloudera AI Workbench logs and Spark UI for debugging information.

