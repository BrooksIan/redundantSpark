"""
Pytest configuration and fixtures for Spark testing.
"""
import sys
import os

# Add Spark Python path if not already present
spark_python_path = '/opt/spark/python'
if spark_python_path not in sys.path:
    sys.path.insert(0, spark_python_path)

# Add py4j to path
spark_lib_path = '/opt/spark/python/lib'
if spark_lib_path not in sys.path:
    sys.path.insert(0, spark_lib_path)

# Set SPARK_HOME if not set
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/opt/spark'

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a SparkSession for testing.
    Uses local mode with minimal resources for fast test execution.
    """
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_data(spark_session):
    """
    Create a sample DataFrame with known duplicates for testing.
    """
    from pyspark.sql.types import StructType, StructField, StringType
    
    data = [
        ("ID1", "John Doe", "john@example.com", "123 Main St"),
        ("ID2", "John Doe", "john@example.com", "123 Main St"),  # Exact duplicate
        ("ID3", "Jane Smith", "jane@example.com", "456 Oak Ave"),
        ("ID4", "john doe", "JOHN@EXAMPLE.COM", "123 Main St"),  # Case variation
        ("ID5", "  John Doe  ", "john@example.com", "123 Main St"),  # Whitespace variation
        ("ID6", "Bob Wilson", "bob@example.com", "789 Park Blvd"),
    ]
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
    ])
    
    return spark_session.createDataFrame(data, schema)

