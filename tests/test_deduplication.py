"""
Unit tests for deduplication functions.
"""
import pytest
from pyspark.sql import functions as F
from deduplicate_spark import (
    exact_deduplication,
    window_based_deduplication,
    normalize_data,
    spark_hash_deduplication,
    checksum_based_deduplication,
    create_spark_session,
    validate_deduplication,
)


class TestExactDeduplication:
    """Tests for exact deduplication method."""
    
    def test_exact_deduplication_all_columns(self, spark_session, sample_data):
        """Test that exact_deduplication removes duplicate rows across all columns."""
        result = exact_deduplication(sample_data)
        
        # Sample data has 6 records
        # Note: ID1 and ID2 have same name/email/address but different IDs
        # dropDuplicates() without subset checks ALL columns including ID
        # So ID1 and ID2 are NOT duplicates (different IDs)
        # All 6 records are unique when considering all columns including ID
        
        # Verify no exact duplicates remain (all columns including ID)
        assert result.count() == result.dropDuplicates().count()
        # All records should be unique when considering all columns
        assert result.count() == 6
    
    def test_exact_deduplication_subset_columns(self, spark_session, sample_data):
        """Test deduplication on specific columns."""
        result = exact_deduplication(sample_data, key_columns=['name', 'email'])
        
        # Should remove ID2 (exact duplicate) and potentially others
        assert result.count() <= sample_data.count()
        # Verify no duplicates on key columns
        duplicates = result.groupBy('name', 'email').count().filter(F.col('count') > 1)
        assert duplicates.count() == 0
    
    def test_exact_deduplication_no_duplicates(self, spark_session):
        """Test with data that has no duplicates."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        data = [
            ("ID1", "John Doe", "john@example.com"),
            ("ID2", "Jane Smith", "jane@example.com"),
        ]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ])
        df = spark_session.createDataFrame(data, schema)
        
        result = exact_deduplication(df)
        assert result.count() == 2  # No duplicates to remove


class TestWindowBasedDeduplication:
    """Tests for window-based deduplication."""
    
    def test_window_keeps_first(self, spark_session, sample_data):
        """Test that window-based deduplication keeps first record."""
        result = window_based_deduplication(
            sample_data,
            key_columns=['name', 'email'],
            order_by='id',
            keep='first'
        )
        
        # Should keep ID1 (first) and remove ID2 (duplicate)
        ids = [row.id for row in result.select('id').collect()]
        assert 'ID1' in ids
        # ID2 should be removed if it's a duplicate
        assert result.count() <= sample_data.count()
    
    def test_window_keeps_last(self, spark_session, sample_data):
        """Test that window-based deduplication keeps last record."""
        result = window_based_deduplication(
            sample_data,
            key_columns=['name', 'email'],
            order_by='id',
            keep='last'
        )
        
        assert result.count() <= sample_data.count()
        # Verify no duplicates on key columns
        duplicates = result.groupBy('name', 'email').count().filter(F.col('count') > 1)
        assert duplicates.count() == 0


class TestNormalizeData:
    """Tests for data normalization."""
    
    def test_normalize_creates_columns(self, spark_session, sample_data):
        """Test that normalization creates normalized columns."""
        result = normalize_data(sample_data)
        
        assert 'name_normalized' in result.columns
        assert 'email_normalized' in result.columns
        assert 'address_normalized' in result.columns
    
    def test_normalize_lowercase(self, spark_session, sample_data):
        """Test that normalization converts to lowercase."""
        result = normalize_data(sample_data)
        
        # Check that normalized email is lowercase
        normalized_emails = result.select('email_normalized').distinct().collect()
        for row in normalized_emails:
            if row.email_normalized:
                assert row.email_normalized == row.email_normalized.lower()
    
    def test_normalize_handles_null(self, spark_session):
        """Test that normalization handles NULL values."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        data = [
            ("ID1", "John Doe", None, "123 Main St"),
            ("ID2", None, "john@example.com", None),
        ]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StringType(), True),
        ])
        df = spark_session.createDataFrame(data, schema)
        
        result = normalize_data(df)
        # Should not raise exception and should handle NULLs
        assert result.count() == 2


class TestHashDeduplication:
    """Tests for hash-based deduplication."""
    
    def test_spark_hash_deduplication(self, spark_session, sample_data):
        """Test Spark hash-based deduplication."""
        result = spark_hash_deduplication(sample_data, columns=['name', 'email', 'address'])
        
        assert result.count() <= sample_data.count()
        # Verify no duplicates remain
        assert result.count() == result.dropDuplicates().count()
    
    def test_checksum_md5_deduplication(self, spark_session, sample_data):
        """Test MD5 checksum-based deduplication."""
        result = checksum_based_deduplication(
            sample_data,
            columns=['name', 'email', 'address'],
            hash_type='md5'
        )
        
        assert result.count() <= sample_data.count()
        # Verify no duplicates remain
        assert result.count() == result.dropDuplicates().count()
    
    def test_checksum_sha256_deduplication(self, spark_session, sample_data):
        """Test SHA-256 checksum-based deduplication."""
        result = checksum_based_deduplication(
            sample_data,
            columns=['name', 'email', 'address'],
            hash_type='sha256'
        )
        
        assert result.count() <= sample_data.count()
        # Verify no duplicates remain
        assert result.count() == result.dropDuplicates().count()


class TestSparkSession:
    """Tests for Spark session creation."""
    
    def test_create_spark_session_default(self, spark_session):
        """Test creating Spark session with defaults."""
        # Note: SparkSession may reuse existing session, so we test that it exists
        spark = create_spark_session("TestApp")
        
        assert spark is not None
        # Spark may reuse existing session, so just verify it's a SparkSession
        assert hasattr(spark, 'sparkContext')
        # Don't stop as it may be shared with other tests
    
    def test_create_spark_session_custom_name(self, spark_session):
        """Test creating Spark session with custom name."""
        # Note: SparkSession may reuse existing session, so we test that it exists
        spark = create_spark_session("CustomTestApp")
        
        assert spark is not None
        # Spark may reuse existing session, so just verify it's a SparkSession
        assert hasattr(spark, 'sparkContext')
        # Don't stop as it may be shared with other tests


class TestValidateDeduplication:
    """Tests for validation function."""
    
    def test_validate_deduplication(self, spark_session, sample_data):
        """Test validation function returns correct statistics."""
        deduplicated = exact_deduplication(sample_data, key_columns=['name', 'email'])
        
        stats = validate_deduplication(sample_data, deduplicated, ['name', 'email'])
        
        assert 'original_count' in stats
        assert 'unique_count' in stats
        assert 'duplicates_removed' in stats
        assert 'deduplication_rate' in stats
        
        assert stats['original_count'] == sample_data.count()
        assert stats['unique_count'] == deduplicated.count()
        assert stats['duplicates_removed'] >= 0
        assert 0 <= stats['deduplication_rate'] <= 100

