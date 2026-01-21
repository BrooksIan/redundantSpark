#!/bin/bash
# Script to run tests in Docker container

set -e

echo "=========================================="
echo "Running Tests in Docker Container"
echo "=========================================="

# Check if Docker containers are running
if ! docker ps | grep -q spark-master; then
    echo "Error: Spark containers are not running."
    echo "Please start them with: docker-compose up -d"
    exit 1
fi

echo ""
echo "Installing test dependencies..."
docker-compose exec -T --user root spark-master pip3 install pytest pytest-cov 2>/dev/null || true

echo ""
echo "Running pytest in Docker container..."
echo ""

# Set up environment and run tests
export SPARK_HOME=/opt/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip:$PYTHONPATH

# Run tests with coverage
docker-compose exec -T spark-master bash -c "export SPARK_HOME=/opt/spark && export PYTHONPATH=\$SPARK_HOME/python:\$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip:\$PYTHONPATH && python3 -m pytest tests/ -v --tb=short --cov=deduplicate_spark --cov-report=term-missing"

echo ""
echo "=========================================="
echo "Tests completed!"
echo "=========================================="

