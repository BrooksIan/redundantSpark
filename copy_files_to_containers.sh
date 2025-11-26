#!/bin/bash

# Script to copy data/ directory and Python scripts to Spark containers after deployment
# Usage: ./copy_files_to_containers.sh

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Container names
MASTER_CONTAINER="spark-master"
WORKER_CONTAINER="spark-worker"
TARGET_DIR="/app"

# Function to check if container is running
check_container() {
    local container=$1
    if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        echo -e "${RED}Error: Container ${container} is not running!${NC}"
        echo "Please start the containers first with: docker-compose up -d"
        exit 1
    fi
    echo -e "${GREEN}✓${NC} Container ${container} is running"
}

# Function to wait for container to be ready
wait_for_container() {
    local container=$1
    local max_attempts=30
    local attempt=1
    
    echo -e "${YELLOW}Waiting for ${container} to be ready...${NC}"
    while [ $attempt -le $max_attempts ]; do
        if docker exec ${container} test -d ${TARGET_DIR} 2>/dev/null; then
            echo -e "${GREEN}✓${NC} Container ${container} is ready"
            return 0
        fi
        echo -n "."
        sleep 1
        attempt=$((attempt + 1))
    done
    echo -e "\n${RED}Error: Container ${container} did not become ready in time${NC}"
    return 1
}

# Function to copy files to container
copy_to_container() {
    local container=$1
    local source=$2
    local dest=$3
    
    echo -e "${YELLOW}Copying ${source} to ${container}:${dest}...${NC}"
    if docker cp "${source}" "${container}:${dest}"; then
        echo -e "${GREEN}✓${NC} Successfully copied ${source} to ${container}"
    else
        echo -e "${RED}✗${NC} Failed to copy ${source} to ${container}"
        return 1
    fi
}

# Main execution
echo "=========================================="
echo "Copying files to Spark containers"
echo "=========================================="
echo ""

# Check if containers are running
echo "Checking container status..."
check_container ${MASTER_CONTAINER}
check_container ${WORKER_CONTAINER}
echo ""

# Wait for containers to be ready
wait_for_container ${MASTER_CONTAINER}
wait_for_container ${WORKER_CONTAINER}
echo ""

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Copy data directory
if [ -d "data" ]; then
    echo "Copying data/ directory..."
    copy_to_container ${MASTER_CONTAINER} "data" "${TARGET_DIR}/"
    copy_to_container ${WORKER_CONTAINER} "data" "${TARGET_DIR}/"
    echo ""
else
    echo -e "${YELLOW}Warning: data/ directory not found, skipping...${NC}"
    echo ""
fi

# Copy Python scripts from root directory
echo "Copying Python scripts..."
PYTHON_SCRIPTS=(
    "deduplicate_spark.py"
    "deduplicate_demo.py"
    "deduplicate_files_example.py"
    "bloom_filter_hyperloglog.py"
    "generate_dataset.py"
    "generate_duplicate_files.py"
)

for script in "${PYTHON_SCRIPTS[@]}"; do
    if [ -f "${script}" ]; then
        copy_to_container ${MASTER_CONTAINER} "${script}" "${TARGET_DIR}/"
        copy_to_container ${WORKER_CONTAINER} "${script}" "${TARGET_DIR}/"
    else
        echo -e "${YELLOW}Warning: ${script} not found, skipping...${NC}"
    fi
done

echo ""
echo "=========================================="
echo -e "${GREEN}File copy completed successfully!${NC}"
echo "=========================================="
echo ""
echo "Files are now available in both containers at ${TARGET_DIR}/"
echo ""
echo "To verify, you can run:"
echo "  docker exec ${MASTER_CONTAINER} ls -la ${TARGET_DIR}/"
echo "  docker exec ${WORKER_CONTAINER} ls -la ${TARGET_DIR}/"

