# Docker Commands Helper Guide

This guide contains all Docker and docker-compose commands used to manage the Spark cluster in this project.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Basic Container Management](#basic-container-management)
3. [Cluster Status & Monitoring](#cluster-status--monitoring)
4. [Running Spark Jobs](#running-spark-jobs)
5. [Accessing Containers](#accessing-containers)
6. [Troubleshooting](#troubleshooting)
7. [Image Management](#image-management)
8. [Data Management](#data-management)

---

## Prerequisites

### Check Docker Installation
```bash
docker --version
docker-compose --version
```

### Verify Docker is Running
```bash
docker ps
```

---

## Basic Container Management

### Start the Spark Cluster
Start all services (master and worker) in detached mode:
```bash
docker-compose up -d
```

**What this does:**
- Creates the network (`spark-network`)
- Creates volumes for worker data
- Starts `spark-master` container
- Starts `spark-worker` container
- Both containers run in background (`-d` flag)

### Stop the Spark Cluster
Stop and remove all containers:
```bash
docker-compose down
```

**What this does:**
- Stops all running containers
- Removes containers
- Removes network (but keeps volumes)

### Stop and Remove Volumes
Stop containers and remove all volumes (cleans up data):
```bash
docker-compose down -v
```

### Restart the Cluster
Restart all services:
```bash
docker-compose restart
```

### Rebuild and Start
Rebuild images and start containers (if using custom Dockerfile):
```bash
docker-compose up -d --build
```

**Note:** This project uses a pre-built Apache Spark image, so rebuilding is typically not needed.

---

## Cluster Status & Monitoring

### Check Container Status
View status of all containers:
```bash
docker-compose ps
```

**Output shows:**
- Container names
- Image used
- Status (Up/Down)
- Port mappings
- Creation time

### View Container Logs
View logs from all services:
```bash
docker-compose logs
```

### View Logs for Specific Service
View logs for Spark Master:
```bash
docker-compose logs spark-master
```

View logs for Spark Worker:
```bash
docker-compose logs spark-worker
```

### Follow Logs in Real-Time
Follow logs with auto-refresh:
```bash
docker-compose logs -f
```

Follow logs for specific service:
```bash
docker-compose logs -f spark-master
docker-compose logs -f spark-worker
```

### View Last N Lines of Logs
View last 50 lines:
```bash
docker-compose logs --tail=50 spark-master
```

### Check if Master is Running
Quick check for master status:
```bash
docker-compose logs spark-master | tail -10
```

Look for: `"I have been elected leader! New state: ALIVE"`

### Check if Worker is Registered
Check worker registration:
```bash
docker-compose logs spark-worker | grep -i "registered"
```

Look for: `"Successfully registered with master"`

---

## Running Spark Jobs

### Run a Python Script with spark-submit
Execute a Spark application:
```bash
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name YourAppName \
  your_script.py
```

**Example - Run Deduplication Script:**
```bash
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name DeduplicationTest \
  deduplicate_spark.py exact
```

**Example - Run Bloom Filter Script:**
```bash
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name BloomFilterDemo \
  bloom_filter_hyperloglog.py
```

**Example - Run File Deduplication:**
```bash
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name FileDeduplication \
  bloom_filter_file_deduplication.py
```

**Key Parameters:**
- `--master spark://spark-master:7077`: Connect to Spark master
- `--name`: Application name (appears in Spark UI)
- `-T`: Disable pseudo-TTY allocation (for non-interactive use)

### Run Script with Arguments
Pass arguments to your script:
```bash
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name Deduplication \
  deduplicate_spark.py data/redundant_data.csv spark_hash
```

### Run Script and Save Output
Save output to a file:
```bash
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name Deduplication \
  deduplicate_spark.py exact > output.log 2>&1
```

### Run Script with Filtered Output
Show only important lines:
```bash
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  deduplicate_spark.py exact 2>&1 | grep -E "(Found|Results|SUMMARY)"
```

---

## Accessing Containers

### Open Interactive Shell in Master Container
Access bash shell in spark-master:
```bash
docker-compose exec spark-master bash
```

Once inside, you can:
- Navigate filesystem: `cd /app`
- Run Python: `python3 your_script.py`
- Check Spark: `ls $SPARK_HOME`
- View logs: `tail -f $SPARK_HOME/logs/*.out`

### Open Interactive Shell in Worker Container
```bash
docker-compose exec spark-worker bash
```

### Run Single Command in Container
Execute a command without opening shell:
```bash
docker-compose exec spark-master python3 --version
```

### Check Python Version
```bash
docker-compose exec spark-master python3 --version
```

### List Files in Container
```bash
docker-compose exec spark-master ls -la /app
```

### Check if File Exists
```bash
docker-compose exec spark-master test -f /app/deduplicate_spark.py && echo "File exists"
```

### View Environment Variables
```bash
docker-compose exec spark-master env | grep SPARK
```

---

## Troubleshooting

### Check Container Health
View detailed container information:
```bash
docker-compose ps -a
```

### Inspect Container Configuration
View container details:
```bash
docker inspect spark-master
docker inspect spark-worker
```

### View Resource Usage
Check CPU and memory usage:
```bash
docker stats spark-master spark-worker
```

### Restart a Specific Service
Restart only the master:
```bash
docker-compose restart spark-master
```

Restart only the worker:
```bash
docker-compose restart spark-worker
```

### Stop a Specific Service
Stop only the worker:
```bash
docker-compose stop spark-worker
```

### Start a Specific Service
Start only the worker:
```bash
docker-compose start spark-worker
```

### View Container Logs with Timestamps
```bash
docker-compose logs -t spark-master
```

### Check Network Connectivity
Test if worker can reach master:
```bash
docker-compose exec spark-worker ping spark-master
```

### Check Port Accessibility
Verify ports are accessible:
```bash
# From host machine
curl http://localhost:8080  # Spark Master Web UI
curl http://localhost:8081  # Spark Worker Web UI
```

### View Docker Network
List networks:
```bash
docker network ls
```

Inspect Spark network:
```bash
docker network inspect redundantspark_spark-network
```

### Clean Up Everything
Remove containers, networks, and volumes:
```bash
docker-compose down -v
docker system prune -f
```

**Warning:** This removes all data volumes!

---

## Image Management

### Pull the Spark Image
Pull the Apache Spark Docker image:
```bash
docker pull apache/spark:4.1.0-preview4-scala2.13-java21-python3-r-ubuntu
```

### List Local Images
View all Docker images:
```bash
docker images
```

### Search for Spark Image
```bash
docker images | grep spark
```

### Remove Unused Images
Remove dangling images:
```bash
docker image prune
```

Remove all unused images:
```bash
docker image prune -a
```

### View Image Details
```bash
docker inspect apache/spark:4.1.0-preview4-scala2.13-java21-python3-r-ubuntu
```

---

## Data Management

### Copy Files to Container
Copy file from host to container:
```bash
docker cp local_file.txt spark-master:/app/
```

### Copy Files from Container
Copy file from container to host:
```bash
docker cp spark-master:/app/output.csv ./output.csv
```

### View Volume Information
List volumes:
```bash
docker volume ls
```

Inspect volume:
```bash
docker volume inspect redundantspark_spark-master-data
```

### Access Volume Data
Mount volume in a temporary container:
```bash
docker run --rm -it \
  -v redundantspark_spark-master-data:/data \
  ubuntu:22.04 bash
```

### Backup Volume Data
```bash
docker run --rm \
  -v redundantspark_spark-master-data:/data \
  -v $(pwd):/backup \
  ubuntu:22.04 tar czf /backup/spark-master-backup.tar.gz /data
```

---

## Web UI Access

### Spark Master Web UI
Access at: **http://localhost:8080**

**What you can see:**
- Running applications
- Worker nodes
- Completed applications
- Cluster resource usage

### Spark Worker Web UI
Access at: **http://localhost:8081**

**What you can see:**
- Worker status
- Executors
- Running tasks
- Resource usage

### Spark Application UI
When running a job, access at: **http://localhost:4040**

**What you can see:**
- Job progress
- Stage details
- Task execution
- SQL queries
- Storage information

---

## Common Workflows

### Complete Setup Workflow
```bash
# 1. Pull the Spark image
docker pull apache/spark:4.1.0-preview4-scala2.13-java21-python3-r-ubuntu

# 2. Start the cluster
docker-compose up -d

# 3. Wait for cluster to be ready
sleep 15

# 4. Check status
docker-compose ps
docker-compose logs spark-master | tail -10

# 5. Run a test job
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name TestJob \
  deduplicate_spark.py exact
```

### Daily Development Workflow
```bash
# Start cluster
docker-compose up -d

# Run your script
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --name MyApp \
  my_script.py

# Check results
docker-compose exec spark-master ls -la /app/data/

# Stop when done
docker-compose down
```

### Debugging Workflow
```bash
# Check if cluster is running
docker-compose ps

# View recent logs
docker-compose logs --tail=50 spark-master
docker-compose logs --tail=50 spark-worker

# Access container for debugging
docker-compose exec spark-master bash

# Inside container:
cd /app
python3 -c "import pyspark; print(pyspark.__version__)"
ls -la data/
```

### Clean Restart Workflow
```bash
# Stop everything
docker-compose down

# Remove volumes (optional - removes data)
docker-compose down -v

# Start fresh
docker-compose up -d

# Wait and verify
sleep 15
docker-compose logs spark-master | grep "ALIVE"
```

---

## Environment Variables

### View Current Environment
```bash
docker-compose exec spark-master env
```

### Key Environment Variables
- `SPARK_HOME=/opt/spark`
- `SPARK_MASTER_HOST=spark-master`
- `SPARK_MASTER_PORT=7077`
- `SPARK_WORKER_CORES=2`
- `SPARK_WORKER_MEMORY=2g`

---

## Port Mappings

| Port | Service | Description |
|------|---------|-------------|
| 8080 | Spark Master | Master Web UI |
| 8081 | Spark Worker | Worker Web UI |
| 7077 | Spark Master | Master port for Spark connections |
| 6066 | Spark Master | REST API port |
| 4040 | Spark Apps | Application UI (when running jobs) |

---

## Quick Reference

### Most Common Commands
```bash
# Start cluster
docker-compose up -d

# Stop cluster
docker-compose down

# View logs
docker-compose logs -f spark-master

# Run script
docker-compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 your_script.py

# Access shell
docker-compose exec spark-master bash

# Check status
docker-compose ps
```

### Useful Aliases (Optional)
Add to your `~/.bashrc` or `~/.zshrc`:
```bash
alias spark-up='docker-compose up -d'
alias spark-down='docker-compose down'
alias spark-logs='docker-compose logs -f spark-master'
alias spark-shell='docker-compose exec spark-master bash'
alias spark-submit='docker-compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077'
```

---

## Troubleshooting Common Issues

### Issue: Containers won't start
```bash
# Check Docker is running
docker ps

# Check for port conflicts
netstat -an | grep -E "(8080|8081|7077)"

# View error logs
docker-compose logs
```

### Issue: Worker not connecting to master
```bash
# Check network
docker network inspect redundantspark_spark-network

# Check master is running
docker-compose logs spark-master | grep "ALIVE"

# Restart worker
docker-compose restart spark-worker
```

### Issue: Script can't find files
```bash
# Verify files are in container
docker-compose exec spark-master ls -la /app/data/

# Check working directory
docker-compose exec spark-master pwd
```

### Issue: Out of memory
```bash
# Check resource usage
docker stats

# Increase worker memory in docker-compose.yml
# SPARK_WORKER_MEMORY=4g  # Change from 2g to 4g
docker-compose down
docker-compose up -d
```

---

## Additional Resources

- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081
- **Docker Compose Docs**: https://docs.docker.com/compose/
- **Apache Spark Docs**: https://spark.apache.org/docs/latest/

---

## Notes for New Admins

1. **Always check cluster status** before running jobs: `docker-compose ps`
2. **Wait 10-15 seconds** after starting cluster for services to initialize
3. **Use `-T` flag** with `exec` when running non-interactive commands
4. **Check logs** if something doesn't work: `docker-compose logs spark-master`
5. **Port 4040** is dynamic - each Spark application gets its own port
6. **Volumes persist data** - use `docker-compose down -v` to remove them
7. **The cluster uses a pre-built image** - no need to build from Dockerfile

---

**Last Updated**: November 25, 2025  
**Spark Version**: 4.1.0-preview4  
**Docker Image**: `apache/spark:4.1.0-preview4-scala2.13-java21-python3-r-ubuntu`

