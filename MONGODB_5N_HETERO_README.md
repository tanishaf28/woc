# MongoDB Workload A - Heterogeneous 5-Node Cluster Setup

## Quick Start Guide

### Configuration
Your new heterogeneous cluster setup:
- **2 Strong Nodes (c16 - 16 vCPU):** 192.168.73.159, 192.168.73.84
- **3 Weak Nodes (c4 - 4 vCPU):** 192.168.73.69, 192.168.73.235, 192.168.73.194

### Files Created
1. `config/cluster_hetero_5n_2s3w.conf` - Cluster configuration file
2. `start_mongodb_hetero_5n.sh` - Startup script
3. `stop_mongodb_hetero_5n.sh` - Shutdown script

### Step 1: Make scripts executable
```bash
chmod +x start_mongodb_hetero_5n.sh stop_mongodb_hetero_5n.sh
```

### Step 2: Run MongoDB with Workload A
```bash
# Start cluster with workload A
bash start_mongodb_hetero_5n.sh a

# Or with a time limit (e.g., 300 seconds = 5 minutes):
# bash start_mongodb_hetero_5n.sh a 300
```

### Step 3: Monitor the cluster
```bash
# View server logs from first node
ssh -i /path/to/tani.pem ubuntu@192.168.73.159 "tail -f /home/ubuntu/woc/logs/server_*.log"

# View evaluation results
ssh -i /path/to/tani.pem ubuntu@192.168.73.159 "tail -f /home/ubuntu/woc/eval/*.csv"
```

### Step 4: Stop the cluster
```bash
bash stop_mongodb_hetero_5n.sh
```

## What the scripts do:

### start_mongodb_hetero_5n.sh
1. Builds the WOC binary locally
2. Distributes binary and config to all 5 servers and 2 client nodes
3. Starts MongoDB daemon on each server with replica set
4. Initializes MongoDB replica set
5. Starts WOC server processes on all 5 nodes
6. Starts WOC client processes on 2 client nodes
7. Begins workload execution with specified workload (a-f)

### Workload Options
- **a** - Read-heavy (50% read, 50% write)
- **b** - Read-mostly (95% read, 5% write)
- **c** - Read-only
- **d** - Read-latest
- **e** - Short-ranges
- **f** - Read-modify-write

## Evaluation Metrics
Results will be saved in:
- Latency: `/home/ubuntu/woc/eval/*.csv`
- Logs: `/home/ubuntu/woc/logs/server_*.log`

## Troubleshooting

### Connection issues?
Check SSH key permissions:
```bash
chmod 600 ~/.ssh/tani.pem
```

### MongoDB not initializing?
SSH to first server and check:
```bash
ssh -i ~/.ssh/tani.pem ubuntu@192.168.73.159
mongosh --eval "rs.status()"
```

### Processes not stopping?
Manual cleanup:
```bash
for ip in 192.168.73.159 192.168.73.84 192.168.73.69 192.168.73.235 192.168.73.194; do
  ssh -i ~/.ssh/tani.pem ubuntu@$ip "pkill -9 woc; pkill -9 mongod"
done
```

## Notes
- The cluster uses MongoDB replica set for fault tolerance
- Evaluation results are timestamped with node configurations
- Ensure SSH keys are properly set up before running scripts
