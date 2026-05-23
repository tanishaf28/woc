# Eval Folder Structure (From Code)

## Actual Structure from meters.go

Results are **automatically organized by server/client ID**:

```
./eval/
├── server0/
│   ├── s0_n5_f2_b10_YYYYMMDD_HHMMSS.csv
│   ├── s0_n5_f2_b10_YYYYMMDD_HHMMSS.csv
│   └── ...
├── server1/
│   ├── s1_n5_f2_b10_YYYYMMDD_HHMMSS.csv
│   └── ...
├── server2/
│   ├── s2_n5_f2_b10_YYYYMMDD_HHMMSS.csv
│   └── ...
├── server3/
│   ├── s3_n5_f2_b10_YYYYMMDD_HHMMSS.csv
│   └── ...
├── server4/
│   ├── s4_n5_f2_b10_YYYYMMDD_HHMMSS.csv
│   └── ...
├── client0/
│   ├── client0_n5_f2_b10_YYYYMMDD_HHMMSS.csv
│   └── ...
└── client1/
    ├── client1_n5_f2_b10_YYYYMMDD_HHMMSS.csv
    └── ...
```

## How Results Get Organized

**From [eval/meters.go](eval/meters.go#L148-L166):**

### For Servers
- If filename starts with `s` (like `s0_n5_...`)
- Extracts server ID from character position 1 (s**0**...)
- Saves to: `./eval/server{id}/`

### For Clients
- If filename starts with `client` (like `client0_n5_...`)
- Saves to: `./eval/{client_part}/` (e.g., `client0/`, `client1/`)

### Default
- If filename doesn't match either pattern
- Saves to: `./eval/default/`

## File Naming

**Format:** `{ID}_{params}_{YYYYMMDD_HHMMSS}.csv`

### Server filenames
```
s0_n5_f2_b10_suffix_20260512_143025.csv
s1_n5_f2_b10_suffix_20260512_143025.csv
s2_n5_f2_b10_suffix_20260512_143025.csv
s3_n5_f2_b10_suffix_20260512_143025.csv
s4_n5_f2_b10_suffix_20260512_143025.csv
```
**From main.go line 79:**
```go
fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_%s", 
    myServerID, numOfServers, quorum, batchsize, suffix)
```

### Client filenames
```
client0_n5_f2_b10_suffix_20260512_143025.csv
client1_n5_f2_b10_suffix_20260512_143025.csv
```

## Retrieve Results

```bash
# View folder structure
ssh -i ~/.ssh/tani.pem ubuntu@192.168.73.159 \
  "find /home/ubuntu/woc/eval -type f -name '*.csv' | sort"

# Download all results
rsync -avz -e "ssh -i ~/.ssh/tani.pem" \
  ubuntu@192.168.73.159:/home/ubuntu/woc/eval/ ./eval_data/

# Or download specific server/client results
rsync -avz -e "ssh -i ~/.ssh/tani.pem" \
  ubuntu@192.168.73.159:/home/ubuntu/woc/eval/server0/ ./server0_results/
```

## Analysis

**Merge results from all servers:**
```bash
# All server0 results
cat eval_data/server0/*.csv > server0_merged.csv

# All servers combined
cat eval_data/server*/*.csv > all_servers.csv

# Extract specific metrics
grep "throughput" eval_data/server*/*.csv | cut -d, -f2- > throughput_all.csv
```

## When Running Evaluations

Each evaluation (EVAL 1-4) will:

1. Start 5 servers (with IDs 0-4) on 5 nodes
2. Start 2 clients (with IDs -1, which become client0/client1) on 2 nodes
3. Each server writes to `./eval/server{id}/`
4. Each client writes to `./eval/client{id}/`
5. All files timestamped with test execution time

**Result:** Clean separation of metrics by role (server vs client) and by instance ID.

---

## Code References

- **meters.go lines 148-166:** Folder organization logic
- **main.go line 79:** Filename generation
- **eval/meters.go line 146:** SaveToFile() function
