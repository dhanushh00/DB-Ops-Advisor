# DB Ops Advisor — Cloud Database Benchmarking & Observability Suite

> **Self-Hosted PostgreSQL (AWS EC2 + Docker) vs. Managed AWS RDS PostgreSQL**  
> An end-to-end cloud database benchmarking, chaos engineering, scalability profiling, and financial TCO analysis platform.

---

## 📌 Executive Summary

**DB Ops Advisor** is a cloud database evaluation platform built to address the operational and financial trade-offs between **Self-Hosted PostgreSQL on EC2 (Docker-containerized)** and **Managed AWS RDS PostgreSQL**.

Unlike conventional black-box load testers, DB Ops Advisor features an integrated test harness: it synthesizes high-throughput transactional OLTP workloads, orchestrates automated chaos engineering experiments (container termination and RDS reboot/failover), measures real-time network Round-Trip Times (RTT), profiles percentile latency distributions (p50/p95/p99/max), and produces deterministic cloud architecture recommendations with exportable executive PDF reports.

---

## 🚀 Core Features & Modules

### 1. 💓 Live Infrastructure Health Monitor
- Persistent global telemetry header tracking connection status, ping, and database round-trip times (RTT) for both Self-Hosted EC2 and Managed RDS targets every 5 seconds.
- Instant visual indicator of node reachability, connection saturation, and network latency degradation.

### 2. ⚡ Live Benchmarking & Dual Telemetry
- **Side-by-Side Dual Execution:** Runs asynchronous concurrent benchmarks across EC2 and RDS simultaneously using `asyncio` worker pools.
- **Dynamic Workload Generation:** Configurable transaction counts, duration, and traffic profiles:
  - **Steady (OLTP Read/Write):** Uniform transaction pacing.
  - **Bursty (Traffic Spikes):** High-velocity stress testing simulating flash crowd events.
- **Real-Time Telemetry Cards:**
  - Throughput (Transactions Per Second - TPS)
  - Latency (95th Percentile Latency in ms)
  - Network RTT (True client-to-database TCP/SQL latency)
  - Error Rates & SQL Exception Tracking

### 3. 📊 Latency Distribution Profiler
- Microsecond-accurate latency breakdown comparing:
  - **p50 (Median Latency)**
  - **p95 (95th Percentile)**
  - **p99 (Tail Latency)**
  - **Max Latency**
- Visual bar comparison highlighting jitter, storage I/O stalls, and tail-latency outliers between self-hosted EBS volumes and managed RDS instances.

### 4. 💥 Chaos Engineering & Failure Simulator
- **EC2 Self-Hosted Chaos:** Issues instant container termination (`docker kill pg-selfhosted`) and tracks restart-to-recovery MTTR (Mean Time to Recovery) with automatic container recreation (`docker start`).
- **AWS RDS Failure:** Calls the AWS Boto3 API (`reboot_db_instance` with Multi-AZ force failover) and tracks DNS repointing and failover availability recovery timelines.
- Real-time animated stopwatch and live operational event logs.

### 5. 📈 Scalability Explorer (Concurrency Ramp)
- Automated multi-threaded concurrency sweep testing workloads from **1, 2, 4, 8, 16, up to 32 parallel threads**.
- Plots Concurrency vs. Throughput (TPS) and Concurrency vs. Latency curves.
- Identifies the exact saturation inflection point and connection lock contention thresholds for both architectures.

### 6. 📜 Benchmark History & PDF Executive Export
- Embedded SQLite storage (`benchmarks.db`) archiving all past test runs with workload parameters, timestamps, and performance metrics.
- Client-side vector PDF generation using `jsPDF` and `AutoTable` for executive reports, academic submissions, and infrastructure reviews.
- Instant historical run clearing and filtering.

### 7. ⚖️ Automated Decision Engine & Verdict Dashboard
- Deterministic multi-dimensional scoring rubric evaluating:
  - **Peak Throughput (TPS)**
  - **Latency Predictability**
  - **Failover & Recovery Speed (MTTR)**
  - **Operational Overhead & Risk**
- Generates clear, data-driven deployment recommendations (e.g., when to choose RDS vs. Self-Hosted EC2).

### 8. 💰 3-Year Total Cost of Ownership (TCO) Advisor
- Interactive financial modeling comparing:
  - **EC2:** Compute instances, gp3 EBS volumes, data transfer, and DevOps engineering overhead ($/hr).
  - **RDS:** Multi-AZ/Single-AZ database instances, provisioned IOPS/storage, automated backup snapshots, and managed SLAs.
- Dynamic 3-year TCO savings breakdown.

---

## 🏛️ System Architecture

```
                                  +---------------------------------------+
                                  |     Client Web Browser (Port 8000)     |
                                  | Chart.js / jsPDF / Glassmorphism UI   |
                                  +-------------------+-------------------+
                                                      |
                                           REST API / JSON
                                                      |
                                                      v
                                  +---------------------------------------+
                                  |        FastAPI Application Server     |
                                  |       (Uvicorn Daemon on AWS EC2)     |
                                  +---------+-------------------+---------+
                                            |                   |
                     Docker Engine / Localhost                  | AWS VPC / Network Peering
                                            |                   |
                                            v                   v
            +---------------------------------+       +---------------------------------+
            |     Self-Hosted PostgreSQL      |       |        Managed AWS RDS          |
            |   (Docker: postgres:15-alpine)  |       |   (PostgreSQL 15 Multi-AZ/Single)|
            |  Localhost:5432 on AWS EC2 gp3  |       |    AWS RDS Endpoint (Port 5432) |
            +---------------------------------+       +---------------------------------+
```

---

## 📂 Project Structure

```
dbops-advisor/
├── backend/
│   ├── main.py              # FastAPI server, REST API routes, SQLite persistence, test orchestrator
│   ├── db_ops.py            # Sysbench engine, Docker management, AWS RDS Boto3 failover
│   └── requirements.txt     # Python dependencies (FastAPI, uvicorn, psycopg2-binary, boto3)
├── frontend/
│   └── index.html           # Single-page application (HTML5, Vanilla CSS, Chart.js, jsPDF)
├── .gitignore               # Ignores venv, caches, environment configs, and .pem keys
└── README.md                # Comprehensive documentation
```

---

## 🔌 API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/` | `GET` | Serves the single-page dashboard UI |
| `/api/health-check` | `GET` | Health status and live network RTT (ms) for EC2 and RDS |
| `/api/run-test` | `POST` | Executes benchmark on a selected target (`ec2` or `rds`) |
| `/api/run-simultaneous` | `POST` | Concurrently executes benchmarks on both targets in parallel |
| `/api/scalability-test` | `POST` | Runs a multi-thread concurrency ramp test (1 to 32 threads) |
| `/api/fail/{target}` | `POST` | Injects chaos failure (`docker kill` or RDS reboot/failover) |
| `/api/verdict` | `POST` | Computes architecture comparison matrix and decision score |
| `/api/history` | `GET` | Returns list of historical benchmark runs from SQLite |
| `/api/history/clear` | `POST` | Clears all stored benchmark history runs |

---

## 🛠️ Installation & Deployment (AWS EC2)

### 1. Prerequisites on EC2 Instance
Ensure your Ubuntu EC2 instance (`t2.micro` or `t3.medium`) has Docker, Python 3, sysbench, and AWS CLI installed:

```bash
sudo apt update && sudo apt install -y docker.io python3-pip sysbench awscli
sudo usermod -aG docker $USER
```

### 2. Environment Configuration
Configure AWS credentials for RDS failover permissions:
```bash
aws configure
```

Set required environment variables:
```bash
export EC2_HOST="localhost"
export EC2_PG_PASSWORD="YourEC2Password"
export RDS_HOST="your-rds-endpoint.ap-south-1.rds.amazonaws.com"
export RDS_PG_PASSWORD="YourRDSPassword"
export RDS_INSTANCE_ID="your-rds-instance-id"
export AWS_REGION="ap-south-1"
```

### 3. Install Python Dependencies
```bash
cd backend
pip3 install -r requirements.txt
```

### 4. Running the Application

**Direct Execution:**
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

**Running as a Systemd Service (`/etc/systemd/system/dbops.service`):**
```ini
[Unit]
Description=DB Ops Advisor Service
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/db-ops-advisor
Environment="PATH=/home/ubuntu/.local/bin:/usr/bin"
EnvironmentFile=/home/ubuntu/db-ops-advisor/.env
ExecStart=/usr/local/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl daemon-reload
sudo systemctl enable dbops
sudo systemctl start dbops
```

---

## 💻 Tech Stack

- **Backend:** Python 3.10+, FastAPI, Uvicorn, SQLite3, Psycopg2, Boto3, Subprocess/Sysbench
- **Frontend:** Vanilla HTML5 / CSS3 (Datadog/AWS Dark Console Glassmorphic theme), JavaScript (ES6+), Chart.js (CDN), jsPDF & autoTable (CDN)
- **Cloud & Infrastructure:** AWS EC2 (Ubuntu 24.04 LTS), AWS RDS (PostgreSQL Engine), Docker CE
