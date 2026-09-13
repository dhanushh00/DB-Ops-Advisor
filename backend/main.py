import asyncio
import json
import os
import re
import subprocess
import time
import datetime
import sqlite3
import boto3
import psycopg2
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

app = FastAPI(title="DB Ops Advisor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

RDS_HOST = "db-ops-rds.cd0y8g4e2j77.ap-south-1.rds.amazonaws.com"
DB_USER = "dbopsuser"
DB_PASS = "dbopspassword"
DB_NAME = "benchmarks"
DB_PATH = os.path.join(os.path.dirname(__file__), "benchmark_history.db")

# Store latest completed run stats in memory for the verdict scorecard
benchmark_records = {
    "ec2": {"tps": 461.44, "p95": 10.46, "recovery_sec": 4.0},
    "rds": {"tps": 219.31, "p95": 20.74, "recovery_sec": 38.5},
}

# ---------------------------------------------------------------------------
# Feature 1: SQLite History Database
# ---------------------------------------------------------------------------
def init_db():
    """Create the benchmark_history table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            target TEXT NOT NULL,
            mode TEXT NOT NULL,
            duration INTEGER,
            threads INTEGER,
            final_tps REAL,
            final_p95 REAL,
            final_qps REAL,
            avg_latency REAL,
            p99_latency REAL,
            max_latency REAL,
            network_rtt_ms REAL
        )
    """)
    conn.commit()
    conn.close()

def save_to_history(target, mode, duration, threads, tps, p95, qps=None,
                    avg_lat=None, p99_lat=None, max_lat=None, rtt=None):
    """Insert a completed benchmark record into SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO benchmark_history
        (timestamp, target, mode, duration, threads, final_tps, final_p95,
         final_qps, avg_latency, p99_latency, max_latency, network_rtt_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.datetime.utcnow().isoformat() + "Z",
        target, mode, duration, threads, tps, p95, qps,
        avg_lat, p99_lat, max_lat, rtt
    ))
    conn.commit()
    conn.close()


class TestRequest(BaseModel):
    target: str      # "ec2" or "rds"
    mode: str        # "steady" or "bursty"
    duration: int = 15

class SimultaneousRequest(BaseModel):
    mode: str = "steady"
    duration: int = 15

class ScalabilityRequest(BaseModel):
    target: str      # "ec2" or "rds"
    duration: int = 5


def get_target_host(target: str) -> str:
    return "localhost" if target == "ec2" else RDS_HOST


# ---------------------------------------------------------------------------
# Feature 4: Network RTT measurement
# ---------------------------------------------------------------------------
def measure_network_rtt(host: str) -> float:
    """Measure a SELECT 1 round-trip to the database in milliseconds."""
    try:
        start = time.perf_counter()
        conn = psycopg2.connect(
            host=host, port=5432, user=DB_USER, password=DB_PASS,
            dbname=DB_NAME, connect_timeout=2
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        return round((time.perf_counter() - start) * 1000, 2)
    except Exception:
        return -1


def check_db_health(host: str) -> bool:
    try:
        conn = psycopg2.connect(
            host=host, port=5432, user=DB_USER, password=DB_PASS, dbname=DB_NAME, connect_timeout=1
        )
        conn.close()
        return True
    except Exception:
        return False


@app.on_event("startup")
def on_startup():
    init_db()


@app.get("/")
def serve_index():
    return FileResponse("static/index.html")


# ---------------------------------------------------------------------------
# Feature 4: Live Health Monitor endpoint
# ---------------------------------------------------------------------------
@app.get("/api/health-check")
def health_check():
    """Returns live health + RTT for both database targets."""
    ec2_rtt = measure_network_rtt("localhost")
    rds_rtt = measure_network_rtt(RDS_HOST)
    return {
        "ec2": {"alive": ec2_rtt >= 0, "latency_ms": ec2_rtt if ec2_rtt >= 0 else None},
        "rds": {"alive": rds_rtt >= 0, "latency_ms": rds_rtt if rds_rtt >= 0 else None},
    }


# ---------------------------------------------------------------------------
# Existing Run Test endpoint — enhanced with RTT + latency distribution
# ---------------------------------------------------------------------------
@app.post("/api/run-test")
async def run_test(req: TestRequest):
    host = get_target_host(req.target)
    threads = 4 if req.mode == "steady" else 16
    rate = 0 if req.mode == "steady" else 150

    cmd = [
        "sysbench",
        "--db-driver=pgsql",
        f"--pgsql-host={host}",
        "--pgsql-port=5432",
        f"--pgsql-user={DB_USER}",
        f"--pgsql-password={DB_PASS}",
        f"--pgsql-db={DB_NAME}",
        "--tables=4",
        "--table-size=25000",
        f"--threads={threads}",
        f"--time={req.duration}",
        "--report-interval=1",
        "--percentile=99",
        "oltp_read_write",
        "run"
    ]
    if rate > 0:
        cmd.insert(-2, f"--rate={rate}")

    async def event_generator():
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        
        # Regex patterns matching Sysbench 1.0 output:
        # [ 1s ] thds: 4 tps: 438.05 qps: 8810.91 ... lat (ms,95%): 11.45 ...
        tps_regex = re.compile(r"tps:\s*([0-9.]+)")
        lat_regex = re.compile(r"lat\s*(?:\(ms,95%\)|\(95th%\))?:\s*([0-9.]+)")
        qps_regex = re.compile(r"qps:\s*([0-9.]+)")
        
        final_tps_regex = re.compile(r"transactions:\s+\d+\s+\(([0-9.]+)\s+per sec\.\)")
        final_p95_regex = re.compile(r"95th percentile:\s+([0-9.]+)")
        final_avg_regex = re.compile(r"avg:\s+([0-9.]+)")
        final_p99_regex = re.compile(r"99th percentile:\s+([0-9.]+)")
        final_max_regex = re.compile(r"max:\s+([0-9.]+)")
        final_qps_regex = re.compile(r"queries:\s+\d+\s+\(([0-9.]+)\s+per sec\.\)")

        final_tps = None
        final_p95 = None
        final_avg = None
        final_p99 = None
        final_max = None
        final_qps = None

        # Measure network RTT once at start
        rtt = measure_network_rtt(host)

        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            
            tps_match = tps_regex.search(text)
            lat_match = lat_regex.search(text)
            qps_match = qps_regex.search(text)

            if tps_match:
                tps_val = float(tps_match.group(1))
                lat_val = float(lat_match.group(1)) if lat_match else 0.0
                qps_val = float(qps_match.group(1)) if qps_match else 0.0

                payload = {
                    "type": "tick",
                    "target": req.target,
                    "tps": tps_val,
                    "p95": lat_val,
                    "qps": qps_val,
                    "network_rtt_ms": rtt,
                    "raw": text
                }
                yield f"data: {json.dumps(payload)}\n\n"

            # Match final summary stats
            ft = final_tps_regex.search(text)
            if ft:
                final_tps = float(ft.group(1))
            fp = final_p95_regex.search(text)
            if fp:
                final_p95 = float(fp.group(1))
            fa = final_avg_regex.search(text)
            if fa:
                final_avg = float(fa.group(1))
            f99 = final_p99_regex.search(text)
            if f99:
                final_p99 = float(f99.group(1))
            fm = final_max_regex.search(text)
            if fm:
                final_max = float(fm.group(1))
            fq = final_qps_regex.search(text)
            if fq:
                final_qps = float(fq.group(1))

        await proc.wait()

        if final_tps:
            benchmark_records[req.target]["tps"] = round(final_tps, 2)
        if final_p95:
            benchmark_records[req.target]["p95"] = round(final_p95, 2)

        # Save to history (Feature 1)
        save_to_history(
            target=req.target, mode=req.mode, duration=req.duration,
            threads=threads, tps=final_tps, p95=final_p95, qps=final_qps,
            avg_lat=final_avg, p99_lat=final_p99, max_lat=final_max, rtt=rtt
        )

        summary = {
            "type": "complete",
            "target": req.target,
            "final_tps": benchmark_records[req.target]["tps"],
            "final_p95": benchmark_records[req.target]["p95"],
            "avg_latency": final_avg,
            "p99_latency": final_p99,
            "max_latency": final_max,
            "final_qps": final_qps,
            "network_rtt_ms": rtt,
        }
        yield f"data: {json.dumps(summary)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Feature 2: True Simultaneous Side-by-Side endpoint
# ---------------------------------------------------------------------------
@app.post("/api/run-simultaneous")
async def run_simultaneous(req: SimultaneousRequest):
    """Run sysbench on BOTH EC2 and RDS concurrently, interleaving SSE events."""
    threads = 4 if req.mode == "steady" else 16
    rate = 0 if req.mode == "steady" else 150

    def build_cmd(host):
        cmd = [
            "sysbench",
            "--db-driver=pgsql",
            f"--pgsql-host={host}",
            "--pgsql-port=5432",
            f"--pgsql-user={DB_USER}",
            f"--pgsql-password={DB_PASS}",
            f"--pgsql-db={DB_NAME}",
            "--tables=4",
            "--table-size=25000",
            f"--threads={threads}",
            f"--time={req.duration}",
            "--report-interval=1",
            "--percentile=99",
            "oltp_read_write",
            "run"
        ]
        if rate > 0:
            cmd.insert(-2, f"--rate={rate}")
        return cmd

    async def event_generator():
        ec2_cmd = build_cmd("localhost")
        rds_cmd = build_cmd(RDS_HOST)

        ec2_proc = await asyncio.create_subprocess_exec(
            *ec2_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        rds_proc = await asyncio.create_subprocess_exec(
            *rds_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )

        tps_regex = re.compile(r"tps:\s*([0-9.]+)")
        lat_regex = re.compile(r"lat\s*(?:\(ms,95%\)|\(95th%\))?:\s*([0-9.]+)")
        qps_regex = re.compile(r"qps:\s*([0-9.]+)")
        final_tps_regex = re.compile(r"transactions:\s+\d+\s+\(([0-9.]+)\s+per sec\.\)")
        final_p95_regex = re.compile(r"95th percentile:\s+([0-9.]+)")
        final_avg_regex = re.compile(r"avg:\s+([0-9.]+)")
        final_p99_regex = re.compile(r"99th percentile:\s+([0-9.]+)")
        final_max_regex = re.compile(r"max:\s+([0-9.]+)")
        final_qps_regex = re.compile(r"queries:\s+\d+\s+\(([0-9.]+)\s+per sec\.\)")

        ec2_rtt = measure_network_rtt("localhost")
        rds_rtt = measure_network_rtt(RDS_HOST)

        yield f"data: {json.dumps({'type': 'init', 'ec2_rtt': ec2_rtt, 'rds_rtt': rds_rtt})}\n\n"

        async def read_stream(proc, target, rtt):
            finals = {}
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                tps_m = tps_regex.search(text)
                lat_m = lat_regex.search(text)
                qps_m = qps_regex.search(text)

                if tps_m:
                    yield json.dumps({
                        "type": "tick",
                        "target": target,
                        "tps": float(tps_m.group(1)),
                        "p95": float(lat_m.group(1)) if lat_m else 0.0,
                        "qps": float(qps_m.group(1)) if qps_m else 0.0,
                        "network_rtt_ms": rtt,
                        "raw": text
                    })

                ft = final_tps_regex.search(text)
                if ft: finals["tps"] = float(ft.group(1))
                fp = final_p95_regex.search(text)
                if fp: finals["p95"] = float(fp.group(1))
                fa = final_avg_regex.search(text)
                if fa: finals["avg"] = float(fa.group(1))
                f99 = final_p99_regex.search(text)
                if f99: finals["p99"] = float(f99.group(1))
                fm = final_max_regex.search(text)
                if fm: finals["max"] = float(fm.group(1))
                fq = final_qps_regex.search(text)
                if fq: finals["qps"] = float(fq.group(1))

            await proc.wait()
            if "tps" in finals:
                benchmark_records[target]["tps"] = round(finals["tps"], 2)
            if "p95" in finals:
                benchmark_records[target]["p95"] = round(finals["p95"], 2)

            save_to_history(
                target=target, mode=req.mode, duration=req.duration,
                threads=threads, tps=finals.get("tps"), p95=finals.get("p95"),
                qps=finals.get("qps"), avg_lat=finals.get("avg"),
                p99_lat=finals.get("p99"), max_lat=finals.get("max"), rtt=rtt
            )

            yield json.dumps({
                "type": "complete",
                "target": target,
                "final_tps": finals.get("tps", 0),
                "final_p95": finals.get("p95", 0),
                "avg_latency": finals.get("avg"),
                "p99_latency": finals.get("p99"),
                "max_latency": finals.get("max"),
                "final_qps": finals.get("qps"),
                "network_rtt_ms": rtt,
            })

        # Interleave both streams using asyncio queues
        queue = asyncio.Queue()

        async def enqueue(stream):
            async for item in stream:
                await queue.put(item)

        ec2_task = asyncio.create_task(enqueue(read_stream(ec2_proc, "ec2", ec2_rtt)))
        rds_task = asyncio.create_task(enqueue(read_stream(rds_proc, "rds", rds_rtt)))

        done_count = 0
        while done_count < 2:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=0.5)
                yield f"data: {item}\n\n"
                parsed = json.loads(item)
                if parsed.get("type") == "complete":
                    done_count += 1
            except asyncio.TimeoutError:
                if ec2_task.done() and rds_task.done() and queue.empty():
                    break

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Feature 3: Scalability Explorer endpoint
# ---------------------------------------------------------------------------
@app.post("/api/scalability-test")
async def scalability_test(req: ScalabilityRequest):
    """Ramp thread counts [1, 2, 4, 8, 16, 32] and report TPS for each."""
    host = get_target_host(req.target)
    thread_counts = [1, 2, 4, 8, 16, 32]

    async def event_generator():
        for threads in thread_counts:
            yield f"data: {json.dumps({'type': 'status', 'message': f'Testing {threads} thread(s) on {req.target.upper()}...'})}\n\n"

            cmd = [
                "sysbench",
                "--db-driver=pgsql",
                f"--pgsql-host={host}",
                "--pgsql-port=5432",
                f"--pgsql-user={DB_USER}",
                f"--pgsql-password={DB_PASS}",
                f"--pgsql-db={DB_NAME}",
                "--tables=4",
                "--table-size=25000",
                f"--threads={threads}",
                f"--time={req.duration}",
                "--percentile=99",
                "oltp_read_write",
                "run"
            ]

            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )

            output = await proc.stdout.read()
            await proc.wait()
            text = output.decode("utf-8", errors="replace")

            tps_m = re.search(r"transactions:\s+\d+\s+\(([0-9.]+)\s+per sec\.\)", text)
            p95_m = re.search(r"95th percentile:\s+([0-9.]+)", text)
            p99_m = re.search(r"99th percentile:\s+([0-9.]+)", text)
            avg_m = re.search(r"avg:\s+([0-9.]+)", text)

            tps = float(tps_m.group(1)) if tps_m else 0
            p95 = float(p95_m.group(1)) if p95_m else 0
            p99 = float(p99_m.group(1)) if p99_m else 0
            avg_lat = float(avg_m.group(1)) if avg_m else 0

            payload = {
                "type": "scale-point",
                "target": req.target,
                "threads": threads,
                "tps": round(tps, 2),
                "p95": round(p95, 2),
                "p99": round(p99, 2),
                "avg_latency": round(avg_lat, 2),
            }
            yield f"data: {json.dumps(payload)}\n\n"

        yield f"data: {json.dumps({'type': 'scale-done', 'target': req.target})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Existing Failure Simulator (unchanged)
# ---------------------------------------------------------------------------
@app.post("/api/fail/{target}")
async def trigger_failure(target: str):
    async def fail_stream():
        start_time = time.time()
        yield f"data: {json.dumps({'status': f'Triggering failure on {target.upper()}...'})}\n\n"
        
        host = get_target_host(target)
        
        if target == "ec2":
            try:
                subprocess.run(["docker", "restart", "postgres-selfhosted"], check=True)
                yield f"data: {json.dumps({'status': 'Docker container killed & restarting...'})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'status': f'Docker command error: {str(e)}'})}\n\n"
        else:
            try:
                rds = boto3.client("rds", region_name="ap-south-1")
                rds.reboot_db_instance(DBInstanceIdentifier="db-ops-rds", ForceFailover=False)
                yield f"data: {json.dumps({'status': 'AWS RDS reboot API called. Waiting for endpoint reboot...'})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'status': f'RDS Notice: {str(e)} - Monitoring recovery...'})}\n\n"

        await asyncio.sleep(2)
        yield f"data: {json.dumps({'status': 'Database is down. Polling connection for recovery...'})}\n\n"
        
        recovered = False
        for _ in range(120):
            await asyncio.sleep(1)
            elapsed = round(time.time() - start_time, 1)
            is_up = check_db_health(host)
            yield f"data: {json.dumps({'status': f'Waiting... {elapsed}s elapsed', 'elapsed': elapsed, 'recovered': is_up, 'target': target})}\n\n"
            if is_up:
                recovered = True
                benchmark_records[target]["recovery_sec"] = elapsed
                yield f"data: {json.dumps({'status': f'Recovered in {elapsed} seconds!', 'elapsed': elapsed, 'done': True, 'target': target})}\n\n"
                break

        if not recovered:
            yield f"data: {json.dumps({'status': 'Recovery polling timed out after 120s', 'elapsed': 120.0, 'done': True, 'target': target})}\n\n"

    return StreamingResponse(fail_stream(), media_type="text/event-stream")


@app.get("/api/verdict")
def get_verdict():
    return benchmark_records


# ---------------------------------------------------------------------------
# Feature 1: History endpoints
# ---------------------------------------------------------------------------
@app.get("/api/history")
def get_history():
    """Return all benchmark history records as JSON."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM benchmark_history ORDER BY id DESC LIMIT 100").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/history/clear")
def clear_history():
    """Delete all benchmark history records."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM benchmark_history")
    conn.commit()
    conn.close()
    return {"status": "cleared"}
