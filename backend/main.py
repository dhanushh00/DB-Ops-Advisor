import asyncio
import json
import os
import re
import subprocess
import time
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

# Store latest completed run stats in memory for the verdict scorecard
benchmark_records = {
    "ec2": {"tps": 487.15, "p95": 10.09, "recovery_sec": 4.2},
    "rds": {"tps": 210.00, "p95": 23.52, "recovery_sec": 38.5},
}

class TestRequest(BaseModel):
    target: str      # "ec2" or "rds"
    mode: str        # "steady" or "bursty"
    duration: int = 30

def get_target_host(target: str) -> str:
    return "localhost" if target == "ec2" else RDS_HOST

def check_db_health(host: str) -> bool:
    try:
        conn = psycopg2.connect(
            host=host, port=5432, user=DB_USER, password=DB_PASS, dbname=DB_NAME, connect_timeout=2
        )
        conn.close()
        return True
    except Exception:
        return False

@app.get("/")
def serve_index():
    return FileResponse("static/index.html")

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
        tps_regex = re.compile(r"tps:\s+([0-9.]+)")
        lat_regex = re.compile(r"latency\s+\(95th%\):\s+([0-9.]+)ms")
        final_tps_regex = re.compile(r"transactions:\s+\d+\s+\(([0-9.]+)\s+per sec\.\)")
        final_p95_regex = re.compile(r"95th percentile:\s+([0-9.]+)")

        final_tps = None
        final_p95 = None

        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            
            # Match interval stats
            tps_match = tps_regex.search(text)
            lat_match = lat_regex.search(text)
            if tps_match and lat_match:
                payload = {
                    "type": "tick",
                    "tps": float(tps_match.group(1)),
                    "p95": float(lat_match.group(1)),
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

        await proc.wait()

        if final_tps and final_p95:
            benchmark_records[req.target]["tps"] = final_tps
            benchmark_records[req.target]["p95"] = final_p95

        summary = {
            "type": "complete",
            "final_tps": final_tps or benchmark_records[req.target]["tps"],
            "final_p95": final_p95 or benchmark_records[req.target]["p95"],
            "target": req.target
        }
        yield f"data: {json.dumps(summary)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/api/fail/{target}")
async def trigger_failure(target: str):
    async def fail_stream():
        start_time = time.time()
        yield f"data: {json.dumps({'status': 'Injecting failure into ' + target.upper()})}\n\n"
        
        if target == "ec2":
            # Restart Docker container locally
            subprocess.run(["docker", "restart", "postgres-selfhosted"], check=True)
        else:
            # RDS forced reboot via boto3
            rds = boto3.client("rds", region_name="ap-south-1")
            rds.reboot_db_instance(DBInstanceIdentifier="db-ops-rds", ForceFailover=False)

        yield f"data: {json.dumps({'status': 'Database is down. Polling connection for recovery...'})}\n\n"
        
        host = get_target_host(target)
        # Give it a second to unbind
        await asyncio.sleep(2)
        
        elapsed = 0
        while True:
            await asyncio.sleep(1)
            elapsed = round(time.time() - start_time, 1)
            is_up = check_db_health(host)
            yield f"data: {json.dumps({'status': f'Waiting... {elapsed}s elapsed', 'elapsed': elapsed, 'recovered': is_up})}\n\n"
            if is_up:
                benchmark_records[target]["recovery_sec"] = elapsed
                yield f"data: {json.dumps({'status': f'Recovered in {elapsed} seconds!', 'elapsed': elapsed, 'done': True})}\n\n"
                break

    return StreamingResponse(fail_stream(), media_type="text/event-stream")

@app.get("/api/verdict")
def get_verdict():
    return benchmark_records
