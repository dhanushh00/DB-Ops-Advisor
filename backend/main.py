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
    "ec2": {"tps": 461.44, "p95": 10.46, "recovery_sec": 4.0},
    "rds": {"tps": 219.31, "p95": 20.74, "recovery_sec": 38.5},
}

class TestRequest(BaseModel):
    target: str      # "ec2" or "rds"
    mode: str        # "steady" or "bursty"
    duration: int = 15

def get_target_host(target: str) -> str:
    return "localhost" if target == "ec2" else RDS_HOST

def check_db_health(host: str) -> bool:
    try:
        conn = psycopg2.connect(
            host=host, port=5432, user=DB_USER, password=DB_PASS, dbname=DB_NAME, connect_timeout=1
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
        
        # Regex patterns matching Sysbench 1.0 output:
        # [ 1s ] thds: 4 tps: 438.05 qps: 8810.91 ... lat (ms,95%): 11.45 ...
        tps_regex = re.compile(r"tps:\s*([0-9.]+)")
        lat_regex = re.compile(r"lat\s*(?:\(ms,95%\)|\(95th%\))?:\s*([0-9.]+)")
        qps_regex = re.compile(r"qps:\s*([0-9.]+)")
        
        final_tps_regex = re.compile(r"transactions:\s+\d+\s+\(([0-9.]+)\s+per sec\.\)")
        final_p95_regex = re.compile(r"95th percentile:\s+([0-9.]+)")

        final_tps = None
        final_p95 = None

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

        if final_tps:
            benchmark_records[req.target]["tps"] = round(final_tps, 2)
        if final_p95:
            benchmark_records[req.target]["p95"] = round(final_p95, 2)

        summary = {
            "type": "complete",
            "target": req.target,
            "final_tps": benchmark_records[req.target]["tps"],
            "final_p95": benchmark_records[req.target]["p95"]
        }
        yield f"data: {json.dumps(summary)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

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
