"""
db_ops.py — wraps sysbench, Docker, and AWS RDS calls.
This is the "engine" behind every button in the app: it generates traffic
itself (via sysbench) and injects failures itself (via docker / boto3) —
no third-party load-testing or chaos-engineering service involved.
"""
import subprocess
import time
import re
import boto3
import os

# ---- Config (edit these, or set as environment variables on your EC2 box) ----
EC2_HOST = os.getenv("EC2_HOST", "localhost")
EC2_PASSWORD = os.getenv("EC2_PG_PASSWORD", "test123")
RDS_HOST = os.getenv("RDS_HOST", "")           # e.g. pg-managed.xxxx.ap-south-1.rds.amazonaws.com
RDS_PASSWORD = os.getenv("RDS_PG_PASSWORD", "")
RDS_INSTANCE_ID = os.getenv("RDS_INSTANCE_ID", "pg-managed")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")


def _target_config(target: str):
    """Resolve 'ec2' or 'rds' into (host, password)."""
    if target == "ec2":
        return EC2_HOST, EC2_PASSWORD
    elif target == "rds":
        return RDS_HOST, RDS_PASSWORD
    raise ValueError("target must be 'ec2' or 'rds'")


def _run_sysbench(host, password, threads, duration, mode="run"):
    """Runs one sysbench invocation and returns raw stdout."""
    cmd = [
        "sysbench", "oltp_read_write",
        f"--db-driver=pgsql",
        f"--pgsql-host={host}",
        f"--pgsql-port=5432",
        f"--pgsql-user=postgres",
        f"--pgsql-password={password}",
        f"--pgsql-db=postgres",
        f"--tables=4",
        f"--table-size=10000",
        f"--threads={threads}",
        f"--time={duration}",
        mode,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + 30)
    return result.stdout + result.stderr


def _parse_sysbench(output: str):
    """Pulls TPS, avg latency, p95 latency out of raw sysbench text."""
    def grab(pattern, cast=float):
        m = re.search(pattern, output)
        return cast(m.group(1)) if m else None

    return {
        "tps": grab(r"transactions:\s+\d+\s+\(([\d.]+) per sec\.\)"),
        "avg_latency_ms": grab(r"avg:\s+([\d.]+)"),
        "p95_latency_ms": grab(r"95th percentile:\s+([\d.]+)"),
        "max_latency_ms": grab(r"max:\s+([\d.]+)"),
    }


def prepare_target(target: str):
    """Run once per target before benchmarking — creates test tables."""
    host, password = _target_config(target)
    output = _run_sysbench(host, password, threads=4, duration=0, mode="prepare")
    return {"target": target, "raw": output}


def run_baseline(target: str, threads: int = 4, duration: int = 60):
    """Steady-load benchmark — this is your Review 1 baseline, now callable via API."""
    host, password = _target_config(target)
    raw = _run_sysbench(host, password, threads, duration, mode="run")
    metrics = _parse_sysbench(raw)
    return {"target": target, "pattern": "steady", "threads": threads, "duration": duration,
            "metrics": metrics, "raw": raw}


def run_bursty(target: str, cycles: int = 3, normal_threads: int = 2,
               burst_threads: int = 10, normal_seconds: int = 20, burst_seconds: int = 10):
    """
    Alternates low load and high load to simulate a real bursty traffic pattern.
    Returns one result per phase so the frontend can plot latency over time.
    """
    host, password = _target_config(target)
    timeline = []
    for cycle in range(cycles):
        normal_raw = _run_sysbench(host, password, normal_threads, normal_seconds, mode="run")
        timeline.append({
            "phase": "normal", "cycle": cycle,
            "metrics": _parse_sysbench(normal_raw)
        })
        burst_raw = _run_sysbench(host, password, burst_threads, burst_seconds, mode="run")
        timeline.append({
            "phase": "burst", "cycle": cycle,
            "metrics": _parse_sysbench(burst_raw)
        })
    return {"target": target, "pattern": "bursty", "cycles": cycles, "timeline": timeline}


def inject_failure_ec2(container_name: str = "pg-test", poll_interval: float = 1.0, timeout: int = 120):
    """Kills the self-hosted Postgres container and times recovery."""
    subprocess.run(["docker", "kill", container_name], capture_output=True, text=True)
    subprocess.run(["docker", "start", container_name], capture_output=True, text=True)
    start = time.time()
    elapsed = 0
    while elapsed < timeout:
        check = subprocess.run(
            ["docker", "exec", container_name, "pg_isready", "-U", "postgres"],
            capture_output=True, text=True
        )
        if check.returncode == 0:
            return {"target": "ec2", "recovery_seconds": round(time.time() - start, 2)}
        time.sleep(poll_interval)
        elapsed = time.time() - start
    return {"target": "ec2", "recovery_seconds": None, "error": "timed out waiting for recovery"}


def inject_failure_rds(poll_interval: float = 2.0, timeout: int = 300):
    """Triggers an RDS Multi-AZ failover and times recovery via boto3."""
    client = boto3.client("rds", region_name=AWS_REGION)
    start = time.time()
    client.reboot_db_instance(DBInstanceIdentifier=RDS_INSTANCE_ID, ForceFailover=True)

    elapsed = 0
    while elapsed < timeout:
        desc = client.describe_db_instances(DBInstanceIdentifier=RDS_INSTANCE_ID)
        status = desc["DBInstances"][0]["DBInstanceStatus"]
        if status == "available":
            # confirm it actually accepts connections, not just "available" in the API
            check = subprocess.run(
                ["pg_isready", "-h", RDS_HOST, "-p", "5432", "-U", "postgres"],
                capture_output=True, text=True
            )
            if check.returncode == 0:
                return {"target": "rds", "recovery_seconds": round(time.time() - start, 2)}
        time.sleep(poll_interval)
        elapsed = time.time() - start
    return {"target": "rds", "recovery_seconds": None, "error": "timed out waiting for recovery"}


def build_verdict(ec2_baseline: dict, rds_baseline: dict, ec2_bursty: dict, rds_bursty: dict,
                   ec2_recovery: float, rds_recovery: float,
                   ec2_admin_hours: float, rds_admin_hours: float,
                   hourly_rate: float, ec2_monthly_cost: float, rds_monthly_cost: float, years: int = 3):
    """
    Combines every measured dimension into a side-by-side verdict —
    no single blended score, each dimension gets its own honest winner.
    """
    def avg_p95(bursty_result):
        vals = [p["metrics"]["p95_latency_ms"] for p in bursty_result["timeline"]
                if p["phase"] == "burst" and p["metrics"]["p95_latency_ms"]]
        return sum(vals) / len(vals) if vals else None

    ec2_tco = ec2_monthly_cost * 12 * years + ec2_admin_hours * hourly_rate * 12 * years
    rds_tco = rds_monthly_cost * 12 * years + rds_admin_hours * hourly_rate * 12 * years

    rows = [
        {
            "dimension": "Steady-State Throughput",
            "ec2": ec2_baseline["metrics"]["tps"],
            "rds": rds_baseline["metrics"]["tps"],
            "unit": "TPS",
            "winner": "ec2" if ec2_baseline["metrics"]["tps"] > rds_baseline["metrics"]["tps"] else "rds",
        },
        {
            "dimension": "Traffic Handling (burst p95 latency)",
            "ec2": avg_p95(ec2_bursty),
            "rds": avg_p95(rds_bursty),
            "unit": "ms (lower is better)",
            "winner": "ec2" if avg_p95(ec2_bursty) < avg_p95(rds_bursty) else "rds",
        },
        {
            "dimension": "Recovery Speed",
            "ec2": ec2_recovery,
            "rds": rds_recovery,
            "unit": "seconds (lower is better)",
            "winner": "ec2" if ec2_recovery < rds_recovery else "rds",
        },
        {
            "dimension": "Manageability (admin hours logged)",
            "ec2": ec2_admin_hours,
            "rds": rds_admin_hours,
            "unit": "hrs/month (lower is better)",
            "winner": "ec2" if ec2_admin_hours < rds_admin_hours else "rds",
        },
        {
            "dimension": f"{years}-Year Total Cost of Ownership",
            "ec2": round(ec2_tco, 2),
            "rds": round(rds_tco, 2),
            "unit": "$ (lower is better)",
            "winner": "ec2" if ec2_tco < rds_tco else "rds",
        },
    ]
    return {"rows": rows, "hourly_rate_assumed": hourly_rate, "years": years}
