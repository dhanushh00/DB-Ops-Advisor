# DB Ops Advisor

A self-contained web app that benchmarks self-hosted PostgreSQL (EC2 + Docker)
against managed PostgreSQL (AWS RDS) — it generates its own traffic (via
sysbench) and injects its own failures (via `docker kill` / RDS force-failover)
with no third-party load-testing or chaos-engineering tool involved.

## Where this must run

**On your EC2 instance** (`pg-selfhosted`) — because it needs:
- `sysbench` installed (you already have this)
- `docker` access (you already have this)
- Network access to both `localhost:5432` (self-hosted) and your RDS endpoint
- AWS credentials configured (for the RDS failover call)

## Setup (run these on your EC2 instance, via PuTTY)

1. Copy this whole `dbops-advisor` folder onto your EC2 instance (e.g. using `scp`,
   or just re-create the files there with `nano`/`vim` — it's only 3 files).

2. Install Python dependencies:
   ```
   cd dbops-advisor/backend
   pip3 install -r requirements.txt
   ```

3. Configure AWS credentials on the EC2 instance (needed for the RDS failover button):
   ```
   aws configure
   ```
   (Enter your AWS access key, secret key, and region `ap-south-1`. If `aws` CLI
   isn't installed: `sudo apt install -y awscli`.)

4. Set your environment variables (edit these with your real values):
   ```
   export EC2_HOST=localhost
   export EC2_PG_PASSWORD=test123
   export RDS_HOST=pg-managed.cd0y8g4e2j77.ap-south-1.rds.amazonaws.com
   export RDS_PG_PASSWORD=test1234!
   export RDS_INSTANCE_ID=pg-managed
   export AWS_REGION=ap-south-1
   ```

5. Run the server:
   ```
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

6. Open a browser to `http://<YOUR_EC2_PUBLIC_IP>:8000` — you'll need to add an
   inbound security group rule allowing port 8000 from "My IP" (same way you
   opened port 22 and 5432 earlier).

## Using it

- **Run Test tab** — pick EC2 or RDS, pick steady or bursty, click Start. Watch
  the live TPS/latency numbers and chart.
- **Failure Simulator tab** — click "Inject Failure" for either target, watch
  the recovery timer.
- **Verdict Dashboard tab** — currently a placeholder table; wire it up (see
  "Next step" below) once you've collected real numbers from the two tabs above.
- **Cost Advisor tab** — fully working standalone; adjust the sliders/inputs and
  see the 3-year TCO comparison update live.

## Next step (Review 2/3 work)

The `/api/verdict` endpoint and `build_verdict()` function in `db_ops.py` are
ready — they just need the frontend's "Generate Verdict" button wired to
actually POST your collected baseline/bursty/recovery numbers instead of the
placeholder alert. That's a small next step once you've run a few real tests
through the app and want to see the full comparison table populate.

## Files

- `backend/main.py` — FastAPI app, defines all `/api/...` routes
- `backend/db_ops.py` — the actual engine: sysbench wrapper, Docker kill,
  RDS failover via boto3, and the verdict-scoring logic
- `backend/requirements.txt` — Python dependencies
- `frontend/index.html` — the whole UI (single file, Chart.js from CDN, no build step)
ssh -i "$HOME\.ssh\db-ops-key.pem" ubuntu@13.233.155.21
