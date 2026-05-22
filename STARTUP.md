# EC22 — Startup & Operations Guide

---

## Prerequisites

- Docker and Docker Compose installed on both machines
- EC21's MySQL must be running before starting EC22
- Ports available on the EC22 host: `8080` (Airflow), `9090` (Spark Master UI), `7077` (Spark), `8082` (Spark Worker UI), `5432` (PostgreSQL)

---

## First-Time Setup

### Step 1 — Start EC21's MySQL only

On the EC21 machine (or locally):

```bash
cd EC21
docker compose up mysql -d
```

Wait until it is healthy:

```bash
docker compose ps mysql
# STATUS should show: healthy
```

### Step 2 — Apply the analytical schema to EC21's MySQL

Run this once — it creates the 15 `analytics_*` tables that PySpark writes into:

```bash
# Run from the EC22 directory
docker exec mysql mysql -uroot -proot123 healthcare \
  < mysql/analytical_schema.sql
```

Verify the tables were created:

```bash
docker exec mysql mysql -uroot -proot123 -e \
  "SELECT TABLE_NAME FROM information_schema.TABLES
   WHERE TABLE_SCHEMA='healthcare' AND TABLE_NAME LIKE 'analytics_%'
   ORDER BY TABLE_NAME;"
```

You should see all 15 `analytics_*` table names.

### Step 3 — Build and start EC22

```bash
cd EC22
docker compose up -d --build --wait
```

`--build` compiles both Dockerfiles (Airflow + Spark).  
`--wait` blocks until every service is healthy or has completed successfully — no need to retry.

First build takes a few minutes (downloads base images and JAR files).

### Step 4 — Verify all services are up

```bash
docker compose ps
```

Expected states:

| Container               | Status                  |
|-------------------------|-------------------------|
| ec22-postgres           | Up (healthy)            |
| ec22-airflow-init       | Exited (0)              |
| ec22-airflow-webserver  | Up (healthy)            |
| ec22-airflow-scheduler  | Up (healthy)            |
| ec22-spark-master       | Up (healthy)            |
| ec22-spark-worker       | Up                      |

### Step 5 — Open the Airflow UI and enable the DAG

Open **http://localhost:8080** — log in with `admin / admin123`.

The DAG `healthcare_analytics_pipeline` is paused by default on first load.  
Click the toggle next to the DAG name to **unpause** it.

---

## Daily Operations

### Start everything (after a machine restart)

```bash
# 1. Start EC21 MySQL
cd EC21 && docker compose up mysql -d

# 2. Start EC22
cd EC22 && docker compose up -d --wait
```

No `--build` needed unless you changed a Dockerfile or source file.

### Stop everything

```bash
# Stop EC22
cd EC22 && docker compose down

# Stop EC21 MySQL
cd EC21 && docker compose down mysql
```

Data is preserved in Docker volumes (`postgres-data`, `airflow-logs`, and EC21's MySQL volume).

### Full reset (wipes all data)

```bash
cd EC22 && docker compose down -v
cd EC21 && docker compose down -v
```

After a full reset you must re-apply the analytical schema (Step 2 above).

---

## Triggering the Pipeline

### Automatic schedule

The DAG runs automatically every day at **02:00 UTC** once unpaused.

### Manual trigger — Airflow UI

1. Open **http://localhost:8080**
2. Find `healthcare_analytics_pipeline`
3. Click the **▶ Trigger DAG** button (play icon on the right)

### Manual trigger — CLI

```bash
docker exec ec22-airflow-scheduler \
  airflow dags trigger healthcare_analytics_pipeline
```

### Check run status

```bash
docker exec ec22-airflow-scheduler \
  airflow dags list-runs -d healthcare_analytics_pipeline
```

---

## Monitoring

### Airflow UI

| URL                   | Credentials      |
|-----------------------|------------------|
| http://localhost:8080 | admin / admin123 |

- **DAGs** tab → click the DAG → **Graph** view shows real-time task status
- Green = success, Red = failed, Yellow = running, Grey = queued

### Spark UI

| UI            | URL                    |
|---------------|------------------------|
| Spark Master  | http://localhost:9090  |
| Spark Worker  | http://localhost:8082  |

The Spark Master UI shows running and completed applications.  
Click an application to see executor logs and stage timings.

### Logs

```bash
# Airflow scheduler logs (live)
docker logs -f ec22-airflow-scheduler

# Airflow webserver logs
docker logs -f ec22-airflow-webserver

# Spark master logs
docker logs -f ec22-spark-master

# Spark worker logs
docker logs -f ec22-spark-worker
```

---

## Running Spark Jobs Manually (bypass Airflow)

Run a single analytics group:

```bash
# Financial analytics (7 tables)
docker exec ec22-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/utils.py \
  --jars /opt/spark/jars/mysql-connector-j.jar \
  /opt/spark/jobs/financial_analytics.py

# Operational analytics (4 tables)
docker exec ec22-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/utils.py \
  --jars /opt/spark/jars/mysql-connector-j.jar \
  /opt/spark/jobs/operational_analytics.py

# Patient analytics (4 tables)
docker exec ec22-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/utils.py \
  --jars /opt/spark/jars/mysql-connector-j.jar \
  /opt/spark/jobs/patient_analytics.py
```

Run all 15 analytics in one shot:

```bash
docker exec ec22-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/utils.py \
  --jars /opt/spark/jars/mysql-connector-j.jar \
  /opt/spark/jobs/healthcare_analytics.py
```

---

## Verifying Analytics Output

Check all 15 analytical tables have data:

```bash
docker exec mysql mysql -uroot -proot123 -e "
  SELECT TABLE_NAME, TABLE_ROWS
  FROM information_schema.TABLES
  WHERE TABLE_SCHEMA = 'healthcare'
    AND TABLE_NAME LIKE 'analytics_%'
  ORDER BY TABLE_NAME;
"
```

Query a specific table:

```bash
docker exec mysql mysql -uroot -proot123 healthcare -e \
  "SELECT * FROM analytics_revenue_by_doctor LIMIT 10;"
```

---

## Rebuilding After Code Changes

If you edit a Spark job (`spark/jobs/*.py`) or DAG (`airflow/dags/*.py`):

```bash
# Jobs and DAGs are bind-mounted — no rebuild needed, changes apply immediately
# Trigger the DAG again to pick up the new logic
```

If you edit a **Dockerfile** or `requirements.txt`:

```bash
docker compose up -d --build airflow-webserver airflow-scheduler spark-master spark-worker
```

---

## Deploying to AWS EC2

### EC21 (streaming server)

Start only MySQL (Kafka/Flink optional):

```bash
cd EC21 && docker compose up mysql -d
```

Note EC21's **private IP** from the AWS console.

### EC22 (analytics server)

1. Edit `.env` on the EC22 server:

```env
MYSQL_HOST=<EC21_PRIVATE_IP>   # e.g. 10.0.1.45
```

2. Remove `extra_hosts` from `docker-compose.yml` — not needed on EC2 (containers reach EC21 via private IP directly):

```yaml
# Delete these lines from all services in docker-compose.yml:
extra_hosts:
  - "host.docker.internal:host-gateway"
```

3. Open port `3308` on EC21's security group, inbound from EC22's private IP.

4. Start EC22:

```bash
cd EC22 && docker compose up -d --build --wait
```

---

## Troubleshooting

### airflow-webserver / airflow-scheduler stuck in "Created" state

The `airflow-init` container must exit with code 0 first. Check its logs:

```bash
docker logs ec22-airflow-init
```

If it failed, fix the issue and run:

```bash
docker compose up airflow-init
docker compose up -d --wait airflow-webserver airflow-scheduler
```

### Spark worker not joining the cluster

```bash
docker logs ec22-spark-worker
```

The `SparkClusterSensor` in the DAG waits up to 5 minutes for at least 1 alive worker before submitting jobs — so a slow start is handled automatically.

### MySQL connection refused

Check EC21's MySQL is running and port 3308 is reachable:

```bash
# From EC22 host
curl -v telnet://<MYSQL_HOST>:3308
# or
docker exec ec22-airflow-scheduler \
  python3 -c "import pymysql; pymysql.connect(host='${MYSQL_HOST}', port=3308, user='root', password='root123', database='healthcare'); print('OK')"
```

### DAG not visible in Airflow UI

```bash
# Check for import errors
docker exec ec22-airflow-scheduler airflow dags list
docker exec ec22-airflow-scheduler airflow dags report
```

### Reset Airflow metadata (nuclear option)

```bash
docker compose down
docker volume rm ec22_postgres-data ec22_airflow-logs
docker compose up -d --build --wait
```

---

## Quick Reference

| Action                        | Command                                                                 |
|-------------------------------|-------------------------------------------------------------------------|
| Start everything              | `docker compose up -d --wait`                                           |
| Stop everything               | `docker compose down`                                                   |
| Trigger DAG                   | `docker exec ec22-airflow-scheduler airflow dags trigger healthcare_analytics_pipeline` |
| Check DAG runs                | `docker exec ec22-airflow-scheduler airflow dags list-runs -d healthcare_analytics_pipeline` |
| View scheduler logs           | `docker logs -f ec22-airflow-scheduler`                                 |
| Run all analytics manually    | `docker exec ec22-spark-master spark-submit --master spark://spark-master:7077 --py-files /opt/spark/jobs/utils.py --jars /opt/spark/jars/mysql-connector-j.jar /opt/spark/jobs/healthcare_analytics.py` |
| Check analytical tables       | `docker exec mysql mysql -uroot -proot123 -e "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA='healthcare' AND TABLE_NAME LIKE 'analytics_%';"` |
| Full reset (wipes volumes)    | `docker compose down -v`                                                |
