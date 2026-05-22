# EC22 Healthcare Batch Analytics Pipeline

Batch analytics pipeline using Apache Airflow and Apache Spark (PySpark).
Reads validated healthcare data from MySQL (populated by EC21), computes 15 business analytics across
financial, operational and patient dimensions, and writes results back to MySQL analytical tables.

---

## Architecture

```
EC21 (separate server)                         EC22 (this server)
──────────────────────                         ──────────────────────────────────────────────────
Producer → Kafka → Flink → MySQL  ──────────►  Airflow DAG
                        (operational tables)        └─► SparkClusterSensor (wait for workers)
                                                         ├─► financial_analytics   (7 tables)
                                                         ├─► operational_analytics (4 tables)
                                                         └─► patient_analytics     (4 tables)
                                                                   └─► MySQL (analytical tables)
```

| Layer         | Technology                  | Purpose                                              |
|---------------|-----------------------------|------------------------------------------------------|
| Orchestration | Apache Airflow 2.9.3        | Schedules, monitors and retries Spark jobs (02:00 UTC daily) |
| Cluster Check | SparkClusterSensor (plugin) | Waits for Spark workers before submitting jobs       |
| Processing    | Apache Spark 3.5 (PySpark)  | Reads MySQL via JDBC, computes aggregations, writes back |
| Metadata DB   | PostgreSQL 15               | Airflow internal state                               |
| Analytics DB  | MySQL 8.0 (on EC21)         | Source (operational tables) + sink (analytical tables) |

---

## Project Structure

```
EC22/
├── .env                                    # All environment variables
├── docker-compose.yml                      # Services: Airflow + Spark + PostgreSQL
├── README.md                               # This file
├── STARTUP.md                              # Startup, operations, and deployment guide
├── CONNECTIONS.md                          # Connection credentials and CLI reference
│
├── airflow/
│   ├── Dockerfile                          # Airflow + JDK17 + PySpark + pymysql + MySQL JDBC JAR
│   ├── dags/
│   │   └── healthcare_analytics_dag.py     # Main pipeline DAG (3 parallel Spark jobs)
│   └── plugins/
│       └── spark_cluster_sensor.py         # Custom sensor — waits for live Spark workers
│
├── pyspark/
│   └── Dockerfile                          # Bitnami Spark 3.5 + PySpark + MySQL JDBC JAR
│
├── spark/
│   └── jobs/
│       ├── utils.py                        # Shared JDBC helpers (read/write table, SparkSession)
│       ├── financial_analytics.py          # 7 financial analytics jobs
│       ├── operational_analytics.py        # 4 operational analytics jobs
│       ├── patient_analytics.py            # 4 patient analytics jobs
│       └── healthcare_analytics.py         # Single-file runner (all 15 jobs combined)
│
└── mysql/
    └── analytical_schema.sql               # CREATE TABLE for all 15 analytical tables
```

---

## Services & Ports

| Service            | Container                | Host Port        | Internal Port |
|--------------------|--------------------------|------------------|---------------|
| Airflow Webserver  | ec22-airflow-webserver   | 8080             | 8080          |
| Airflow Scheduler  | ec22-airflow-scheduler   | —                | —             |
| Airflow Init       | ec22-airflow-init        | —                | —             |
| PostgreSQL         | ec22-postgres            | 5432             | 5432          |
| Spark Master       | ec22-spark-master        | 9090 (UI), 7077  | 8080, 7077    |
| Spark Worker       | ec22-spark-worker        | 8082 (UI)        | 8081          |

---

## Credentials

| Service       | User    | Password   |
|---------------|---------|------------|
| Airflow UI    | admin   | admin123   |
| PostgreSQL    | airflow | airflow123 |
| MySQL (EC21)  | root    | root123    |

All values are defined in `.env`.

---

## DAG — healthcare_analytics_pipeline

**Schedule:** Daily at `02:00 UTC`

```
start
  └─► check_mysql_connection        verifies EC21 MySQL is reachable and has data
        └─► wait_for_spark_cluster  SparkClusterSensor — waits for ≥1 alive worker
                ├─► financial_analytics     (7 tables — run in parallel)
                ├─► operational_analytics   (4 tables — run in parallel)
                └─► patient_analytics       (4 tables — run in parallel)
                          └─► pipeline_complete
```

| Setting          | Value          |
|------------------|----------------|
| Schedule         | `0 2 * * *`    |
| Catchup          | Disabled       |
| Max active runs  | 1              |
| Retries          | 2              |
| Retry delay      | 5 minutes      |

---

## Plugin — SparkClusterSensor

Located at `airflow/plugins/spark_cluster_sensor.py`.

Pokes the Spark master REST API (`http://spark-master:8080/json/`) every 15 seconds until
at least the required number of alive workers is available. Prevents Spark jobs from
failing when the worker hasn't joined the cluster yet at DAG start time.

| Parameter        | Default                      | Description                          |
|------------------|------------------------------|--------------------------------------|
| `spark_master_url` | `http://spark-master:8080` | Internal URL of Spark master web UI  |
| `min_workers`    | `1`                          | Minimum alive workers required       |
| `poke_interval`  | `15`                         | Seconds between checks               |
| `timeout`        | `300`                        | Max wait time in seconds             |

---

## Analytical Tables (15 total)

### Financial (7 tables)

| Table                                  | Description                                         |
|----------------------------------------|-----------------------------------------------------|
| `analytics_revenue_by_doctor`          | Total revenue, avg & max bill per doctor            |
| `analytics_revenue_by_specialization`  | Revenue and appointment volume per specialization   |
| `analytics_revenue_by_branch`          | Revenue breakdown per hospital branch               |
| `analytics_billing_payment`            | Bill count & amounts by payment method and status   |
| `analytics_outstanding_payments`       | Pending and failed payments still to be collected   |
| `analytics_monthly_revenue`            | Month-over-month revenue with growth %              |
| `analytics_treatment_cost`             | Avg / min / max / total cost per treatment type     |

### Operational (4 tables)

| Table                                  | Description                                         |
|----------------------------------------|-----------------------------------------------------|
| `analytics_appointment_status`         | Appointment counts and % by status per doctor       |
| `analytics_doctor_workload`            | Unique patients, no-show & cancellation rates       |
| `analytics_peak_hours`                 | Appointment volume and completion rate by hour      |
| `analytics_top_doctors_scorecard`      | Composite score: revenue rank + completion rank     |

### Patient (4 tables)

| Table                                  | Description                                         |
|----------------------------------------|-----------------------------------------------------|
| `analytics_patient_spending`           | Spend, age, gender split per insurance provider     |
| `analytics_patient_age_groups`         | Revenue and appointments by age bracket (0–18 … 65+)|
| `analytics_patient_retention`          | Visit frequency segments and their revenue share    |
| `analytics_new_patient_trend`          | Monthly new patient registrations with gender split |

---

## Spark Jobs

| File                        | Tables written | Description                              |
|-----------------------------|----------------|------------------------------------------|
| `financial_analytics.py`    | 7              | Revenue, billing, trends, outstanding    |
| `operational_analytics.py`  | 4              | Workload, peak hours, doctor scorecard   |
| `patient_analytics.py`      | 4              | Demographics, retention, age groups      |
| `healthcare_analytics.py`   | 15             | All-in-one runner (manual use)           |
| `utils.py`                  | —              | Shared JDBC config, read/write helpers   |

All jobs read from MySQL via JDBC using `spark.read.jdbc()` and write back with
`mode="overwrite"` and `truncate=true` (preserves schema, refreshes data each run).

---

## Environment Variables (.env)

| Variable                      | Default                  | Description                                     |
|-------------------------------|--------------------------|--------------------------------------------------|
| `MYSQL_HOST`                  | host.docker.internal     | EC21 MySQL host — change to EC21 IP on EC2      |
| `MYSQL_PORT`                  | 3308                     | MySQL port exposed by EC21                       |
| `MYSQL_DATABASE`              | healthcare               | Database name                                    |
| `MYSQL_USER`                  | root                     | MySQL user                                       |
| `MYSQL_PASSWORD`              | root123                  | MySQL password                                   |
| `POSTGRES_USER`               | airflow                  | Airflow metadata DB user                         |
| `POSTGRES_PASSWORD`           | airflow123               | Airflow metadata DB password                     |
| `POSTGRES_DB`                 | airflow                  | Airflow metadata DB name                         |
| `POSTGRES_HOST_PORT`          | 5432                     | PostgreSQL host port                             |
| `AIRFLOW_UID`                 | 50000                    | Airflow container user ID                        |
| `AIRFLOW_WEBSERVER_PORT`      | 8080                     | Airflow UI host port                             |
| `AIRFLOW_FERNET_KEY`          | —                        | Fernet key for encrypting secrets                |
| `AIRFLOW__CORE__EXECUTOR`     | LocalExecutor            | Airflow executor type                            |
| `_AIRFLOW_WWW_USER_USERNAME`  | admin                    | Airflow UI username                              |
| `_AIRFLOW_WWW_USER_PASSWORD`  | admin123                 | Airflow UI password                              |
| `SPARK_MASTER_WEBUI_PORT`     | 9090                     | Spark master Web UI host port                    |
| `SPARK_MASTER_PORT`           | 7077                     | Spark master port                                |
| `SPARK_WORKER_WEBUI_PORT`     | 8082                     | Spark worker Web UI host port                    |
| `SPARK_WORKER_CORES`          | 2                        | CPU cores per worker                             |
| `SPARK_WORKER_MEMORY`         | 2g                       | Memory per worker                                |

---

## Documentation

| File             | Contents                                                      |
|------------------|---------------------------------------------------------------|
| `README.md`      | Architecture, services, analytics catalogue, env vars         |
| `STARTUP.md`     | First-time setup, daily start/stop, EC2 deployment, troubleshooting |
| `CONNECTIONS.md` | Connection credentials and CLI reference                      |

---

## Quick Start (Local)

### Prerequisites

EC21's MySQL must be running and the analytical schema applied:

```bash
# 1. Start only MySQL from EC21
cd ../EC21 && docker compose up mysql -d

# 2. Apply analytical schema
docker exec mysql mysql -uroot -proot123 healthcare \
  < ../EC22/mysql/analytical_schema.sql
```

### Start EC22

```bash
# 1. Build and start all services
cd EC22
docker compose up -d --build

# 2. Check all containers are healthy
docker compose ps

# 3. Open Airflow UI — enable and trigger the DAG
#    http://localhost:8080  (admin / admin123)

# 4. Or trigger from CLI
docker exec ec22-airflow-scheduler \
  airflow dags trigger healthcare_analytics_pipeline

# 5. Stop everything
docker compose down

# Full reset (removes volumes)
docker compose down -v
```

---

## Deploying to EC2

| Step | Action |
|------|--------|
| 1 | Update `.env`: set `MYSQL_HOST=<EC21_PRIVATE_IP>` |
| 2 | Remove `extra_hosts` from `docker-compose.yml` (not needed on EC2) |
| 3 | Open port `3308` inbound on EC21's security group from EC22's IP |
| 4 | Run `docker compose up -d --build` on EC22's EC2 server |

---

## Adding a New Analytics Job

1. Create the PySpark job in `spark/jobs/` — import helpers from `utils.py`
2. Add the corresponding `CREATE TABLE` to `mysql/analytical_schema.sql`
3. Add a new `SparkSubmitOperator` task in `airflow/dags/healthcare_analytics_dag.py`
4. Wire it into the DAG dependencies

---

## Useful Commands

```bash
# View Airflow scheduler logs
docker logs -f ec22-airflow-scheduler

# Trigger the DAG manually
docker exec ec22-airflow-scheduler \
  airflow dags trigger healthcare_analytics_pipeline

# Check DAG run status
docker exec ec22-airflow-scheduler \
  airflow dags list-runs -d healthcare_analytics_pipeline

# Run a single Spark job directly (bypassing Airflow)
docker exec ec22-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/utils.py \
  --jars /opt/spark/jars/mysql-connector-j.jar \
  /opt/spark/jobs/financial_analytics.py

# Run all 15 analytics in one shot
docker exec ec22-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/utils.py \
  --jars /opt/spark/jars/mysql-connector-j.jar \
  /opt/spark/jobs/healthcare_analytics.py

# Check all 15 analytical tables have data
docker exec mysql mysql -uroot -proot123 -e "
  SELECT TABLE_NAME, TABLE_ROWS
  FROM information_schema.TABLES
  WHERE TABLE_SCHEMA = 'healthcare'
  AND TABLE_NAME LIKE 'analytics_%'
  ORDER BY TABLE_NAME;
"

# Rebuild after code changes
docker compose up -d --build \
  airflow-webserver airflow-scheduler spark-master spark-worker
```

---

## Web UIs

| UI           | URL                   | Credentials      |
|--------------|-----------------------|------------------|
| Airflow      | http://localhost:8080 | admin / admin123 |
| Spark Master | http://localhost:9090 | —                |
| Spark Worker | http://localhost:8082 | —                |
