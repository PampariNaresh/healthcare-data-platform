# EC22 Healthcare Analytics Pipeline — Connection Reference

## Services Overview

| Service            | Container                | Host Port       | Internal Port |
|--------------------|--------------------------|-----------------|---------------|
| Airflow Webserver  | ec22-airflow-webserver   | 8080            | 8080          |
| Airflow Scheduler  | ec22-airflow-scheduler   | —               | —             |
| PostgreSQL         | ec22-postgres            | 5432            | 5432          |
| Spark Master       | ec22-spark-master        | 9090 (UI), 7077 | 8080, 7077    |
| Spark Worker       | ec22-spark-worker        | 8081 (UI)       | 8081          |
| MySQL (EC21)       | mysql (on EC21 server)   | 3308            | 3306          |

---

## Airflow

| Field    | Value                  |
|----------|------------------------|
| URL      | http://localhost:8080  |
| Username | admin                  |
| Password | admin123               |
| Executor | LocalExecutor          |
| Metadata DB | PostgreSQL (ec22-postgres) |

**Open Airflow UI:**
```
http://localhost:8080
```

**Trigger DAG manually from CLI:**
```bash
docker exec ec22-airflow-scheduler \
  airflow dags trigger healthcare_analytics_pipeline
```

**List DAG runs:**
```bash
docker exec ec22-airflow-scheduler \
  airflow dags list-runs -d healthcare_analytics_pipeline
```

**View task logs:**
```bash
docker exec ec22-airflow-scheduler \
  airflow tasks logs healthcare_analytics_pipeline financial_analytics <run_id>
```

**Airflow REST API:**
```bash
# Get all DAGs
curl -u admin:admin123 http://localhost:8080/api/v1/dags

# Trigger a DAG run
curl -u admin:admin123 -X POST http://localhost:8080/api/v1/dags/healthcare_analytics_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -d '{"conf": {}}'
```

---

## Spark

| Field               | Value                      |
|---------------------|----------------------------|
| Master URL          | spark://spark-master:7077  |
| Master Web UI       | http://localhost:9090      |
| Worker Web UI       | http://localhost:8081      |
| Worker Cores        | 2                          |
| Worker Memory       | 2g                         |
| Deploy Mode         | client                     |
| Airflow Connection  | spark_default              |

**Open Spark Master UI:**
```
http://localhost:9090
```

**Submit a job directly (bypassing Airflow):**
```bash
docker exec ec22-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/utils.py \
  --jars /opt/spark/jars/mysql-connector-j.jar \
  /opt/spark/jobs/financial_analytics.py
```

**Run all analytics in one shot:**
```bash
docker exec ec22-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/utils.py \
  --jars /opt/spark/jars/mysql-connector-j.jar \
  /opt/spark/jobs/healthcare_analytics.py
```

**Check Spark REST API:**
```bash
curl http://localhost:9090/api/v1/applications
```

---

## PostgreSQL (Airflow Metadata DB)

| Field    | Value       |
|----------|-------------|
| Host     | 127.0.0.1   |
| Port     | 5432        |
| Database | airflow     |
| User     | airflow     |
| Password | airflow123  |

**CLI (from host):**
```bash
psql -h 127.0.0.1 -p 5432 -U airflow -d airflow
```

**CLI (from inside container):**
```bash
docker exec -it ec22-postgres psql -U airflow -d airflow
```

> This database is used exclusively by Airflow internals. Do not write to it directly.

---

## MySQL (EC21 Server)

| Field          | Value (local testing)    | Value (EC2 deployment)    |
|----------------|--------------------------|---------------------------|
| Host           | host.docker.internal     | EC21 Private IP           |
| Port           | 3308                     | 3308                      |
| Database       | healthcare               | healthcare                |
| User           | root                     | root                      |
| Password       | root123                  | root123                   |

**JDBC URL (used by all Spark jobs):**
```
jdbc:mysql://<MYSQL_HOST>:3308/healthcare?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
```

**CLI — query operational tables (from host):**
```bash
mysql -h 127.0.0.1 -P 3308 -u root -proot123 healthcare
```

**CLI — query analytical tables:**
```bash
mysql -h 127.0.0.1 -P 3308 -u root -proot123 -e "
  SELECT 'revenue_by_doctor'       AS tbl, COUNT(*) AS rows FROM healthcare.analytics_revenue_by_doctor        UNION ALL
  SELECT 'revenue_by_spec'         AS tbl, COUNT(*) AS rows FROM healthcare.analytics_revenue_by_specialization UNION ALL
  SELECT 'revenue_by_branch'       AS tbl, COUNT(*) AS rows FROM healthcare.analytics_revenue_by_branch         UNION ALL
  SELECT 'billing_payment'         AS tbl, COUNT(*) AS rows FROM healthcare.analytics_billing_payment            UNION ALL
  SELECT 'outstanding_payments'    AS tbl, COUNT(*) AS rows FROM healthcare.analytics_outstanding_payments       UNION ALL
  SELECT 'monthly_revenue'         AS tbl, COUNT(*) AS rows FROM healthcare.analytics_monthly_revenue            UNION ALL
  SELECT 'treatment_cost'          AS tbl, COUNT(*) AS rows FROM healthcare.analytics_treatment_cost             UNION ALL
  SELECT 'appointment_status'      AS tbl, COUNT(*) AS rows FROM healthcare.analytics_appointment_status         UNION ALL
  SELECT 'doctor_workload'         AS tbl, COUNT(*) AS rows FROM healthcare.analytics_doctor_workload            UNION ALL
  SELECT 'peak_hours'              AS tbl, COUNT(*) AS rows FROM healthcare.analytics_peak_hours                 UNION ALL
  SELECT 'top_doctors_scorecard'   AS tbl, COUNT(*) AS rows FROM healthcare.analytics_top_doctors_scorecard      UNION ALL
  SELECT 'patient_spending'        AS tbl, COUNT(*) AS rows FROM healthcare.analytics_patient_spending           UNION ALL
  SELECT 'patient_age_groups'      AS tbl, COUNT(*) AS rows FROM healthcare.analytics_patient_age_groups         UNION ALL
  SELECT 'patient_retention'       AS tbl, COUNT(*) AS rows FROM healthcare.analytics_patient_retention          UNION ALL
  SELECT 'new_patient_trend'       AS tbl, COUNT(*) AS rows FROM healthcare.analytics_new_patient_trend;
"
```

---

## Airflow Spark Connection (spark_default)

Created automatically by `airflow-init` on first startup.

| Field       | Value                     |
|-------------|---------------------------|
| Conn ID     | spark_default             |
| Conn Type   | Spark                     |
| Host        | spark-master              |
| Port        | 7077                      |
| Deploy Mode | client                    |

**Verify the connection exists:**
```bash
docker exec ec22-airflow-scheduler airflow connections get spark_default
```

**Re-create if missing:**
```bash
docker exec ec22-airflow-scheduler airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode": "client"}'
```

---

## Docker Network

| Field        | Value          |
|--------------|----------------|
| Network Name | ec22_ec22-net  |
| Driver       | bridge         |

All EC22 containers communicate using their **container names** as hostnames within this network.

**Inter-service hostnames:**

| Container                | Hostname               |
|--------------------------|------------------------|
| ec22-airflow-webserver   | airflow-webserver      |
| ec22-airflow-scheduler   | airflow-scheduler      |
| ec22-postgres            | postgres               |
| ec22-spark-master        | spark-master           |
| ec22-spark-worker        | spark-worker           |
| MySQL (EC21)             | host.docker.internal (local) / EC21 private IP (EC2) |

---

## Quick Start

```bash
# Start EC21 MySQL only (local testing)
cd ../EC21 && docker compose up mysql -d

# Start all EC22 services
cd ../EC22 && docker compose up -d --build

# Check all containers are healthy
docker compose ps

# Trigger the analytics pipeline manually
docker exec ec22-airflow-scheduler \
  airflow dags trigger healthcare_analytics_pipeline

# Stop all EC22 services
docker compose down

# Full reset (removes volumes)
docker compose down -v
```

---

## EC2 Deployment Checklist

| Step | Action |
|------|--------|
| 1 | Update `MYSQL_HOST` in `.env` to EC21's private IP |
| 2 | Remove `extra_hosts` from `docker-compose.yml` (not needed on EC2) |
| 3 | Open port `3308` in EC21's EC2 security group (inbound from EC22's IP) |
| 4 | Run `docker compose up -d --build` on EC22's EC2 |
