# Spark History Server — Setup Notes

## Overview

A Spark History Server container (`ec22-spark-history`) was added to the EC22 Docker stack so that completed Spark job history — including full stage, task, and executor detail — is permanently accessible via a web UI at `http://<EC22-IP>:18080`.

## Architecture

Spark jobs run in **client mode** via Airflow's `SparkSubmitOperator`. The driver runs inside the Airflow container. Event logs are written by the driver to a shared Docker named volume (`spark-event-logs`), which is also mounted by the History Server container.

```
Airflow container (UID 50000)
  └── SparkSubmitOperator (driver)
        └── writes event logs → /opt/spark/event-logs/
                                      ↑
                              Docker named volume: spark-event-logs
                                      ↑
                              History Server container (root)
                                    port 18080
```

## Files Changed

### `docker-compose.yml`
- Added `spark-event-logs` named volume (top-level)
- Mounted volume into `x-airflow-common` at `/opt/spark/event-logs`
- Mounted volume into `spark-master` and `spark-worker` containers
- Added `spark-history` service:
  - Runs as `user: root` — event logs are written by Airflow UID 50000; root can read any file
  - Exposes port `18080`
  - Mounts `pyspark/conf/spark-defaults.conf` and the shared event-logs volume
  - Depends on `spark-master` health check

### `pyspark/conf/spark-defaults.conf` (new file)
```
spark.eventLog.enabled          true
spark.eventLog.dir              /opt/spark/event-logs
spark.history.fs.logDirectory   /opt/spark/event-logs
spark.history.ui.port           18080
spark.history.retainedApplications 50
```

### `airflow/dags/healthcare_analytics_dag.py`
Added to `SPARK_CONF` (applied to all three SparkSubmitOperator tasks):
```python
"spark.eventLog.enabled": "true",
"spark.eventLog.dir":     "file:/opt/spark/event-logs",
```

## Permission Issue and Fix

Event logs are written by Airflow (UID 50000) with permissions `-rw-rw----`. Running the History Server as the default `spark` user caused `Permission denied` errors and showed 0 applications in the UI.

**Fix:** Added `user: root` to the `spark-history` service in `docker-compose.yml`. Root can read files regardless of ownership, so future logs from any DAG run are always visible without manual permission changes.

## Access

- **URL:** `http://3.6.92.19:18080`
- **Port 18080** must be open in the EC22 EC2 security group (`airflow-spark-ec22`, ap-south-1)

## Verification

```bash
# List apps visible to History Server
curl http://3.6.92.19:18080/api/v1/applications

# Check event log files on the shared volume
docker exec ec22-spark-history ls -la /opt/spark/event-logs/

# Tail History Server logs
docker logs ec22-spark-history --tail 30
```

After the fix, 6 completed applications are visible (2 DAG runs x 3 Spark jobs each):
- HealthcareFinancialAnalytics
- HealthcareOperationalAnalytics
- HealthcarePatientAnalytics
