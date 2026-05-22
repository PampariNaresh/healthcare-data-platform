import socket
import time
import httpx
from langchain_core.tools import tool
import db
import config

AUTH = (config.AIRFLOW_USER, config.AIRFLOW_PASSWORD)
DAG_ID = config.DAG_ID


# ── Tool 1 — SQL query ────────────────────────────────────────────────────────

@tool
def query_analytics_db(sql: str) -> str:
    """Run a read-only MySQL SELECT query on the healthcare database.
    Use for questions about revenue, patients, doctors, appointments, treatments, billing.
    Tables: patients, doctors, appointments, treatments, billing,
    and 15 analytics_* tables (analytics_monthly_revenue, analytics_revenue_by_doctor,
    analytics_appointment_status, analytics_patient_retention, etc).
    Only SELECT statements are allowed."""
    if not sql.strip().upper().startswith("SELECT"):
        return "Error: only SELECT statements are allowed."
    try:
        rows = db.query(sql)
        if not rows:
            return "Query returned no rows."
        return str(rows[:50])
    except Exception as e:
        return f"SQL error: {e}"


# ── Tool 2 — Airflow pipeline status ─────────────────────────────────────────

@tool
def get_pipeline_status() -> str:
    """Get the latest Airflow DAG run status and per-task breakdown.
    Use this when asked about pipeline health, last run, task durations,
    or whether analytics data is fresh."""
    try:
        r = httpx.get(
            f"{config.AIRFLOW_API_URL}/api/v1/dags/{DAG_ID}/dagRuns"
            "?limit=1&order_by=-execution_date",
            auth=AUTH, timeout=10,
        )
        r.raise_for_status()
        runs = r.json().get("dag_runs", [])
        if not runs:
            return "No DAG runs found."
        run = runs[0]
        run_id = run["dag_run_id"]

        tasks_r = httpx.get(
            f"{config.AIRFLOW_API_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances",
            auth=AUTH, timeout=10,
        )
        tasks_r.raise_for_status()
        tasks = tasks_r.json().get("task_instances", [])

        lines = [
            f"DAG: {DAG_ID}",
            f"Run ID: {run_id}",
            f"State: {run.get('state')}",
            f"Execution date: {run.get('execution_date')}",
            f"Start: {run.get('start_date')} | End: {run.get('end_date')}",
            "",
            "Tasks:",
        ]
        for t in tasks:
            dur = f" ({round(t['duration'])}s)" if t.get("duration") else ""
            lines.append(f"  {t['task_id']}: {t['state']}{dur}")
        return "\n".join(lines)
    except Exception as e:
        return f"Could not reach Airflow: {e}"


# ── Tool 3 — Trigger pipeline ─────────────────────────────────────────────────

@tool
def trigger_analytics_pipeline() -> str:
    """Trigger a new Airflow DAG run to refresh all 15 analytics tables.
    Only use this after the user has explicitly confirmed they want to run the pipeline."""
    try:
        r = httpx.post(
            f"{config.AIRFLOW_API_URL}/api/v1/dags/{DAG_ID}/dagRuns",
            auth=AUTH, json={"conf": {}}, timeout=10,
        )
        r.raise_for_status()
        run_id = r.json().get("dag_run_id", "unknown")
        return f"Pipeline triggered successfully. Run ID: {run_id}. Monitor at http://3.6.92.19:8080"
    except Exception as e:
        return f"Failed to trigger pipeline: {e}"


# ── Tool 4 — Infrastructure health ───────────────────────────────────────────

def _http_probe(url: str) -> dict:
    try:
        t0 = time.time()
        r = httpx.get(url, timeout=5.0, follow_redirects=True)
        ms = round((time.time() - t0) * 1000)
        return {"status": "online", "response_ms": ms}
    except Exception as e:
        return {"status": "offline", "error": str(e)[:60]}


def _tcp_probe(host: str, port: int) -> dict:
    try:
        t0 = time.time()
        s = socket.create_connection((host, port), timeout=5)
        ms = round((time.time() - t0) * 1000)
        s.close()
        return {"status": "online", "response_ms": ms}
    except Exception as e:
        return {"status": "offline", "error": str(e)[:60]}


@tool
def check_infrastructure_health() -> str:
    """Check live health status of all 10 services across EC21 and EC22.
    Use when asked about service status, what's running, what's down, or overall system health."""
    checks = {
        "EC21 — Kafka broker (TCP :9092)": _tcp_probe("65.0.80.152", 9092),
        "EC21 — MySQL (TCP :3308)":         _tcp_probe("65.0.80.152", 3308),
        "EC21 — Zookeeper (TCP :2181)":     _tcp_probe("65.0.80.152", 2181),
        "EC21 — Flink UI (:8081)":          _http_probe("http://65.0.80.152:8081/jobs/overview"),
        "EC21 — Kafka UI (:8085)":          _http_probe("http://65.0.80.152:8085"),
        "EC22 — Airflow (:8080)":           _http_probe("http://airflow-webserver:8080/health"),
        "EC22 — Spark Master (:9090)":      _http_probe("http://spark-master:8080/json/"),
        "EC22 — Spark Worker (:8082)":      _http_probe("http://spark-worker:8081"),
        "EC22 — PostgreSQL (TCP :5432)":    _tcp_probe("postgres", 5432),
        "EC22 — Dashboard API (:8000)":     _http_probe("http://dashboard-api:8000/health"),
    }
    lines = []
    for name, result in checks.items():
        icon = "✅" if result["status"] == "online" else "❌"
        ms = f" ({result['response_ms']} ms)" if result.get("response_ms") else ""
        err = f" — {result.get('error', '')}" if result["status"] == "offline" else ""
        lines.append(f"{icon} {name}{ms}{err}")
    online = sum(1 for r in checks.values() if r["status"] == "online")
    lines.insert(0, f"Infrastructure: {online}/{len(checks)} services online\n")
    return "\n".join(lines)


# ── Tool 5 — Kafka topic info ─────────────────────────────────────────────────

@tool
def get_kafka_topic_info() -> str:
    """Get message counts and offset info for the 5 Kafka topics:
    patients, doctors, appointments, treatments, billing.
    Use when asked about event counts, producer status, or how many messages have been processed."""
    try:
        r = httpx.get(
            "http://65.0.80.152:8085/api/clusters/healthcare-cluster/topics",
            timeout=5,
        )
        r.raise_for_status()
        topics = r.json().get("topics", [])

        target = {"patients", "doctors", "appointments", "treatments", "billing"}
        lines = ["Kafka topics (healthcare-cluster):"]
        for t in topics:
            name = t.get("name", "")
            if name not in target:
                continue
            partitions = t.get("partitions", [])
            total_msgs = sum(
                p.get("offsetMax", 0) - p.get("offsetMin", 0)
                for p in partitions
            )
            lines.append(f"  {name}: {total_msgs} messages")
        return "\n".join(lines) if len(lines) > 1 else "No matching topics found."
    except Exception as e:
        return f"Could not reach Kafka UI: {e}"


# ── Tool 6 — Flink job status ─────────────────────────────────────────────────

@tool
def get_flink_job_status() -> str:
    """Get current Flink streaming job state, uptime, and checkpoint info.
    Use when asked whether the streaming pipeline is running, job health, or checkpoint status."""
    try:
        r = httpx.get("http://65.0.80.152:8081/v1/jobs/overview", timeout=5)
        r.raise_for_status()
        jobs = r.json().get("jobs", [])
        if not jobs:
            return "No Flink jobs found."
        lines = [f"Flink jobs ({len(jobs)} total):"]
        for j in jobs:
            uptime_s = j.get("duration", 0) // 1000
            h, m = divmod(uptime_s, 3600)
            m, s = divmod(m, 60)
            lines.append(
                f"  [{j.get('state')}] {j.get('name')} | "
                f"ID: {j.get('jid', '')[:12]}... | "
                f"Uptime: {h}h {m}m {s}s"
            )
        for j in jobs:
            if j.get("state") == "RUNNING":
                try:
                    cr = httpx.get(
                        f"http://65.0.80.152:8081/v1/jobs/{j['jid']}/checkpoints",
                        timeout=5,
                    )
                    ckpt = cr.json()
                    latest = ckpt.get("latest", {}).get("completed", {})
                    if latest:
                        age_s = round((time.time() * 1000 - latest.get("trigger_timestamp", 0)) / 1000)
                        lines.append(
                            f"  Last checkpoint: {age_s}s ago | "
                            f"Duration: {latest.get('end_to_end_duration', 0)}ms"
                        )
                except Exception:
                    pass
        return "\n".join(lines)
    except Exception as e:
        return f"Could not reach Flink: {e}"


# ── Tool 7 — MySQL row counts ─────────────────────────────────────────────────

@tool
def get_mysql_row_counts() -> str:
    """Get live row counts for all 5 operational MySQL tables:
    patients, doctors, appointments, treatments, billing.
    Use when asked how many records exist, data volume, or current database size."""
    tables = ["patients", "doctors", "appointments", "treatments", "billing"]
    lines = ["MySQL healthcare DB — operational table row counts:"]
    for t in tables:
        try:
            count = db.query(f"SELECT COUNT(*) AS n FROM {t}")[0]["n"]
            lines.append(f"  {t}: {count:,} rows")
        except Exception as e:
            lines.append(f"  {t}: error — {e}")
    return "\n".join(lines)


ALL_TOOLS = [
    query_analytics_db,
    get_pipeline_status,
    trigger_analytics_pipeline,
    check_infrastructure_health,
    get_kafka_topic_info,
    get_flink_job_status,
    get_mysql_row_counts,
]
