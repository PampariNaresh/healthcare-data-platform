
"""
Healthcare Analytics Pipeline DAG

Flow:
    start
      └─► check_mysql_connection
            └─► init_analytical_schema
                  └─► wait_for_spark_cluster
                            ├─► financial_analytics    (7 tables — parallel)
                            ├─► operational_analytics  (4 tables — parallel)
                            └─► patient_analytics      (4 tables — parallel)
                                        └─► pipeline_complete

Schedule: daily at 02:00 UTC (after EC21 streaming pipeline has populated MySQL)
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_cluster_sensor import SparkClusterSensor

log = logging.getLogger(__name__)

# ── Config (injected via docker-compose .env) ─────────────────────────────────
MYSQL_HOST     = os.getenv("MYSQL_HOST",     "localhost")
MYSQL_PORT     = os.getenv("MYSQL_PORT",     "3308")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "healthcare")
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root123")

SPARK_JOBS_DIR     = "/opt/spark/jobs"
MYSQL_JAR          = "/opt/spark/jars/mysql-connector-j.jar"
SPARK_MASTER_URL   = "http://spark-master:8080"

SPARK_ENV = {
    "MYSQL_HOST":     MYSQL_HOST,
    "MYSQL_PORT":     MYSQL_PORT,
    "MYSQL_DATABASE": MYSQL_DATABASE,
    "MYSQL_USER":     MYSQL_USER,
    "MYSQL_PASSWORD": MYSQL_PASSWORD,
}

SPARK_CONF = {
    "spark.sql.legacy.timeParserPolicy": "LEGACY",
    "spark.executor.memory":             "1g",
    "spark.driver.memory":               "1g",
    "spark.eventLog.enabled":            "true",
    "spark.eventLog.dir":               "file:/opt/spark/event-logs",
}

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "healthcare-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


ANALYTICAL_SCHEMA_PATH = "/opt/airflow/dags/analytical_schema.sql"


# ── MySQL connectivity check ──────────────────────────────────────────────────
def check_mysql_connection():
    import pymysql
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=int(MYSQL_PORT),
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            connect_timeout=10,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM patients")
        count = cursor.fetchone()[0]
        conn.close()
        log.info(
            "MySQL OK → %s:%s/%s | patients table has %d rows",
            MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, count
        )
    except Exception as e:
        raise RuntimeError(
            f"Cannot connect to MySQL at {MYSQL_HOST}:{MYSQL_PORT} — {e}"
        )


# ── Analytical schema init ────────────────────────────────────────────────────
def init_analytical_schema():
    import pymysql
    with open(ANALYTICAL_SCHEMA_PATH, "r") as f:
        sql = f.read()
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=int(MYSQL_PORT),
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        connect_timeout=10,
    )
    cursor = conn.cursor()
    for statement in sql.split(";"):
        stmt = statement.strip()
        if stmt and not stmt.startswith("--") and stmt.upper() != "USE HEALTHCARE":
            cursor.execute(stmt)
    conn.commit()
    conn.close()
    log.info("Analytical schema applied (CREATE TABLE IF NOT EXISTS — idempotent)")


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="healthcare_analytics_pipeline",
    description="Daily batch analytics: reads MySQL operational tables, writes to analytical tables",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "spark", "analytics"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── 1. Verify MySQL has data ──────────────────────────────────────────────
    check_mysql = PythonOperator(
        task_id="check_mysql_connection",
        python_callable=check_mysql_connection,
    )

    # ── 2. Ensure all 15 analytical tables exist with proper schema ───────────
    init_schema = PythonOperator(
        task_id="init_analytical_schema",
        python_callable=init_analytical_schema,
    )

    # ── 3. Wait for Spark cluster to have live workers ────────────────────────
    wait_for_spark = SparkClusterSensor(
        task_id="wait_for_spark_cluster",
        spark_master_url=SPARK_MASTER_URL,
        min_workers=1,
        poke_interval=15,
        timeout=300,
        mode="poke",
    )

    # ── 4. Financial Analytics (7 tables) ────────────────────────────────────
    financial_analytics = SparkSubmitOperator(
        task_id="financial_analytics",
        conn_id="spark_default",
        application=f"{SPARK_JOBS_DIR}/financial_analytics.py",
        py_files=f"{SPARK_JOBS_DIR}/utils.py",
        jars=MYSQL_JAR,
        env_vars=SPARK_ENV,
        conf=SPARK_CONF,
        name="financial_analytics_{{ ds }}",
        verbose=True,
    )

    # ── 5. Operational Analytics (4 tables) ──────────────────────────────────
    operational_analytics = SparkSubmitOperator(
        task_id="operational_analytics",
        conn_id="spark_default",
        application=f"{SPARK_JOBS_DIR}/operational_analytics.py",
        py_files=f"{SPARK_JOBS_DIR}/utils.py",
        jars=MYSQL_JAR,
        env_vars=SPARK_ENV,
        conf=SPARK_CONF,
        name="operational_analytics_{{ ds }}",
        verbose=True,
    )

    # ── 6. Patient Analytics (4 tables) ──────────────────────────────────────
    patient_analytics = SparkSubmitOperator(
        task_id="patient_analytics",
        conn_id="spark_default",
        application=f"{SPARK_JOBS_DIR}/patient_analytics.py",
        py_files=f"{SPARK_JOBS_DIR}/utils.py",
        jars=MYSQL_JAR,
        env_vars=SPARK_ENV,
        conf=SPARK_CONF,
        name="patient_analytics_{{ ds }}",
        verbose=True,
    )

    pipeline_complete = EmptyOperator(task_id="pipeline_complete")

    # ── Dependencies ──────────────────────────────────────────────────────────
    (
        start
        >> check_mysql
        >> init_schema
        >> wait_for_spark
        >> [financial_analytics, operational_analytics, patient_analytics]
        >> pipeline_complete
    )
