import asyncio
import socket
import time
import httpx
from fastapi import APIRouter

router = APIRouter()


async def _http(client: httpx.AsyncClient, url: str) -> dict:
    try:
        t0 = time.time()
        r = await client.get(url, timeout=5.0, follow_redirects=True)
        ms = round((time.time() - t0) * 1000)
        return {"status": "online", "response_ms": ms}
    except Exception as e:
        return {"status": "offline", "response_ms": None, "error": str(e)[:80]}


def _tcp(host: str, port: int) -> dict:
    try:
        t0 = time.time()
        s = socket.create_connection((host, port), timeout=5)
        ms = round((time.time() - t0) * 1000)
        s.close()
        return {"status": "online", "response_ms": ms}
    except Exception as e:
        return {"status": "offline", "response_ms": None, "error": str(e)[:80]}


@router.get("/status")
async def infrastructure_status():
    async with httpx.AsyncClient() as client:
        airflow, spark_master, spark_worker, flink, kafka_ui, dash_api = await asyncio.gather(
            _http(client, "http://airflow-webserver:8080/health"),
            _http(client, "http://spark-master:8080/json/"),
            _http(client, "http://spark-worker:8081"),
            _http(client, "http://65.0.80.152:8081/jobs/overview"),
            _http(client, "http://65.0.80.152:8085"),
            _http(client, "http://dashboard-api:8000/health"),
        )

    kafka_tcp = _tcp("65.0.80.152", 9092)
    mysql_tcp = _tcp("65.0.80.152", 3308)
    zoo_tcp   = _tcp("65.0.80.152", 2181)
    pg_tcp    = _tcp("postgres",    5432)

    return {
        "timestamp": time.time(),
        "ec21": {
            "host": "65.0.80.152",
            "label": "Real-time Streaming Pipeline",
            "services": [
                {"name": "Kafka Broker",  "icon": "kafka",  **kafka_tcp,  "ui_url": "http://65.0.80.152:8085", "ui_label": "Kafka UI"},
                {"name": "Kafka UI",      "icon": "ui",     **kafka_ui,   "ui_url": "http://65.0.80.152:8085", "ui_label": "Open"},
                {"name": "Flink",         "icon": "flink",  **flink,      "ui_url": "http://65.0.80.152:8081", "ui_label": "Flink UI"},
                {"name": "MySQL",         "icon": "db",     **mysql_tcp},
                {"name": "Zookeeper",     "icon": "zk",     **zoo_tcp},
            ],
        },
        "ec22": {
            "host": "3.6.92.19",
            "label": "Batch Analytics + Dashboard",
            "services": [
                {"name": "Airflow",       "icon": "airflow", **airflow,      "ui_url": "http://3.6.92.19:8080",      "ui_label": "Airflow UI"},
                {"name": "Spark Master",  "icon": "spark",   **spark_master, "ui_url": "http://3.6.92.19:9090",      "ui_label": "Spark UI"},
                {"name": "Spark Worker",  "icon": "spark",   **spark_worker, "ui_url": "http://3.6.92.19:8082",      "ui_label": "Worker UI"},
                {"name": "PostgreSQL",    "icon": "db",      **pg_tcp},
                {"name": "Dashboard API", "icon": "api",     **dash_api,     "ui_url": "http://3.6.92.19:8000/docs", "ui_label": "API Docs"},
            ],
        },
    }
