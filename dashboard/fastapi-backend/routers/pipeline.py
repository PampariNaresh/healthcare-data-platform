from fastapi import APIRouter, HTTPException
import httpx
import config

router = APIRouter()

AUTH = (config.AIRFLOW_USER, config.AIRFLOW_PASSWORD)
BASE = f"{config.AIRFLOW_API_URL}/api/v1"


def _airflow_get(path: str):
    try:
        r = httpx.get(f"{BASE}{path}", auth=AUTH, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Airflow unreachable: {e}")


@router.get("/runs")
def dag_runs():
    data = _airflow_get(f"/dags/{config.DAG_ID}/dagRuns?limit=10&order_by=-execution_date")
    return data.get("dag_runs", [])


@router.get("/last-run")
def last_run():
    data = _airflow_get(f"/dags/{config.DAG_ID}/dagRuns?limit=1&order_by=-execution_date")
    runs = data.get("dag_runs", [])
    if not runs:
        return {"status": "no_runs"}
    run = runs[0]
    run_id = run["dag_run_id"]
    tasks = _airflow_get(f"/dags/{config.DAG_ID}/dagRuns/{run_id}/taskInstances")
    run["task_instances"] = tasks.get("task_instances", [])
    return run


@router.get("/dag-info")
def dag_info():
    return _airflow_get(f"/dags/{config.DAG_ID}")
