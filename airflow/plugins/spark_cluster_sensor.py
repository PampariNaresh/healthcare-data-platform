
"""
SparkClusterSensor
Waits until the Spark master reports at least `min_workers` alive workers
before allowing downstream tasks to proceed.

Usage in a DAG:
    from spark_cluster_sensor import SparkClusterSensor

    wait_for_spark = SparkClusterSensor(
        task_id="wait_for_spark_cluster",
        spark_master_url="http://spark-master:8080",
        min_workers=1,
        poke_interval=15,
        timeout=300,
    )
"""

import requests
from airflow.sensors.base import BaseSensorOperator


class SparkClusterSensor(BaseSensorOperator):
    """
    Pokes the Spark master REST API until the required number
    of alive workers is available.

    :param spark_master_url: Internal URL of the Spark master web UI
    :param min_workers:      Minimum number of alive workers required
    """

    def __init__(self, spark_master_url: str = "http://spark-master:8080",
                 min_workers: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.spark_master_url = spark_master_url.rstrip("/")
        self.min_workers = min_workers

    def poke(self, context) -> bool:
        url = f"{self.spark_master_url}/json/"
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()

            alive_workers = data.get("aliveworkers", 0)
            cores_avail   = data.get("freecores", 0)
            mem_avail_mb  = data.get("freemem", 0)

            self.log.info(
                "Spark cluster — alive workers: %d / %d required | "
                "free cores: %d | free memory: %d MB",
                alive_workers, self.min_workers, cores_avail, mem_avail_mb
            )

            return alive_workers >= self.min_workers

        except requests.RequestException as e:
            self.log.warning("Spark master not reachable at %s — %s", url, e)
            return False
