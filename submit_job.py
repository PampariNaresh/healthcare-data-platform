"""
Flink Multi-Job Submission & Monitor Script
  - Submits all registered jobs to the Flink cluster
  - Monitors each job independently in its own thread
  - Auto-resubmits any FAILED / CANCELED job with exponential backoff
  - Add new jobs to the JOBS list at the bottom of this file

Run inside the Flink container:
    python /opt/flink/jobs/submit_job.py

Or from the host:
    docker exec flink-jobmanager python /opt/flink/jobs/submit_job.py
"""
import logging
import os
import re
import subprocess
import sys
import time
import threading
from dataclasses import dataclass, field
from typing import Optional

import requests

# ── Config ────────────────────────────────────────────────────────────────────

FLINK_REST    = os.getenv("FLINK_REST_URL",  "http://localhost:8081")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
MAX_RETRIES   = int(os.getenv("MAX_RETRIES",   "5"))
RETRY_DELAY   = int(os.getenv("RETRY_DELAY",   "30"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)


def _get_logger(job_name: str):
    logger = logging.getLogger(job_name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            f"%(asctime)s  %(levelname)-8s  [{job_name}]  %(message)s"
        ))
        logger.addHandler(handler)
        logger.propagate = False
    return logger


# ── Job definition ────────────────────────────────────────────────────────────

@dataclass
class FlinkJob:
    name:        str           # display name
    script:      str           # path inside the container
    max_retries: int  = 5      # max resubmit attempts on failure
    retry_delay: int  = 30     # initial wait between retries (doubles each time)
    parallelism: int  = 1      # --p flag for flink run
    args:        list = field(default_factory=list)  # extra CLI args


# ── Build JOBS list from JOB_SCRIPTS env var or auto-discover ─────────────────
# If JOB_SCRIPTS is set, use it (comma-separated paths).
# If not set, auto-discover all *.py files in the jobs directory,
# excluding submit_job.py and kafka_topics_setup.py.

_EXCLUDED = {"submit_job.py", "kafka_topics_setup.py"}
_JOBS_DIR = os.path.dirname(os.path.abspath(__file__))

def _build_jobs() -> list:
    raw = os.getenv("JOB_SCRIPTS", "").strip()
    if raw:
        scripts = [s.strip() for s in raw.split(",") if s.strip()]
    else:
        scripts = sorted(
            os.path.join(_JOBS_DIR, f)
            for f in os.listdir(_JOBS_DIR)
            if f.endswith(".py") and f not in _EXCLUDED
        )
        logging.getLogger("startup").info("Auto-discovered jobs: %s", scripts)

    jobs = []
    for script in scripts:
        name = os.path.splitext(os.path.basename(script))[0].replace("_", "-")
        jobs.append(FlinkJob(
            name        = name,
            script      = script,
            max_retries = MAX_RETRIES,
            retry_delay = RETRY_DELAY,
            parallelism = 1,
        ))
    return jobs

JOBS: list[FlinkJob] = _build_jobs()


# ── REST helpers ──────────────────────────────────────────────────────────────

def wait_for_jobmanager(timeout: int = 60) -> None:
    log = logging.getLogger("startup")
    log.info("Waiting for Flink JobManager at %s ...", FLINK_REST)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{FLINK_REST}/v1/overview", timeout=5)
            if r.status_code == 200:
                info = r.json()
                log.info("JobManager ready — slots: %d available / %d total",
                         info.get("slots-available", "?"),
                         info.get("slots-total", "?"))
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise RuntimeError(f"JobManager not reachable after {timeout}s")


def get_job_state(job_id: str) -> dict:
    r = requests.get(f"{FLINK_REST}/v1/jobs/{job_id}", timeout=10)
    r.raise_for_status()
    return r.json()


def get_job_exceptions(job_id: str) -> str:
    try:
        r = requests.get(f"{FLINK_REST}/v1/jobs/{job_id}/exceptions", timeout=10)
        root = r.json().get("root-exception", "")
        return root[:600] if root else "no exception details"
    except Exception:
        return "could not fetch exceptions"


# ── Submission ────────────────────────────────────────────────────────────────

def submit(job: FlinkJob, log) -> str:
    cmd = ["flink", "run", f"-p{job.parallelism}", "-py", job.script] + job.args
    log.info("Submitting: %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True)
    output = result.stdout + result.stderr

    if result.returncode != 0:
        raise RuntimeError(f"flink run failed (exit {result.returncode}):\n{output}")

    match = re.search(r"Job has been submitted with JobID\s+([a-f0-9]{32})", output)
    if not match:
        raise RuntimeError(f"Cannot parse JobID from:\n{output}")

    return match.group(1)


# ── Monitor loop ──────────────────────────────────────────────────────────────

def monitor(job_id: str, job: FlinkJob, log) -> str:
    log.info("Monitoring JobID=%s (poll every %ds)", job_id, POLL_INTERVAL)
    log.info("Flink UI → %s/#/job/%s/overview", FLINK_REST, job_id)

    while True:
        try:
            info  = get_job_state(job_id)
            state = info.get("state", "UNKNOWN")
            log.info("JobID=%-32s  state=%s", job_id, state)

            if state in ("FAILED", "CANCELED"):
                reason = get_job_exceptions(job_id)
                log.error("Job %s — reason:\n%s", state, reason)
                return state

            if state == "FINISHED":
                log.info("Job finished successfully.")
                return state

        except requests.RequestException as e:
            log.warning("REST poll error: %s", e)

        time.sleep(POLL_INTERVAL)


# ── Per-job thread worker ─────────────────────────────────────────────────────

def run_job(job: FlinkJob, exit_events: dict) -> None:
    log   = _get_logger(job.name)
    delay = job.retry_delay

    for attempt in range(1, job.max_retries + 2):
        if exit_events["shutdown"].is_set():
            log.info("Shutdown requested — exiting.")
            return

        log.info("─── Attempt %d / %d ───", attempt, job.max_retries + 1)
        try:
            job_id      = submit(job, log)
            final_state = monitor(job_id, job, log)

            if final_state == "FINISHED":
                log.info("Completed successfully.")
                return

            if final_state in ("FAILED", "CANCELED"):
                if attempt > job.max_retries:
                    log.error("Max retries (%d) reached — giving up.", job.max_retries)
                    exit_events["any_failed"].set()
                    return
                log.warning("Retrying in %ds (attempt %d of %d)...",
                            delay, attempt + 1, job.max_retries + 1)
                time.sleep(delay)
                delay = min(delay * 2, 300)

        except Exception as e:
            log.error("Error: %s", e)
            if attempt > job.max_retries:
                log.error("Max retries reached — giving up.")
                exit_events["any_failed"].set()
                return
            log.warning("Retrying in %ds ...", delay)
            time.sleep(delay)
            delay = min(delay * 2, 300)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not JOBS:
        logging.getLogger("startup").error("No jobs registered in JOBS list.")
        sys.exit(1)

    wait_for_jobmanager()

    logging.getLogger("startup").info(
        "Submitting %d job(s): %s", len(JOBS), [j.name for j in JOBS]
    )

    exit_events = {
        "shutdown":   threading.Event(),
        "any_failed": threading.Event(),
    }

    threads = []
    for job in JOBS:
        t = threading.Thread(
            target=run_job,
            args=(job, exit_events),
            name=job.name,
            daemon=True
        )
        t.start()
        threads.append(t)
        time.sleep(2)  # stagger submissions slightly

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(5)
    except KeyboardInterrupt:
        logging.getLogger("startup").info("Interrupted — signalling threads to stop...")
        exit_events["shutdown"].set()
        for t in threads:
            t.join(timeout=15)

    if exit_events["any_failed"].is_set():
        logging.getLogger("startup").error("One or more jobs failed permanently.")
        sys.exit(1)

    logging.getLogger("startup").info("All jobs completed.")
    sys.exit(0)


if __name__ == "__main__":
    main()
