import json
import time
import random
import sys
import os
import threading
import uuid
from prometheus_client import start_http_server
from metrics import (
    jobs_completed_total, jobs_failed_total,
    job_duration_seconds, job_queue_wait_seconds,
    jobs_retried_total, jobs_dead_total,
)


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.redis_client import get_redis, claim_job_script, complete_job_script, renew_lease_script
from shared.lua_scripts import BACKOFF_BASE

from worker.handlers import HANDLERS

r = get_redis()
LEASE_DURATION = 5  # seconds

WORKER_ID = str(uuid.uuid4())[:8]

def worker_heartbeat():
    while True:
        r.set(f"worker:{WORKER_ID}", "alive", ex=2)  # expires in 10s if not renewed
        time.sleep(1)  # renew well before expiry


def execute_job(job_id: str, job_type: str, payload: dict) -> bool:
    print(f"[worker] processing {job_id} ({job_type})")
    if payload.get("force_fail"):  # simulate a failure if the payload contains "force_fail": True
        return False
    handler = HANDLERS.get(job_type)
    if not handler:
        print(f"[worker] no handler for job type: {job_type}")
        return False
    return handler(payload)

def claim_job():
    lease_until = time.time() + LEASE_DURATION
    job_id = claim_job_script(
        keys=["jobs:pending:high", "jobs:pending:low", "jobs:processing"],
        args=[lease_until]
    )
    return job_id

def execute_job_with_heartbeat(job_id, job_type, payload):
    stop_flag = threading.Event() #shared across threads

    def renew():
        while not stop_flag.is_set():
            new_lease = time.time() + LEASE_DURATION
            result = renew_lease_script(keys=["jobs:processing"], args=[job_id, new_lease])
            if result == 0:
                print(f"[worker] lease for {job_id} no longer valid")
                break
            stop_flag.wait(LEASE_DURATION / 2)

    t = threading.Thread(target=renew, daemon=True) # new thread to renew heartbeat
    t.start()
    start = time.time()
    try:
        result = execute_job(job_id, job_type, payload)
    finally:
        stop_flag.set()
        job_duration_seconds.labels(type=job_type).observe(time.time() - start)
    return result

def run():
    print("[worker] started")
    print("[worker] waiting for jobs...")
    while True:
        job_id = claim_job()
        if not job_id:
            time.sleep(1)  # no jobs available, wait a bit
            continue

        job_key = f"job:{job_id}"
        job_data = r.hgetall(job_key)
        if not job_data:
            print(f"[worker] job {job_id} has no data, skipping")
            r.zrem("jobs:processing", job_id)
            continue

        job_queue_wait_seconds.labels(priority=job_data.get("priority", "low")).observe(
            time.time() - float(job_data["created_at"])
        )

        current_status = r.hget(job_key, "status")
        if current_status == "done":
            print(f"[worker] {job_id} already done, skipping duplicate execution")
            r.zrem("jobs:processing", job_id)
            continue

        r.hset(job_key, "status", "in_progress")
        payload = json.loads(job_data["payload"])

        attempts = int(job_data["attempts"]) + 1
        max_attempts = int(job_data["max_attempts"])
        r.hset(job_key, "attempts", attempts)

        success = execute_job_with_heartbeat(job_id, job_data["type"], payload)

        if success:
            jobs_completed_total.labels(type=job_data["type"]).inc()
        else:
            jobs_failed_total.labels(type=job_data["type"]).inc()
            
        now = time.time()

        result, run_at_str = complete_job_script(
            keys=["jobs:processing", "jobs:dead", "jobs:delayed"],
            args=[job_id, "1" if success else "0", attempts, max_attempts, BACKOFF_BASE]
        )

        if result == "done":
            print(f"[worker] {job_id} done")
        elif result == "retry":
            jobs_retried_total.labels(type=job_data["type"]).inc()
            run_at = float(run_at_str)
            print(f"[worker] {job_id} failed, retry {attempts}/{max_attempts} (in {run_at - time.time():.1f}s)")
        else:
            jobs_dead_total.labels(type=job_data["type"]).inc()
            print(f"[worker] {job_id} exhausted retries, moved to dead letter")

if __name__ == "__main__":
    start_http_server(8001)
    threading.Thread(target=worker_heartbeat, daemon=True).start()
    run()