import json
import time
import random
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.redis_client import get_redis

r = get_redis()

def execute_job(job_id: str, job_type: str, payload: dict) -> bool:
    print(f"[worker] processing {job_id} ({job_type})")
    time.sleep(random.uniform(1, 3))  # simulate work
    if payload.get("force_fail"):  # simulate a failure if the payload contains "force_fail": True
        return False
    return True

def claim_job():
    result = r.rpoplpush("jobs:pending:high", "jobs:processing")
    if result:
        return result
    try:
        result = r.brpoplpush("jobs:pending:low", "jobs:processing", timeout=5)
    except redis.exceptions.TimeoutError:
        return None
    return result

def requeue_after_delay(job_id, priority, delay_seconds):
    run_at = time.time() + delay_seconds
    r.zadd("jobs:delayed", {job_id: run_at})

def run():
    print("[worker] started")
    while True:
        print("[worker] waiting for jobs...")
        job_id = claim_job()
        if not job_id:
            continue

        job_key = f"job:{job_id}"
        job_data = r.hgetall(job_key)
        if not job_data:
            print(f"[worker] job {job_id} has no data, skipping")
            r.lrem("jobs:processing", 0, job_id)
            continue

        r.hset(job_key, "status", "in_progress")
        payload = json.loads(job_data["payload"])

        attempts = int(job_data["attempts"]) + 1
        max_attempts = int(job_data["max_attempts"])

        success = execute_job(job_id, job_data["type"], payload)

        r.hset(job_key, "attempts", attempts)
        r.lrem("jobs:processing", 0, job_id)
        
        if success:
            r.hset(job_key, "status", "done")
            print(f"[worker] {job_id} done")
        elif attempts <= max_attempts:
            backoff = 2 ** attempts  # exponential backoff in seconds
            r.hset(job_key, "status", "pending")
            print(f"[worker] {job_id} failed, retry {attempts}/{max_attempts} in {backoff}s")
            requeue_after_delay(job_id, job_data.get("priority", "low"), backoff)
        else:
            r.hset(job_key, "status", "dead")
            r.lpush("jobs:dead", job_id)
            print(f"[worker] {job_id} exhausted retries, moved to dead letter")

if __name__ == "__main__":
    run()