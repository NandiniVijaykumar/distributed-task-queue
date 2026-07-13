import json
import time
import random
import sys
import os
import threading

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.redis_client import get_redis

r = get_redis()
LEASE_DURATION = 15  # seconds

def execute_job(job_id: str, job_type: str, payload: dict) -> bool:
    print(f"[worker] processing {job_id} ({job_type})")
    #time.sleep(random.uniform(1, 3))  # simulate work
    time.sleep(17)  # simulate work longer than lease duration to test heartbeat
    if payload.get("force_fail"):  # simulate a failure if the payload contains "force_fail": True
        return False
    return True

def claim_job():
    job_id = r.rpoplpush("jobs:pending:high", "jobs:processing_temp")
    if not job_id:
        try:
            job_id = r.brpoplpush("jobs:pending:low", "jobs:processing_temp", timeout=5)
        except redis.exceptions.TimeoutError:
            return None
    if job_id:
        lease_time = time.time() + LEASE_DURATION
        r.zadd("jobs:processing", {job_id: lease_time})
        r.lrem("jobs:processing_temp", 0, job_id)  # remove from temp list
    return job_id

def execute_job_with_heartbeat(job_id, job_type, payload):
    stop_flag = threading.Event() #shared across threads

    def renew():
        while not stop_flag.is_set(): #while heartbeat not stopped
            r.zadd("jobs:processing", {job_id: time.time() + LEASE_DURATION}) #renew
            stop_flag.wait(LEASE_DURATION / 2)  # renew at half the lease duration

    t = threading.Thread(target=renew, daemon=True) # new thread to renew heartbeat
    t.start()
    try:
        result = execute_job(job_id, job_type, payload)
    finally:
        stop_flag.set()
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
            r.zrem("jobs:processing", 0, job_id)
            continue

        r.hset(job_key, "status", "in_progress")
        payload = json.loads(job_data["payload"])

        attempts = int(job_data["attempts"]) + 1
        max_attempts = int(job_data["max_attempts"])
        r.hset(job_key, "attempts", attempts)

        success = execute_job_with_heartbeat(job_id, job_data["type"], payload)

        r.zrem("jobs:processing", job_id)
        
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