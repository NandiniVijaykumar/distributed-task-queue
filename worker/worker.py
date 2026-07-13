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

def run():
    print("[worker] started")
    while True:
        print("[worker] waiting for jobs...")
        # atomically move a job from pending to processing
        try:
            result = r.brpoplpush("jobs:pending", "jobs:processing", timeout=5)
        except redis.exceptions.TimeoutError:
            continue # socket timeout, loop again
        if result is None:
            continue  # no job available, loop again
        job_id = result

        job_key = f"job:{job_id}"
        job_data = r.hgetall(job_key)
        if not job_data:
            print(f"[worker] job {job_id} has no data, skipping")
            r.lrem("jobs:processing", 0, job_id)
            continue

        r.hset(job_key, "status", "in_progress")
        payload = json.loads(job_data["payload"])

        success = execute_job(job_id, job_data["type"], payload)

        r.hset(job_key, "status", "done" if success else "failed")
        r.lrem("jobs:processing", 0, job_id)
        print(f"[worker] finished {job_id}: {'done' if success else 'failed'}")

if __name__ == "__main__":
    run()