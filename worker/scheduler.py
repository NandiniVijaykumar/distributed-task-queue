import time
import sys
import os
import threading

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.redis_client import get_redis

r=get_redis()

def log_event(message: str):
    print(message)  # keep terminal output too
    entry = f"{time.strftime('%H:%M:%S')} {message}"
    r.lpush("logs:scheduler", entry)
    r.ltrim("logs:scheduler", 0, 99)  # keep only last 100 entries
    
def promote_delayed_jobs():
    while True:
        now = time.time()
        due = r.zrangebyscore("jobs:delayed", 0, now)
        for job_id in due:
            job_data = r.hgetall(f"job:{job_id}")
            if not job_data:
                r.zrem("jobs:delayed", job_id)
                log_event(f"[scheduler] job {job_id} has no data, skipping")
                continue
            priority = job_data.get("priority", "low")
            r.lpush(f"jobs:pending:{priority}", job_id)
            log_event(f"[scheduler] promoted delayed job {job_id} to pending:{priority}")
            r.zrem("jobs:delayed", job_id)
        time.sleep(1)  # check every second

def reap_expired_jobs():
    while True:
        now = time.time()
        expired = r.zrangebyscore("jobs:processing", 0, now)
        for job_id in expired:
            job_data = r.hgetall(f"job:{job_id}")
            if not job_data:
                r.zrem("jobs:processing", job_id)
                continue
            priority = job_data.get("priority", "low")
            attempts = int(job_data.get("attempts", 0))
            max_attempts = int(job_data.get("max_attempts", 3))
            r.zrem("jobs:processing", job_id)
            if attempts < max_attempts:
                r.hset(f"job:{job_id}", "status", "pending")
                r.lpush(f"jobs:pending:{priority}", job_id)
                log_event(f"[reaper] requeued abandoned job {job_id}")
            else:
                r.hset(f"job:{job_id}", "status", "dead")
                r.lpush("jobs:dead", job_id)
                log_event(f"[reaper] {job_id} exceeded max attempts, moved to dead letter")
        time.sleep(1)  # check every second

if __name__ == "__main__":
    threading.Thread(
        target=promote_delayed_jobs,
        daemon=True
    ).start()

    threading.Thread(
        target=reap_expired_jobs,
        daemon=True
    ).start()

    while True:
        time.sleep(60)