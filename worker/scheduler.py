import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.redis_client import get_redis

r=get_redis()

def promote_delayed_jobs():
    while True:
        now = time.time()
        due = r.zrangebyscore("jobs:delayed", 0, now)
        for job_id in due:
            job_data = r.hgetall(f"job:{job_id}")
            if not job_data:
                r.zrem("jobs:delayed", job_id)
                print(f"[scheduler] job {job_id} has no data, skipping")
                continue
            priority = job_data.get("priority", "low")
            r.lpush(f"jobs:pending:{priority}", job_id)
            print(f"[scheduler] promoted delayed job {job_id} to pending:{priority}")
            r.zrem("jobs:delayed", job_id)

if __name__ == "__main__":
    promote_delayed_jobs()