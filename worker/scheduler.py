import time
import sys
import os
import threading

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.redis_client import get_redis, reap_to_delayed_script, promote_delayed_script

r=get_redis()

def log_event(message: str):
    print(message)  # keep terminal output too
    entry = f"{time.strftime('%H:%M:%S')} {message}"
    r.lpush("logs:scheduler", entry)
    r.ltrim("logs:scheduler", 0, 99)  # keep only last 100 entries
    
def promote_delayed_jobs():
    while True:
        now = time.time()
        due = promote_delayed_script(keys=["jobs:delayed"], args=[now])
        for job_id in due:
            log_event(f"[scheduler] promoted {job_id} back to pending")
        time.sleep(1)  # check every second

def reap_expired_jobs():
    while True:
        now = time.time()
        run_at = now + 3
        expired = reap_to_delayed_script(keys=["jobs:processing", "jobs:delayed","jobs:dead"], args=[now, run_at])
        for job_id in expired:
            log_event(f"[reaper] {job_id} lease expired, moved to delayed queue")
        time.sleep(1)

if __name__ == "__main__":
    log_event("[scheduler] starting scheduler...")
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