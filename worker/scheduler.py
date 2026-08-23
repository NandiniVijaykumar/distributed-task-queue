import time
import sys
import os
import threading
from prometheus_client import start_http_server
from metrics import leases_reaped_total, jobs_retried_total, jobs_dead_total, queue_depth, workers_online

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.redis_client import get_redis, reap_script, promote_delayed_script
from shared.lua_scripts import BACKOFF_BASE

r=get_redis()

def log_event(message: str):
    print(message)  # keep terminal output too
    entry = f"{time.strftime('%H:%M:%S')} {message}"
    r.lpush("logs:scheduler", entry)
    r.ltrim("logs:scheduler", 0, 99)  # keep only last 100 entries
    
def promote_delayed_jobs():
    while True:
        try:
            now = time.time()
            due = promote_delayed_script(keys=["jobs:delayed"], args=[now])
            for job_id in due:
                log_event(f"[scheduler] promoted {job_id} back to pending")
            time.sleep(1)  # check every second
        except Exception as e:
            log_event(f"[reaper] error: {e}")
        time.sleep(1)

def reap_expired_jobs():
    while True:
        try:
            now = time.time()
            results = reap_script(keys=["jobs:processing", "jobs:delayed", "jobs:dead"], args=[now, BACKOFF_BASE])
            for i in range(0, len(results), 2):
                job_id = results[i]
                outcome = results[i + 1]
                leases_reaped_total.inc()
                if outcome == "dead":
                    jobs_dead_total.labels(type=r.hget(f"job:{job_id}", "type")).inc()
                    log_event(f"[reaper] {job_id} lease expired, exceeded max attempts, moved to dead letter")
                else:
                    jobs_retried_total.labels(type=r.hget(f"job:{job_id}", "type")).inc()
                    log_event(f"[reaper] {job_id} lease expired, moved to delayed queue")
        except Exception as e:
            log_event(f"[reaper] error: {e}")
        time.sleep(1)


def update_gauges():
    while True:
        queue_depth.labels(priority="high").set(r.llen("jobs:pending:high"))
        queue_depth.labels(priority="low").set(r.llen("jobs:pending:low"))
        workers_online.set(len(r.keys("worker:*")))
        time.sleep(1)

if __name__ == "__main__":
    start_http_server(8002)
    log_event("[scheduler] starting scheduler...")
    threading.Thread(target=promote_delayed_jobs, daemon=True).start()
    threading.Thread(target=reap_expired_jobs, daemon=True).start()
    threading.Thread(target=update_gauges, daemon=True).start()
    while True:
        time.sleep(60)