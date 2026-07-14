from fastapi import FastAPI, HTTPException
import json
from .models import JobCreate, Job, new_job
from shared.redis_client import get_redis
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
r = get_redis()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/jobs")
def submit_job(job_create: JobCreate):
    job = new_job(job_create)
    r.hset(f"job:{job.id}", mapping={
        "type": job.type,
        "payload": json.dumps(job.payload),
        "status": job.status,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "priority": job.priority,
        "created_at": job.created_at,
    })
    r.lpush(f"jobs:pending:{job.priority}", job.id)
    return {"id": job.id, "status": job.status}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    data = r.hgetall(f"job:{job_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Job not found")
    data["payload"] = json.loads(data["payload"])
    return data

@app.get("/stats")
def get_stats():
    worker_keys = r.keys("worker:*")
    return {
        "pending_high": r.llen("jobs:pending:high"),
        "pending_low": r.llen("jobs:pending:low"),
        "processing": r.zcard("jobs:processing"),
        "delayed": r.zcard("jobs:delayed"),
        "dead": r.llen("jobs:dead"),
        "workers_online": len(r.keys("worker:*")),
    }

@app.get("/jobs")
def list_recent_jobs(limit: int = 20):
    # scan for job keys, return most recent by created_at
    keys = r.keys("job:*")
    jobs = []
    for key in keys:
        data = r.hgetall(key)
        if data:
            data["id"] = key.split(":", 1)[1]
            data["payload"] = json.loads(data["payload"])
            jobs.append(data)
    jobs.sort(key=lambda j: float(j["created_at"]), reverse=True)
    return jobs[:limit]

@app.get("/logs")
def get_logs(limit: int = 30):
    return r.lrange("logs:scheduler", 0, limit - 1)