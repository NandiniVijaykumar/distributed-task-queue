from fastapi import FastAPI, HTTPException
import json
from .models import JobCreate, Job, new_job
from shared.redis_client import get_redis

app = FastAPI()
r = get_redis()

@app.post("/jobs")
def submit_job(job_create: JobCreate):
    job = new_job(job_create)
    r.hset(f"job:{job.id}", mapping={
        "type": job.type,
        "payload": json.dumps(job.payload),
        "status": job.status,
        "attempts": job.attempts,
        "created_at": job.created_at
    })
    r.lpush("jobs:pending", job.id)
    return {"id": job.id, "status": job.status}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    data = r.hgetall(f"job:{job_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Job not found")
    data["payload"] = json.loads(data["payload"])
    return data