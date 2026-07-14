from pydantic import BaseModel
from typing import Literal, Optional
import uuid
import time

class JobCreate(BaseModel):
    type: Literal["resize_image", "process_pdf","long_task"]
    payload: dict
    priority: Literal["low", "high"] = "low"

class Job(BaseModel):
    id: str
    type: str
    payload: dict
    status: str = "pending"
    attempts: int = 0
    max_attempts: int = 3 # for retries
    priority: Literal["low", "high"]
    created_at: float

def new_job(job_create: JobCreate) -> Job:
    return Job(
        id=str(uuid.uuid4()),
        type=job_create.type,
        payload=job_create.payload,
        created_at=time.time(),
        priority=job_create.priority
    )