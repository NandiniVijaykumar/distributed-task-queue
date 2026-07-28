from pydantic import BaseModel, Field
from typing import Literal, Optional
import uuid
import time

class JobCreate(BaseModel):
    type: Literal["resize_image", "process_pdf","long_task"]
    payload: dict
    priority: Literal["low", "high"] = "low"
    max_attempts: int = Field(default=3, ge=1, le=10)

class Job(BaseModel):
    id: str
    type: str
    payload: dict
    status: str = "pending"
    attempts: int = 0
    max_attempts: int = Field(default=3, ge=1, le=10)
    priority: Literal["low", "high"]
    created_at: float

def new_job(job_create: JobCreate) -> Job:
    return Job(
        id=str(uuid.uuid4()),
        type=job_create.type,
        payload=job_create.payload,
        created_at=time.time(),
        priority=job_create.priority,
        max_attempts=job_create.max_attempts
    )