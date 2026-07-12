from pydantic import BaseModel
from typing import Literal, Optional
import uuid
import time

class JobCreate(BaseModel):
    type: Literal["resize_image", "send_email", "process_pdf"]
    payload: dict

class Job(BaseModel):
    id: str
    type: str
    payload: dict
    status: str = "pending"
    attempts: int = 0
    created_at: float

def new_job(job_create: JobCreate) -> Job:
    return Job(
        id=str(uuid.uuid4()),
        type=job_create.type,
        payload=job_create.payload,
        created_at=time.time()
    )