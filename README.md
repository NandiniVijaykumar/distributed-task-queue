# Distributed Task Queue

A Redis-backed job queue with atomic job claiming, retry with exponential backoff, priority scheduling, and worker heartbeat-based failure recovery. Clients submit jobs (image resize, PDF processing); worker processes claim and execute them; a scheduler process handles delayed retries and reaps jobs abandoned by crashed workers.

This isn't a wrapper around an existing queue library - the queueing, leasing, and failure-recovery mechanics are built directly on Redis primitives (lists, sorted sets, Lua scripts) to demonstrate the underlying concepts a system like Celery or Sidekiq automates.

---

## What it does

- Accepts jobs via a REST API (`POST /jobs`), each with a type, payload, priority and max retry count
- Workers claim jobs atomically - no two workers can ever process the same job
- Failed jobs retry with exponential backoff, up to a configurable limit, then move to a dead-letter queue
- High-priority jobs are processed before low-priority ones
- Each in-progress job is tracked with a time-limited lease, renewed periodically by the worker (a "heartbeat"). If a worker crashes mid-job, the lease expires, and a separate reaper process detects this and recovers the job - without ever running it twice
- A live dashboard shows queue depth, active workers, and recent job status

## Architecture

```
Client --POST /jobs--> FastAPI --> Redis (job hash + pending queue)
                                       |
                                       v
                          Worker(s) -- poll + RPOP -->  jobs:pending:{high,low}
                                 |  (atomic claim + lease assignment, via Lua script;
                                 |   worker sleeps 1s and retries if both queues are empty)
                                 v
                          jobs:processing (sorted set, scored by lease expiry)
                                 |
                    success -----+----- failure
                       |                   |
                  status: done      attempts < max? --> jobs:delayed (scored by retry time)
                                        |
                                     no --> jobs:dead

Scheduler (separate process, polls every 1s):
  - promotes due jobs from jobs:delayed back to jobs:pending
  - reaps jobs:processing entries whose lease has expired,
    checks attempts vs max_attempts, routes to jobs:delayed or jobs:dead
```

Claiming is currently poll-based (`RPOP` inside a Lua script, `time.sleep(1)` on an empty queue), not a blocking pop.

### Redis data model

| Key | Type | Purpose |
|---|---|---|
| `job:{id}` | Hash | Full job record: type, payload, status, attempts, max_attempts, priority, created_at |
| `jobs:pending:high` / `jobs:pending:low` | List | Jobs waiting to be claimed, by priority |
| `jobs:processing` | Sorted set | Jobs currently claimed; score = lease expiry timestamp |
| `jobs:delayed` | Sorted set | Jobs waiting to be retried; score = scheduled retry timestamp |
| `jobs:dead` | List | Jobs that exhausted all retry attempts |
| `worker:{id}` | String (TTL) | Worker liveness heartbeat; expires automatically if the worker stops renewing it |
| `logs:scheduler` | List (capped) | Recent reaper/scheduler events, surfaced on the dashboard |

## Setup

```bash
# Redis
docker run -d -p 6379:6379 redis

# Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run each in a separate terminal
uvicorn api.main:app --reload
python3 worker/worker.py       # run multiple instances to see concurrent claiming
python3 worker/scheduler.py

# Dashboard
open static/dashboard.html     # polls the API every 2s
```

## API

- `POST /jobs` — submit a job: `{"type": "resize_image", "payload": {...}, "priority": "high", "max_attempts": 5}` (max_attempts optional, defaults to 3, bounded 1-10)
- `GET /jobs/{id}` - check a job's status
- `GET /jobs?limit=20` - list recent jobs
- `GET /stats` - queue depths, dead-letter count, workers online
- `GET /logs` / `DELETE /logs` - scheduler/reaper event log

## Design decisions

**Why Redis instead of a message broker (RabbitMQ, SQS).** Redis is a data store, not a queue system - it has no built-in concept of consumer liveness, delivery acknowledgment, or message redelivery. All of that (leasing, heartbeat renewal, crash detection, retry/backoff) is implemented in this project's own worker and scheduler code, on top of Redis's list and sorted-set primitives. This project is to demonstrate the underlying mechanics.

**Atomicity via Lua scripts.** Claiming a job, completing a job (removing from `jobs:processing` and routing to done/retry/dead), and reaping expired leases are each implemented as single Redis Lua scripts rather than sequences of separate commands. Redis executes a script as one atomic unit, so there's no window where another process can observe or interfere with a partially-applied state change (e.g., a job removed from the processing set but not yet marked `done`).

**max_attempts is client-configurable but bounded (1-10),** enforced via Pydantic's `Field(ge=1, le=10)` at the API layer. An unbounded, fully client-controlled retry limit would let a caller set it high enough to effectively defeat dead-letter routing, letting a failing job retry indefinitely and starve other jobs of worker time. A fixed bound avoids that while still giving callers real control.

## Limitations

**The completion race is a fundamental limitation.** Even with atomic scripts, there remains a gap between "worker finishes executing a job" and "worker successfully calls the atomic completion script." If the worker process dies in that window, the reaper will still see an expired lease and requeue the job - meaning it can genuinely run twice. This is the classic **at-least-once delivery** problem. The mitigation here is an idempotency check (a job whose status is already `done` is skipped rather than re-executed), which covers the common case but doesn't eliminate the narrow window where a crash happens after work completes but before the status write lands. A fully correct fix would require the job handlers themselves to be idempotent (e.g., safe to run twice), this project's handlers (image resize, PDF read) happen to be naturally idempotent, but that's a property of the handlers, not a guarantee the queue provides.

**`GET /jobs` uses `KEYS job:*`,** an O(n) full-keyspace scan.

**Low-priority jobs can starve.** `CLAIM_JOB_SCRIPT` always pops from the high queue first and only checks the low queue if high is empty. There's no aging, no weighting, no fairness ratio - if high-priority jobs keep arriving faster than workers can drain them, low-priority jobs wait indefinitely, not just "longer." Any fix needs some form of aging (e.g., promote a low job to high after it's waited past a threshold) or a weighted pick between the two queues instead of a strict priority order.

**The scheduler is a single point of failure.** The design assumes exactly one scheduler process running. If it crashes, nothing promotes delayed jobs back to pending and nothing reaps expired leases - the whole crash-recovery mechanism silently stops until it's restarted, with no automatic detection that it's down. Note this is not a correctness risk from running multiple schedulers concurrently - the Lua scripts are atomic, so two scheduler instances racing on the same reap/promote call would just do redundant work, not corrupt state or double-process a job. So the gap is availability (nobody's doing the job), not safety.

**No fencing tokens.** Job ownership is tracked by lease expiry alone, not by a unique ownership token per claim. This means that in a narrow timing window, it's theoretically possible for a reaper to reassign a job while the original worker is still (slowly) finishing it, without either side being aware of the other. The correct fix is a fencing token - a unique value issued at claim time, checked before any write is accepted - enforced via an atomic compare-and-set.

**File outputs are ephemeral.** The image resize and PDF handlers read/write local files. In this local/demo setup that's fine; in the deployed version, worker filesystems aren't persistent or shared, so output files wouldn't survive a restart or be retrievable elsewhere. A production version would write outputs to object storage (S3 or similar).

## Future work

- Fencing tokens for job ownership, enforced via Lua compare-and-set
- A single blocking claim across both priority queues using `BLMPOP` (Redis 7+), removing the current poll-and-sleep behavior and the small latency gap where a worker waiting on an empty queue won't notice a new high-priority job until its next poll
- Aging or weighted selection between `jobs:pending:high` and `jobs:pending:low` so low-priority jobs can't starve indefinitely
- Run multiple scheduler instances with leader election (e.g. a Redis-based lock with lease renewal) so the promote/reap loop survives a crashed scheduler instead of stopping entirely
- Replace `KEYS`-based listing with a maintained index