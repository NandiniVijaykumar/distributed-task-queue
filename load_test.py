"""
load_test.py — fire a burst of mixed jobs at the task queue API so the
Prometheus metrics / Grafana dashboards actually have something to show:
queue depth spiking then draining, retries, dead-letters, and a spread
of job durations.

Usage:
    python3 load_test.py                # defaults below
    python3 load_test.py --count 200 --api http://localhost:8000

While this runs, watch:
    curl -s -L localhost:8000/stats
    curl -s -L localhost:8001/metrics | grep job_duration_seconds
    curl -s -L localhost:8002/metrics | grep queue_depth
or just watch the dashboard / Grafana panels update live.
"""

import argparse
import json
import random
import time
import urllib.request
import urllib.error


def submit_job(api: str, job: dict) -> dict:
    data = json.dumps(job).encode()
    req = urllib.request.Request(
        f"{api}/jobs", data=data, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as e:
        print(f"[load_test] submit failed: {e}")
        return {}


def make_job() -> dict:
    """Pick a random job shape: mostly normal, some slow, some forced failures."""
    roll = random.random()
    priority = random.choice(["high", "low"])

    if roll < 0.55:
        # fast no-op — bulk of "normal" traffic
        return {"type": "noop", "payload": {}, "priority": priority}

    elif roll < 0.75:
        # slow job — stretches job_duration_seconds and queue_wait for
        # whatever's stuck behind it, good for seeing backlog build up
        return {
            "type": "long_task",
            "payload": {"duration_seconds": random.randint(2, 6)},
            "priority": priority,
        }

    elif roll < 0.9:
        # forced failure — will retry with backoff, eventually may dead-letter
        # depending on max_attempts, exercises jobs_failed_total /
        # jobs_retried_total / jobs_dead_total
        return {
            "type": "noop",
            "payload": {"force_fail": True},
            "priority": priority,
            "max_attempts": random.choice([1, 2, 3]),
        }

    else:
        job_type = random.choice(["resize_image", "process_pdf"])

        if job_type == "resize_image":
            return {
                "type": "resize_image",
                "payload": {
                    "file": "test_files/sample.jpg",
                    "width": 200,
                    "height": 200,
                },
                "priority": priority,
                "max_attempts": 2,
            }

        else:
            return {
                "type": "process_pdf",
                "payload": {
                    "file": "test_files/sample.pdf",
                },
                "priority": priority,
                "max_attempts": 2,
            }


def main():
    parser = argparse.ArgumentParser(description="Load-test the task queue API")
    parser.add_argument("--api", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--count", type=int, default=100, help="number of jobs to submit")
    parser.add_argument(
        "--rate", type=float, default=10.0,
        help="jobs per second to submit (controls burstiness)"
    )
    args = parser.parse_args()

    delay = 1.0 / args.rate if args.rate > 0 else 0
    submitted = 0
    started = time.time()

    print(f"[load_test] submitting {args.count} jobs to {args.api} at ~{args.rate}/s")
    for i in range(args.count):
        job = make_job()
        result = submit_job(args.api, job)
        if result.get("id"):
            submitted += 1
        if (i + 1) % 20 == 0:
            print(f"[load_test] {i + 1}/{args.count} submitted...")
        time.sleep(delay)

    elapsed = time.time() - started
    print(
        f"[load_test] done: {submitted}/{args.count} jobs submitted "
        f"in {elapsed:.1f}s. Watch /stats and the metrics endpoints "
        f"over the next 30-60s as the queue drains, retries fire, and "
        f"the reaper/scheduler catch anything left hanging."
    )


if __name__ == "__main__":
    main()