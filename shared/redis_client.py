import redis
import os
from shared.lua_scripts import (
    CLAIM_JOB_SCRIPT, RENEW_LEASE_SCRIPT,
    REAP_SCRIPT, PROMOTE_DELAYED_SCRIPT
)

def get_redis():
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    return redis.from_url(redis_url, decode_responses=True)

r = get_redis()
claim_job_script = r.register_script(CLAIM_JOB_SCRIPT)
renew_lease_script = r.register_script(RENEW_LEASE_SCRIPT)
REAP_SCRIPT = r.register_script(REAP_SCRIPT)
promote_delayed_script = r.register_script(PROMOTE_DELAYED_SCRIPT)