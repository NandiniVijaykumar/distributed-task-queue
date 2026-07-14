CLAIM_JOB_SCRIPT = """
local high_key = KEYS[1]
local low_key = KEYS[2]
local processing_key = KEYS[3]
local lease_until = ARGV[1]

local job_id = redis.call('RPOP', high_key)
if not job_id then
    job_id = redis.call('RPOP', low_key)
end
if not job_id then
    return nil
end

redis.call('ZADD', processing_key, lease_until, job_id)
redis.call('HSET', 'job:' .. job_id, 'status', 'in_progress')
return job_id
"""

RENEW_LEASE_SCRIPT = """
local processing_key = KEYS[1]
local job_id = ARGV[1]
local new_lease = ARGV[2]

if redis.call('ZSCORE', processing_key, job_id) then
    redis.call('ZADD', processing_key, new_lease, job_id)
    return 1
end
return 0
"""

REAP_SCRIPT = """
local processing_key = KEYS[1]
local delayed_key = KEYS[2]
local dead_key = KEYS[3]
local now = ARGV[1]
local run_at = ARGV[2]

local expired = redis.call('ZRANGEBYSCORE', processing_key, 0, now)
local reaped = {}
for i, job_id in ipairs(expired) do
    redis.call('ZREM', processing_key, job_id)
    local job_data_key = 'job:' .. job_id
    local attempts = tonumber(redis.call('HGET', job_data_key, 'attempts')) or 0
    local max_attempts = tonumber(redis.call('HGET', job_data_key, 'max_attempts')) or 3
    run_at = tonumber(run_at) + 3^attempts

    if attempts >= max_attempts then
        redis.call('HSET', job_data_key, 'status', 'dead')
        redis.call('LPUSH', dead_key, job_id)
    else
        redis.call('ZADD', delayed_key, run_at, job_id)
        redis.call('HSET', job_data_key, 'status', 'delayed')
    end
    table.insert(reaped, job_id)
end
return reaped
"""

PROMOTE_DELAYED_SCRIPT = """
local delayed_key = KEYS[1]
local now = ARGV[1]

local due = redis.call('ZRANGEBYSCORE', delayed_key, 0, now)
for i, job_id in ipairs(due) do
    redis.call('ZREM', delayed_key, job_id)
    local job_data_key = 'job:' .. job_id
    local priority = redis.call('HGET', job_data_key, 'priority')
    redis.call('HSET', job_data_key, 'status', 'pending')
    if priority == 'high' then
        redis.call('LPUSH', 'jobs:pending:high', job_id)
    else
        redis.call('LPUSH', 'jobs:pending:low', job_id)
    end
end
return due
"""