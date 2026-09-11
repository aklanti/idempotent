-- KEYS[1] = idempotency key
-- ARGV[1] = serialized Completed entry
-- ARGV[2] = token run id
-- ARGV[3] = token sequence
-- ARGV[4] = ttl_ms on completion
-- ARGV[5] = fingerprint of the completing request

local stored = redis.call('HMGET', KEYS[1], 'run_id', 'ft', 'fp')
if not stored[2] then
    return 2
end
if stored[1] ~= ARGV[2] or stored[2] ~= ARGV[3] then
    return 1
end
local stored_fingerprint = stored[3]
if stored_fingerprint and stored_fingerprint ~= ARGV[5] then
    return 3
end
redis.call('HSET', KEYS[1], 'status', 'complete', 'data', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return 0
