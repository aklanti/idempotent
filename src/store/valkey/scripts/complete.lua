-- KEYS[1] = idempotency key
-- ARGV[1] = serialized Completed entry
-- ARGV[2] = fencing_token
-- ARGV[3] = ttl_ms

local current_token = redis.call('HGET', KEYS[1], 'ft')
if not current_token then
    return 2
end

if current_token ~= ARGV[2] then
    return 1
end

redis.call('HSET', KEYS[1], 'status', 'complete', 'data', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 0
