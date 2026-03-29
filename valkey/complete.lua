-- KEYS[1] = idempotency key
-- ARGV[1] = serialized Completed entry
-- ARGV[2] = fencing_token
-- ARGV[3] = ttl_ms on completion.
-- ARGV[4] = fingerprint of the completing request

local stored = redis.call('HMGET', KEYS[1], 'ft', 'fp')
local current_token = stored[1]
if not current_token then
    return 2
end

if current_token ~= ARGV[2] then
    return 1
end

-- Bind the cached response to the claimed request. Entries claimed before this
-- field existed have no 'fp'; skip the check for them rather than reject.
local stored_fingerprint = stored[2]
if stored_fingerprint and stored_fingerprint ~= ARGV[4] then
    return 3
end

redis.call('HSET', KEYS[1], 'status', 'complete', 'data', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 0
