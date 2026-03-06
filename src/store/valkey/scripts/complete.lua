-- KEYS[1] = idempotency key
-- ARGV[1] = serialized Completed entry
-- ARGV[2] = fencing_token
-- ARGV[3] = ttl_ms

local current_token = redis.call('HGET', KEYS[1], 'ft')
if not current_token then
    return redis.error_reply('KEY_MISSING')
end
if current_token ~= ARGV[2] then
    return redis.error_reply('FENCING_MISMATCH')
end
redis.call('HSET', KEYS[1], 'data', ARGV[1])
redis.call('HDEL', KEYS[1], 'ft')
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 'OK'
