-- KEYS[1] = idempotency key
-- KEY[2] = counter key
-- ARGV[1] = serialized Processing entry
-- ARGV[2] = ttl_ms

local values = redis.call('HMGET', KEYS[1], 'status', 'ft', 'data')
if values[3] then
    return {values[1], values[2], values[3]}  -- caller inspects status and fingerprint
end

local ft = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], 'status', 'in_progress', 'data', ARGV[1], 'ft', ft)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {'created', tostring(ft), ARGV[1]}  -- proceed with handler
