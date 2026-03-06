-- KEYS[1] = idempotency key
-- ARGV[1] = serialized Processing entry
-- ARGV[2] = fencing_token
-- ARGV[3] = ttl_ms

local existing = redis.call('HGET', KEYS[1], 'data')
if existing then
    return existing  -- caller inspects status and fingerprint
end

redis.call('HSET', KEYS[1], 'data', ARGV[1], 'ft', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return nil  -- proceed with handler
