-- KEYS[1] = idempotency key
-- KEYS[2] = counter key
-- ARGV[1] = serialized Processing entry
-- ARGV[2] = ttl_ms
-- ARGV[3] = fingerprint of the claiming request

local values = redis.call('HMGET', KEYS[1], 'status', 'run_id', 'ft', 'data')
if values[4] then
    return {values[1], values[2], values[3], values[4]}  -- caller inspects status and fingerprint
end

-- The run id is chosen when the server process starts and never written to disk, so a
-- crash that loses the last second of writes can roll the counter back but cannot give
-- two runs of the server the same token.
local run_id = string.sub(string.match(redis.call('INFO', 'server'), 'run_id:(%x+)'), 1, 16)
local ft = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], 'status', 'in_progress', 'data', ARGV[1], 'run_id', run_id, 'ft', ft, 'fp', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {'created', run_id, tostring(ft), ARGV[1]}  -- proceed with handler
