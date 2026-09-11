-- KEYS[1] = idempotency key
-- ARGV[1] = token run id
-- ARGV[2] = token sequence
-- ARGV[3] = ttl_ms

local stored = redis.call('HMGET', KEYS[1], 'run_id', 'ft', 'status')
if not stored[2] then
  return 2 -- key missing / expired
end
if stored[1] ~= ARGV[1] or stored[2] ~= ARGV[2] then
  return 1 -- mismatch fencing token
end

-- Only a live claim can be extended; a completed entry keeps its replay TTL.
if stored[3] ~= 'in_progress' then
  return 2
end

redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 0
