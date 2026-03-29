-- KEYS[1] = idempotency key
-- ARGV[1] = fencing token
-- ARGV[2] = ttl_ms

local stored = redis.call('HMGET', KEYS[1], 'ft', 'status')
local current_token = stored[1]
if not current_token then
  return 2 -- key missing / expired
end

if current_token ~= ARGV[1] then
  return 1 -- mismatch fencing token
end

-- Only a live claim can be extended; a completed entry keeps its replay TTL.
if stored[2] ~= 'in_progress' then
  return 2
end

redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 0
