-- KEYS[1] = idempotency key
-- ARGV[1] = token run id
-- ARGV[2] = token sequence
local stored = redis.call('HMGET', KEYS[1], 'run_id', 'ft')
if not stored[2] then
  return 2 -- key missing / expired
end
if stored[1] ~= ARGV[1] or stored[2] ~= ARGV[2] then
  return 1 -- fencing mismatch
end
redis.call('DEL', KEYS[1])
return 0
