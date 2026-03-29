-- KEYS[1] = idempotency key
-- ARGV[1] = fencing token
local current_token = redis.call('HGET', KEYS[1], 'ft')
if not current_token then
  return 2 -- key missing / expired
end

if current_token ~= ARGV[1] then
  return 1 -- fencing mismatch
end
redis.call('DEL', KEYS[1])
return 0
