-- KEYS[1] = idempotency key
-- ARGV[1] = fencing token
-- ARGV[2] = ttl_ms


local current_token = redis.call("HGET", KEYS[1], 'ft')
if not current_token then
  return 2 -- key missing / expired
end

if current_token ~= ARGV[1] then
  return 1 -- mismatch fencing token
end

redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 0
