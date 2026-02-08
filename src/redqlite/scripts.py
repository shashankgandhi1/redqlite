
LUAREDIS_LPOP_TO_KEY = """
local val = redis.call('LPOP', KEYS[1])
if val then
    redis.call('SET', KEYS[2], val)
    return val
else
    return nil
end
"""

LUAREDIS_KEY_TO_RPUSH = """
local val = redis.call('GET', KEYS[1])
if val then
    redis.call('DEL', KEYS[1])
    redis.call('RPUSH', KEYS[2], val)
    return val
else
    return nil
end
"""
