from redis import Redis

from redqlite.utils import create_topic, get_topic_meta, update_topic


conn = Redis(host="localhost", port=6379)

conn.flushall()

topic1_meta = create_topic(conn, "topic1", 4)
topic2_meta = create_topic(conn, "topic2", 2)


assert topic1_meta.get("partitions") == 4
assert topic2_meta.get("partitions") == 2

topic1_meta = update_topic(conn, "topic1", 5)
try:
	update_topic(conn, "topic2", 1)
	assert False, "Should not allow removing partitions"
except:
	assert True

topic2_meta = get_topic_meta(conn, "topic2")
assert topic2_meta.get("partitions") == 2

print(topic1_meta)
print(topic2_meta)