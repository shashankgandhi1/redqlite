from redis import Redis
import time
import json

import logging

from redqlite.worker import RQWorkerPool
from redqlite.producer import RQProducer
from redqlite.utils import create_topic, monitor
from redqlite.serializers import JsonSerializer, StringSerializer

logging.basicConfig(level=logging.INFO)

conn = Redis(host="localhost", port=6379)

conn.flushall()

rq_producer1 = RQProducer(redis_conn=conn, serializer=JsonSerializer)
rq_producer2 = RQProducer(redis_conn=conn, serializer=StringSerializer)

topic1 = create_topic(conn, "topic1", 4)
topic2 = create_topic(conn, "topic2", 3)


def callback_func_1(msg, **kwargs):
	heartbeat = kwargs.get("heartbeat")
	counter = msg.get("counter")
	if counter % 4 == 0:
		raise ValueError(f"bad packet #{msg.get('counter')}")

	if counter % 5 == 0:
		time.sleep(2.5)
		print(f"Finished sleeping for packet #{counter}")

	if counter % 3 == 0:
		time.sleep(1)
		heartbeat()
		time.sleep(1)
		heartbeat()
		time.sleep(1)
		print(f"Finished sleeping for packet #{counter}")

	print(f"Processed packet #{counter}")
	return


def callback_func_2(msg, **kwargs):
	heartbeat = kwargs.get("heartbeat")
	time.sleep(1)
	heartbeat()
	time.sleep(1)
	heartbeat()
	time.sleep(2)

	print(f"Processed ping from {msg}")


rq_worker_pool_1 = RQWorkerPool(
	redis_conn=conn, 
	topic="topic1", 
	timeout_ms=2000, 
	callback=callback_func_1, 
	num_workers=2, 
	serializer=JsonSerializer
)

rq_worker_pool_2 = RQWorkerPool(
	redis_conn=conn,
	topic="topic2",
	timeout_ms=3000,
	callback=callback_func_2,
	num_workers=2,
	serializer=StringSerializer
)

for i in range(1, 21):
	rq_producer1.send("topic1", {"counter": i}, i)

rq_worker_pool_1.start()

for source in ["alpha", "beta", "gamma", "delta", "omega", "tau", "mu", "nu", "eta", "sigma", "zeta"]:
	rq_producer2.send("topic2", source, source)

rq_worker_pool_2.start()

report_mid = monitor(conn, expand=True)
print(json.dumps(report_mid))

print("server simulating now")
for i in range(60):	
	time.sleep(1)
print("server simulation finished")


report = monitor(conn, expand=True)
print(json.dumps(report))

assert report.get("topics").get("topic1").get("dlq").get("count") == 8
assert report.get("topics").get("topic2").get("dlq").get("count") == 0
