from redqlite.producer import RQProducer
from redqlite.worker import RQWorker, RQWorkerPool
# from redqlite.subscriber import RQSubscriber
from redqlite.serializers import StringSerializer
from redqlite.utils import create_topic

import logging
import time

from redis import Redis

logging.basicConfig(level=logging.INFO)

conn = Redis(host="localhost", port=6379)

conn.flushall()

create_topic(conn, "allmsgs", 4)

rqproducer = RQProducer(redis_conn=conn, serializer=StringSerializer)


def callback_fn(msg, **kwargs):
    time.sleep(0.2)
    print(msg)


rqworker = RQWorker(topic="allmsgs", timeout_ms=10000, callback=callback_fn)
rqworker_pool = RQWorkerPool(topic="allmsgs", timeout_ms=10000, callback=callback_fn, num_workers=2)

if __name__ == "__main__":
    print("Sending messages to queue")

    rqproducer.send("allmsgs", "hello world 1", "t")
    rqproducer.send("allmsgs", "hello world 2", "u")
    rqproducer.send("allmsgs", "hello world 3", "v")
    rqproducer.send("allmsgs", "hello world 4", "w")

    rqproducer.send("allmsgs", "world hello 5", "a")
    rqproducer.send("allmsgs", "world hello 6", "b")
    rqproducer.send("allmsgs", "world hello 7", "c")
    rqproducer.send("allmsgs", "world hello 8", "d")

    rqproducer.send("allmsgs", "world hello 9", "aa")
    rqproducer.send("allmsgs", "world hello 10", "bb")
    rqproducer.send("allmsgs", "world hello 11", "cc")
    rqproducer.send("allmsgs", "world hello 12", "dd")

    print("Starting RQWorker")

    rqworker_pool.start()

    print("keep program alive")

    for i in range(30):
        time.sleep(1)
    
    print("Stopping rqworker")
    rqworker_pool.stop()
