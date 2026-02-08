from redqlite.producer import RQProducer
from redqlite.subscriber import RQSubscriber
from redqlite.serializers import StringSerializer

import logging
import time

from redis import Redis

logging.basicConfig(level=logging.INFO)

redis_conn = Redis()

rqproducer = RQProducer(redis_conn=redis_conn, serializer=StringSerializer)

def callback_fn(msg, **kwargs):
    time.sleep(0.2)
    print(msg)


rqsub1 = RQSubscriber(redis_conn=redis_conn, channel="testchannel", callback=callback_fn, serializer=StringSerializer)
rqsub2 = RQSubscriber(redis_conn=redis_conn, channel="testchannel", callback=callback_fn, serializer=StringSerializer)
rqsub3 = RQSubscriber(redis_conn=redis_conn, channel="testchannel", callback=callback_fn, serializer=StringSerializer)
rqsub4 = RQSubscriber(redis_conn=redis_conn, channel="testchannel", callback=callback_fn, serializer=StringSerializer)
rqsub5 = RQSubscriber(redis_conn=redis_conn, channel="testchannel", callback=callback_fn, serializer=StringSerializer)

if __name__ == "__main__":
    print("Starting RQ Subscribers ...")
    rqsub1.start()
    rqsub2.start()
    rqsub3.start()
    rqsub4.start()
    rqsub5.start()

    print("Broadcasting messages")

    rqproducer.broadcast("testchannel", "hello world")

    rqproducer.broadcast("testchannel", "hello world 2")

    print("keep program alive")
    for i in range(2):
        time.sleep(1)

    rqproducer.broadcast("testchannel", "hello world 3")

    for i in range(30):
        time.sleep(1)
    
    print("Stopping RQSubscriber")
    rqsub1.stop()
    rqsub2.stop()
    rqsub3.stop()
    rqsub4.stop()
    rqsub5.stop()
