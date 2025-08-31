from redqlite.producer import RQProducer
from redqlite.worker import RQWorker, RQWorkerPool
from redqlite.subscriber import RQSubscriber

import logging
import time

logging.basicConfig(level=logging.INFO)

rqproducer = RQProducer()


def callback_fn(msg, **kwargs):
    time.sleep(0.2)
    print(msg)

rqworker = RQWorker(topic="allmsgs", timeout_ms=10000, callback=callback_fn)
rqworker_pool = RQWorkerPool(topic="allmsgs", timeout_ms=10000, callback=callback_fn, num_workers=2)

rqsub1 = RQSubscriber(channel="testchannel", callback=callback_fn)
rqsub2 = RQSubscriber(channel="testchannel", callback=callback_fn)
rqsub3 = RQSubscriber(channel="testchannel", callback=callback_fn)
rqsub4 = RQSubscriber(channel="testchannel", callback=callback_fn)
rqsub5 = RQSubscriber(channel="testchannel", callback=callback_fn)

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

    # print("Broadcasting messages")

    # rqproducer.broadcast("testchannel", "hello world")

    print("Starting RQWorker")

    rqworker_pool.start()
    # rqsub1.start()
    # rqsub2.start()
    # rqsub3.start()
    # rqsub4.start()
    # rqsub5.start()

    # rqproducer.broadcast("testchannel", "hello world 2")

    print("keep program alive")
    for i in range(2):
        time.sleep(1)

    # rqproducer.broadcast("testchannel", "hello world 3")

    for i in range(30):
        time.sleep(1)
    
    print("Stopping rqworker")
    rqworker_pool.stop()

    # print("Stopping RQSubscriber")
    # rqsub1.stop()
    # rqsub2.stop()
    # rqsub3.stop()
    # rqsub4.stop()
    # rqsub5.stop()