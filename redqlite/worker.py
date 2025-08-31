from redis import Redis
from typing import Callable
import threading
import time
import logging
import traceback

from .utils import _gen_id, get_topic_meta, create_topic
from .config import _get_topic_partition_data_key, _get_topic_partition_worker_key, _get_worker_topic_partition_key, _get_lock_key

class RQWorker:
    def __init__(self, 
                host: str = "localhost",
                port: int = 6379, 
                username: str = None, 
                password: str = None, 
                redis_conn: Redis = None, 
                topic: str = None, 
                callback: Callable = None, 
                serializer = None,
                timeout_ms: int = 10000,
                poll_timeout_ms: int = 1000):
        """
        Initialize RQLite Worker
        - host: Hostname for Redis server
        - port: Port where redis server is listening
        - username: Username of Redis server (if any)
        - password: Password of Redis server (if any)
        - redis_conn: Redis Connection object from Redis library
        - topic: Name of the topic to listen
        - callback: Callback function to execute on the message polled
        - serializer: Serializer class with deserialize method for the message
        - timeout_ms: Timeout for the worker in milliseconds
        - poll_timeout_ms: Sleep time for polling if no messages available
        """
        if not topic:
            raise Exception(f"Error: RQLite worker topic cannot be None")
        
        self.id = _gen_id()
        self.topic = topic        
        self.callback = callback
        self.serializer = serializer
        
        if redis_conn:
            self.conn = redis_conn
        else:
            self.conn = Redis(host=host, port=port, username=username, password=password)

        self.timeout_ms = timeout_ms
        self.poll_timeout_ms = poll_timeout_ms

        self._partition = None
        self._thread = None
        self._running = False
        self._processing = False

    
    def poll(self):
        """
        Implements polling message from the topic queues
        """
        topic_metadata = get_topic_meta(self.conn, self.topic)
        if not topic_metadata:
            topic_metadata = create_topic(self.conn, self.topic)
        
        # Fetch number of messages in all partition queues
        if self._partition is not None:
            self.beat()
            message = self.conn.lpop(_get_topic_partition_data_key(self.topic, self._partition))
            if not message:
                self._detach()
                return self.poll()
            
            if self.serializer:
                message = self.serializer.deserialize(message)
            
            return message
            
        else:
            for i in range(topic_metadata.get("partitions")):
                if self.conn.get(_get_topic_partition_worker_key(self.topic, i)):
                    continue
                partition_size = self.conn.llen(_get_topic_partition_data_key(self.topic, i))
                if partition_size == 0:
                    continue

                if not self._attach(i):
                    continue

                message = self.conn.lpop(_get_topic_partition_data_key(self.topic, self._partition))
                if not message:
                    self._detach()
                    continue

                if self.serializer:
                    message = self.serializer.deserialize(message)
                
                return message

        return None

    def _detach(self):
        """
        Detach worker from the current partition. 
        """

        # self.conn.delete(_get_worker_topic_partition_key(self.id))
        topic_partition_key = _get_topic_partition_worker_key(self.topic, self._partition)
        lock = self.conn.lock(_get_lock_key(topic_partition_key), timeout=10)
        if lock.acquire(blocking=True, blocking_timeout=1):
            worker_id_bytes: bytes = self.conn.get(topic_partition_key)
            if worker_id_bytes and worker_id_bytes.decode("utf-8") == self.id:
                self.conn.delete(topic_partition_key)
            lock.release()
        
        self._partition = None

    def _attach(self, partition: int): 
        """
        Attach worker with the given partition. 
        """
        is_attached = False
        topic_partition_key = _get_topic_partition_worker_key(self.topic, partition)
        lock = self.conn.lock(_get_lock_key(topic_partition_key), timeout=10)
        if lock.acquire(blocking=True, blocking_timeout=1):
            worker_id_bytes: bytes = self.conn.get(topic_partition_key)
            if not worker_id_bytes:
                self.conn.set(topic_partition_key, self.id, ex=int(self.timeout_ms/1000))
                is_attached = True
            lock.release()

        if not is_attached:
            return False

        # self.conn.set(_get_worker_topic_partition_key(self.id), f"{self.topic}-{partition}", ex=int(self.timeout_ms * 1000))
        self._partition = partition
        return True

    def beat(self):
        """
        Implements sending heartbeat to keep the RQLite worker alive
        """
        topic_partition_key = _get_topic_partition_worker_key(self.topic, self._partition)
        lock = self.conn.lock(_get_lock_key(topic_partition_key), timeout=10)
        if lock.acquire(blocking=True, blocking_timeout=1):
            worker_id_bytes: bytes = self.conn.get(topic_partition_key)
            if worker_id_bytes:
                if worker_id_bytes.decode("utf-8") == self.id:
                    self.conn.set(topic_partition_key, self.id, ex=int(self.timeout_ms/1000))
                else:
                    self._partition = None
                    raise Exception(f"ERROR: Cannot send beat for RQLite worker '{self.id}' on queue '{self.topic}-{self._partition}'. Queue attached to a different worker.")
            else:
                self._partition = None
                raise Exception(f"ERROR: Cannot send heartbeat for RQLite worker '{self.id}' on queue '{self.topic}-{self._partition}'. Worker has timed out.")
            lock.release()
        else:
            raise Exception(f"ERROR: Cannot obtain lock on key '{topic_partition_key}' in Redis.")

        return True

    def commit(self):
        """
        Implements commit for consuming the message. 
        Currently only implements sending a heartbeat
        """
        if not self._partition:
            return
        self.beat()
        
    def run(self):
        while self._running:
            try:
                msg = self.poll()

                if not msg:
                    time.sleep(self.poll_timeout_ms / 1000.)
                    continue

                self._processing = True
                if self.callback:
                    self.callback(msg, heartbeat=self.beat)
                logging.info(f"Worker {self.id} finished processing message from queue {self.topic}-{self._partition}")
            except Exception as ex:
                traceback.print_exc()
                logging.error(f"{type(ex).__name__}: {str(ex)}. Failed to run callback function '{self.callback.__name__}' for message '{msg}'.")
            finally:
                self.commit()
                self._processing = False

    def start(self):
        """
        Start worker on a background thread
        """
        logging.info(f"Staring RQLite Worker '{self.id}' listening on topic '{self.topic}'")
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._running = True
        self._thread.start()

    def stop(self, safe: bool = True):
        """
        Stop worker thread
        - safe: Waits for current message to finish processing and detach worker before stopping.
        """
        logging.info(f"Stopping RQLite Worker '{self.id}' listening on topic '{self.topic}'")
        self._running = False
        if safe:
            self._thread.join()
        self._detach()


class RQWorkerPool:
    def __init__(self, 
                host: str = "localhost",
                port: int = 6379, 
                username: str = None, 
                password: str = None, 
                redis_conn: Redis = None, 
                topic: str = None, 
                callback: Callable = None, 
                serializer = None,
                timeout_ms: int = 10000,
                poll_timeout_ms: int = 1000, 
                num_workers: int = 1):
        """
        Initialize RQLite Worker Pool
        - host: Hostname for Redis server
        - port: Port where redis server is listening
        - username: Username of Redis server (if any)
        - password: Password of Redis server (if any)
        - redis_conn: Redis Connection object from Redis library
        - topic: Name of the topic to listen
        - callback: Callback function to execute on the message polled
        - serializer: Serializer class with deserialize method for the message
        - timeout_ms: Timeout for the worker in milliseconds
        - poll_timeout_ms: Sleep time for polling if no messages available
        - num_workers: Number of concurrent workers on the same topic
        """
        if not topic:
            raise Exception(f"Error: RQLite worker topic cannot be None")
        
        self.id = _gen_id()
        self.topic = topic        
        self.callback = callback
        self.serializer = serializer
        
        if redis_conn:
            self.conn = redis_conn
        else:
            self.conn = Redis(host=host, port=port, username=username, password=password)

        self.timeout_ms = timeout_ms
        self.poll_timeout_ms = poll_timeout_ms

        self._running = False

        self.workers: list[RQWorker] = [RQWorker(redis_conn=self.conn, topic=topic, callback=callback, serializer=serializer, timeout_ms=timeout_ms, poll_timeout_ms=poll_timeout_ms) for _ in range(num_workers)]

    def start(self):
        logging.info(f"Starting RQLite worker pool '{self.id}' listening on topic '{self.topic}'")
        self._running = True
        for worker in self.workers:
            worker.start()
    
    def stop(self):
        logging.info(f"Stopping RQLite worker pool '{self.id}' listening on topic '{self.topic}'")
        for worker in self.workers:
            worker.stop()
        self._running = False