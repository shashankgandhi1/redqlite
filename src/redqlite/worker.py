from redis import Redis
from redis import exceptions as RedisExceptions
from typing import Callable
import threading
import time
import logging
# import traceback

from .utils import _gen_id, get_topic_meta, _create_topic, _validate_topic
from .config import _get_topic_partition_data_key, _get_topic_partition_worker_key, _get_lock_key, _get_topic_partition_processing_key, _get_topic_dlq_key
from .config import REDIS_TIMEOUT_RETRY_INIT, REDIS_TIMEOUT_RETRY_MAX
from .serializers import JsonSerializer
from .scripts import LUAREDIS_LPOP_TO_KEY, LUAREDIS_KEY_TO_RPUSH
from .errors import ConnectionError

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
                 poll_timeout_ms: int = 1000,
                 max_retries: int = 2):
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
        - max_retries: Number of retries if worker times out processing a message
        """
        if not topic:
            raise Exception("Error: RQLite worker topic cannot be None")

        if not _validate_topic(topic):
            raise Exception(f"ERROR: RQLite worker topic '{topic}' invalid. Topic name can only be alphanumeric")
        
        if timeout_ms < 2 * poll_timeout_ms:
            raise Exception("ERROR: RQLite worker cannot be initialized. Worker timeout_ms should be >= twice the poll_timeout_ms")

        self.id = _gen_id()
        self.topic = topic
        self.callback = callback
        self.serializer = serializer
        self.max_retries = max_retries
        
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
            topic_metadata = _create_topic(self.conn, self.topic)
        
        # Fetch number of messages in all partition queues
        if self._partition is not None:
            self.beat()
            
            topic_partition_data_key = _get_topic_partition_data_key(self.topic, self._partition)
            topic_partition_proc_key = _get_topic_partition_processing_key(self.topic, self._partition)
            if self.conn.llen(topic_partition_data_key) == 0 and self.conn.get(topic_partition_proc_key) is None:
                self._detach()
                return self.poll()

            # message = self.conn.lpop(_get_topic_partition_data_key(self.topic, self._partition))
            self._move_from_processing_to_partition()
            message = self._move_from_partition_to_processing()
            
            if not message:
                self._detach()
                return self.poll()
            
            return message
            
        else:
            for i in range(topic_metadata.get("partitions")):
                if self.conn.get(_get_topic_partition_worker_key(self.topic, i)):
                    continue

                topic_partition_data_key = _get_topic_partition_data_key(self.topic, i)
                topic_partition_proc_key = _get_topic_partition_processing_key(self.topic, i)
                if self.conn.llen(topic_partition_data_key) == 0 and self.conn.get(topic_partition_proc_key) is None:
                    continue

                if not self._attach(i):
                    continue

                # message = self.conn.lpop(_get_topic_partition_data_key(self.topic, self._partition))
                self._move_from_processing_to_partition()
                message = self._move_from_partition_to_processing()
                
                if not message:
                    self._detach()
                    continue
                
                return message

        return None

    def _move_from_partition_to_processing(self):
        script = self.conn.register_script(LUAREDIS_LPOP_TO_KEY)
        source_key = _get_topic_partition_data_key(self.topic, self._partition)
        target_key = _get_topic_partition_processing_key(self.topic, self._partition)
        msg = script(keys=[source_key, target_key])
        return msg

    def _move_from_processing_to_partition(self):
        script = self.conn.register_script(LUAREDIS_KEY_TO_RPUSH)
        source_key = _get_topic_partition_processing_key(self.topic, self._partition)
        target_key = _get_topic_partition_data_key(self.topic, self._partition)
        msg = script(keys=[source_key, target_key])
        return msg

    def _move_to_dlq(self):
        script = self.conn.register_script(LUAREDIS_KEY_TO_RPUSH)
        source_key = _get_topic_partition_processing_key(self.topic, self._partition)
        target_key = _get_topic_dlq_key(self.topic)
        msg = script(keys=[source_key, target_key])
        return msg

    def _develope(self, msg_envelope: dict) -> bytes:
        message = msg_envelope.get("payload")
        return message.encode("utf-8")

    def _update_retries(self, msg_envelope: dict):
        if msg_envelope.get("retries") is None:
            msg_envelope["retries"] = self.max_retries
        else:
            msg_envelope["retries"] -= 1

        self.beat()
        self.conn.set(_get_topic_partition_processing_key(self.topic, self._partition), JsonSerializer.serialize(msg_envelope))

    def _detach(self):
        """
        Detach worker from the current partition. 
        """

        # self.conn.delete(_get_worker_topic_partition_key(self.id))
        topic_partition_key = _get_topic_partition_worker_key(self.topic, self._partition)
        lock = self.conn.lock(_get_lock_key(topic_partition_key), timeout=10)
        if lock.acquire(blocking=True, blocking_timeout=1):
            try:
                worker_id_bytes: bytes = self.conn.get(topic_partition_key)
                if worker_id_bytes and worker_id_bytes.decode("utf-8") == self.id:
                    self.conn.delete(topic_partition_key)
            finally:
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
            try:
                worker_id_bytes: bytes = self.conn.get(topic_partition_key)
                if not worker_id_bytes:
                    self.conn.set(topic_partition_key, self.id, ex=int(self.timeout_ms / 1000))
                    is_attached = True
            finally:
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
            try:
                worker_id_bytes: bytes = self.conn.get(topic_partition_key)
                if worker_id_bytes:
                    if worker_id_bytes.decode("utf-8") == self.id:
                        self.conn.set(topic_partition_key, self.id, ex=int(self.timeout_ms / 1000))
                    else:
                        err_msg = f"Cannot send beat for RQLite worker '{self.id}' on queue '{self.topic}-{self._partition}'. Queue attached to a different worker."
                        self._partition = None
                        raise ConnectionError(err_msg)
                else:
                    err_msg = f"Cannot send heartbeat for RQLite worker '{self.id}' on queue '{self.topic}-{self._partition}'. Worker has timed out."
                    self._partition = None
                    raise ConnectionError(err_msg)
            finally:
                lock.release()
        else:
            raise ConnectionError(f"RQLite worker '{self.id}' cannot obtain lock on key '{topic_partition_key}' in Redis.")

        return True

    def commit(self, msg_envelope: dict):
        """
        Implements commit for consuming the message. 
        Currently only implements sending a heartbeat
        """
        if self._partition is None:
            return
        self.beat()
        if msg_envelope.get("err"):
            self.conn.set(_get_topic_partition_processing_key(self.topic, self._partition), JsonSerializer.serialize(msg_envelope))
            self._move_to_dlq()
        else:
            self.conn.delete(_get_topic_partition_processing_key(self.topic, self._partition))
        return
        
    def run(self):
        redis_timeout = REDIS_TIMEOUT_RETRY_INIT
        while self._running:
            msg_envelope = None
            try:
                msg_envelope = self.poll()

                if not msg_envelope:
                    time.sleep(self.poll_timeout_ms / 1000.)
                    continue

                self._processing = True

                msg_envelope = JsonSerializer.deserialize(msg_envelope)
                msg = self._develope(msg_envelope)
                self._update_retries(msg_envelope)

                if msg_envelope.get("retries") <= 0:
                    raise Exception("ERROR: Max retries reached for the message")

                if self.serializer:
                    msg = self.serializer.deserialize(msg)
                
                if self.callback:
                    self.callback(msg, heartbeat=self.beat)
                self.commit(msg_envelope)
                
                logging.info(f"Worker {self.id} finished processing message from queue {self.topic}-{self._partition}")
            
            except ConnectionError as cex:
                logging.error(str(cex))

            except (RedisExceptions.ConnectionError, RedisExceptions.TimeoutError) as rex:
                logging.critical(f"ERROR: {type(rex).__name__}: {str(rex)}. Freezing worker {self.id} on queue {self.topic}-{self._partition} for {redis_timeout} seconds")
                time.sleep(redis_timeout)
                redis_timeout = min(redis_timeout * 2, REDIS_TIMEOUT_RETRY_MAX)
                continue

            except Exception as ex:
                # traceback.print_exc()
                if msg_envelope and isinstance(msg_envelope, dict):
                    msg_envelope["err"] = f"{type(ex).__name__}: {str(ex)}"
                    logging.error(f"{type(ex).__name__}: {str(ex)}. Worker {self.id} failed to run callback function '{self.callback.__name__}' for message '{msg}' on topic {self.topic}-{self._partition}.", exc_info=True)
                    self.commit(msg_envelope)
                else:
                    logging.error(f"{type(ex).__name__}: {str(ex)}. Worker {self.id} failed before reaching callback function. No message found", exc_info=True)
            finally:                
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
        if self._thread:
            if safe:
                self._thread.join()
            self._thread = None
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
                 max_retries: int = 2,
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
        - max_retries: Number of retries if worker times out processing a message
        - num_workers: Number of concurrent workers on the same topic
        """
        if not topic:
            raise Exception("Error: RQLite worker topic cannot be None")

        if not _validate_topic(topic):
            raise Exception(f"ERROR: RQLite worker pool topic '{topic}' invalid. Topic name can only be alphanumeric")

        if timeout_ms < 2 * poll_timeout_ms:
            raise Exception("ERROR: RQLite worker pool cannot be initialized. Worker timeout_ms should be >= twice the poll_timeout_ms")
        
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

        self.workers: list[RQWorker] = [RQWorker(redis_conn=self.conn, topic=topic, callback=callback, serializer=serializer, timeout_ms=timeout_ms, poll_timeout_ms=poll_timeout_ms, max_retries=max_retries) for _ in range(num_workers)]

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
