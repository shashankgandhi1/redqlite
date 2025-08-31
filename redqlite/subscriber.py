from redis import Redis
from typing import Callable
import threading
import time
import logging
import traceback

from .utils import _gen_id
from .config import _get_channel_subscriber_key, _get_channel_data_subscriber_key


class RQSubscriber:
	def __init__(self,
				host: str = "localhost",
				port: int = 6379,
				username: str = None,
				password: str = None,
				redis_conn: Redis = None,
				channel: str = None,
				callback: Callable = None,
				serializer = None,
				timeout_ms: int = 10000,
				poll_timeout_ms: int = 1000):
		"""
		Initialize RQLite Subscriber
		- host: Hostname for Redis server
		- port: Port where Redis server is listening
		- username: Username of Redis server (if any)
		- password: Password of Redis server (if any)
		- redis_conn: Redis Connection object from Redis library
		- channel: Name of the channel to subscribe
		- callback: Callback function executed on message poll
		- serializer: Serializer class with deserialize method for the message
        - timeout_ms: Timeout for the worker (in milliseconds)
        - poll_timeout_ms: Sleep time (in milliseconds) for polling if no messages available
		"""

		if not channel:
			raise Exception(f"Error: RQlite subscriber channel cannot be None")

		if poll_timeout_ms >= timeout_ms:
			raise Exception(f"ERROR: RQlite Subscriber's poll_timeout_ms must be less than timeout_ms")

		self.id = _gen_id()
		self.channel = channel
		self.callback = callback
		self.serializer = serializer

		if redis_conn:
			self.conn = redis_conn
		else:
			self.conn = Redis(host=host, port=port, username=username, password=password)

		self.timeout_ms = timeout_ms
		self.poll_timeout_ms = poll_timeout_ms

		self._thread = None
		self._running = None
		self._processing = None


	def poll(self):
		"""
		Impelments polling message from subscriber specific data queues
		"""
		subscriber_key = _get_channel_subscriber_key(self.channel, self.id)
		data_key = self.conn.get(subscriber_key)
		if not data_key:
			data_key = self._spawn()
		else:
			data_key = data_key.decode("utf-8")

		msg = self.conn.lpop(data_key)

		if not msg:
			return None

		if self.serializer:
			msg = self.serializer.deserialize(msg)

		return msg


	def _spawn(self):
		"""
		Implements setting RQLite Subscriber key to Redis Server for broadcast to find
		"""
		subscriber_key = _get_channel_subscriber_key(self.channel, self.id)
		data_key = _get_channel_data_subscriber_key(self.channel, self.id)

		self.conn.set(subscriber_key, data_key, ex=int(self.timeout_ms/1000))
		return data_key

	def _remove(self):
		"""
		Impelments removing RQLite Subscriber key from Redis Server
		"""
		subscriber_key = _get_channel_subscriber_key(self.channel, self.id)
		self.conn.delete(subscriber_key)
		return True


	def beat(self):
		"""
		Implements sending heartbeat to keep RQLite Subscriber alive 
		"""
		subscriber_key = _get_channel_subscriber_key(self.channel, self.id)
		is_key = self.conn.expire(subscriber_key, int(self.timeout_ms/1000))
		if not is_key:
			raise Exception(f"ERROR: Cannot send heartbeat for RQLite subscriber '{self.id}'. Subscriber has timed out")

		return True


	def commit(self):
		"""
		Implements commit for consuming a message.
		Currently only implements sending a heartbeat
		"""
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
		        logging.info(f"Subscriber '{self.id}'' finished processing message from channel '{self.channel}'")
		    except Exception as ex:
		        traceback.print_exc()
		        logging.error(f"{type(ex).__name__}: {str(ex)}. Failed to run callback function '{self.callback.__name__}' for message '{msg}'.")
		    finally:
		        self.commit()
		        self._processing = False



	def start(self):
		"""
		Start subscriber on a background thread 
		"""
		logging.info(f"Staring RQLite Subscriber '{self.id}' listening on channel '{self.channel}'")
		self._thread = threading.Thread(target=self.run, daemon=True)
		self._running = True
		self._thread.start()


	def stop(self, safe: bool = True):
		"""
        Stop subscriber thread
        - safe: Waits for current message to finish processing and remove subscriber before stopping.
        """
		logging.info(f"Stopping RQLite Subscriber '{self.id}' listening on channel '{self.channel}'")
		self._running = False
		if safe:
		    self._thread.join()
		self._remove()
