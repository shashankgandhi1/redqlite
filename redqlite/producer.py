from redis import Redis
import json

from .config import _get_topic_partition_data_key, _get_channel_subscriber_pre
from .utils import create_topic, get_topic_meta, _get_partition
from .redis import SafeRedis

class RQProducer:
    """
    RQLite Producer:
    """
    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 6379, 
                 username: str = None, 
                 password: str = None,
                 redis_conn: SafeRedis = None,
                 serializer = None):
        
        """
        Initialize the RQLite Producer with 
        - host: Hostname for Redis server
        - port: Port where redis server is listening
        - username: Username of Redis server (if any)
        - password: Password of Redis server (if any)
        - redis_conn: Redis Connection object from Redis library
        - serializer: Serializer class with serialize method for the message
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.serializer = serializer

        if redis_conn:
            self.conn = redis_conn
        else:
            self.conn = SafeRedis(host=host, port=port, username=username, password=password)

        if not self._test_conn():
            raise Exception(f"Could not connect to Redis at {host}:{port}.")
    
    def _test_conn(self):
        """
        Implements test redis connection.
        """
        return self.conn.ping()
    
    def send(self, topic: str, message, partition_key = None):
        """
        Implements send message to Redis list with the name _rqlite-topic-{topic}-{partition}-data.
        Topic is created if topic does not exist.
        Partition is decided based on partition_key. Default partition is 0.
        Message is RPUSH'ed to Redis list
        """
        try:
            if self.serializer:
                message = self.serializer.serialize(message)
        except Exception as ex:
            raise Exception(f"Error serializing the message '{message}'")
        
        topic_metadata: dict = get_topic_meta(self.conn, topic)
        if not topic_metadata:
            topic_metadata = create_topic(self.conn, topic)
        
        if not topic_metadata:
            raise Exception(f"Error creating topic '{topic}' in Redis.")
        
        partition = _get_partition(partition_key, topic_metadata.get("partitions", 1))

        self.conn.rpush(_get_topic_partition_data_key(topic, partition), message)     
        
        return True


    def broadcast(self, channel: str, message):
        """
        Implements broadcast message to all Redis lists corresponding to subscriber with the prefix _rqlite-channel-{channel}-subscriber.
        Message is RPUSH'ed to Redis list
        """

        try:
            if self.serializer:
                message = self.serializer.serialize(message)

        except Exception as ex:
            raise Exception(f"Error serializing the message '{message}")

        channel_subscriber_keys = [key.decode("utf-8") for key in self.conn.keys(f"{_get_channel_subscriber_pre(channel)}*")]

        for subscriber_key in channel_subscriber_keys:
            channel_data_subscriber_key = self.conn.get(subscriber_key)
            if not channel_data_subscriber_key:
                continue
            channel_data_subscriber_key = channel_data_subscriber_key.decode("utf-8")
            self.conn.rpush(channel_data_subscriber_key, message)

        return True

