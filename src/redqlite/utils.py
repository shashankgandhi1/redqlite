from redis import Redis
import json
import hashlib
from uuid import uuid4
from typing import Callable
import re

from .config import _get_topic_meta_key, TOPIC_REGEX, CHANNEL_REGEX

def _validate_topic(topic: str) -> bool:
    if not topic:
        return False

    if re.fullmatch(TOPIC_REGEX, topic):
        return True

    return False

def _validate_channel(channel: str) -> bool:
    if not channel:
        return False

    if re.fullmatch(CHANNEL_REGEX, channel):
        return True

    return False


def create_topic(redis_conn: Redis, topic: str, num_partitions: int = 1):
    if not _validate_topic(topic):
        raise Exception(f"ERROR: Cannot create topic '{topic}'. Topic name can only be alphanumeric")

    metadata = {
        "name": topic,
        "partitions": num_partitions
    }
    try:
        redis_conn.set(_get_topic_meta_key(topic), json.dumps(metadata))
    except:
        raise Exception(f"ERROR: Could not create RQLite topic '{topic}' in Redis")
    
    return metadata

def get_topic_meta(redis_conn: Redis, topic: str):
    topic_metadata: bytes = redis_conn.get(_get_topic_meta_key(topic))
    if topic_metadata:
        topic_metadata = json.loads(topic_metadata.decode())
    return topic_metadata

def _hash_md5(key: str):
    if not key:
        raise ValueError(f"ERROR: Invalid key '{key}'")
    
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

def _get_partition(partition_key: str, num_partitions: int, hash_func: Callable[[str], int] = _hash_md5):
    if not partition_key:
        return 0
    partition_key = str(partition_key)
    return hash_func(partition_key) % num_partitions
    
def _gen_id():
    return hashlib.md5(str(uuid4()).encode("utf-8")).hexdigest()
