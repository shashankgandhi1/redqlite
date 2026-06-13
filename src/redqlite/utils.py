from redis import Redis
import json
import hashlib
from uuid import uuid4
from typing import Callable
import re

from .serializers import JsonSerializer
from .config import _get_lock_key, _get_topic_meta_key, _get_topic_partition_data_key, _get_topic_partition_worker_key, _get_topic_partition_processing_key, _get_topic_dlq_key
from .config import TOPIC_REGEX, CHANNEL_REGEX

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

def get_topic_meta(redis_conn: Redis, topic: str):
    topic_metadata: bytes = redis_conn.get(_get_topic_meta_key(topic))
    if topic_metadata:
        topic_metadata = json.loads(topic_metadata.decode())
    return topic_metadata

def _create_topic(redis_conn: Redis, topic: str, num_partitions: int):
    metadata = {
        "name": topic,
        "partitions": num_partitions
    }
    redis_conn.set(_get_topic_meta_key(topic), json.dumps(metadata))
    return metadata

def create_topic(redis_conn: Redis, topic: str, num_partitions: int = 1):
    if not _validate_topic(topic):
        raise Exception(f"ERROR: Cannot create topic '{topic}'. Topic name can only be alphanumeric")

    if get_topic_meta(redis_conn, topic):
        raise Exception(f"ERROR: Cannot create new topic '{topic}'. Topic already exists.")

    return _create_topic(redis_conn, topic, num_partitions)

def _update_topic(redis_conn: Redis, topic: str, num_partitions: int):
    metadata = {
        "name": topic,
        "partitions": num_partitions
    }
    redis_conn.set(_get_topic_meta_key(topic), json.dumps(metadata))
    return metadata

# def _remove_partition(redis_conn: Redis, topic: str):
#     is_removed = False
#     topic_meta_key = _get_topic_meta_key(topic)

#     lock = redis_conn.lock(_get_lock_key(topic_meta_key), timeout=10)
#     if lock.acquire(blocking=True, blocking_timeout=1):
#         try:
#             topic_metadata = get_topic_meta(redis_conn, topic)
#             if topic_metadata:
#                 num_partitions = topic_metadata.get("partitions")
#                 remove_partition = num_partitions - 1
#                 topic_partition_data_key = _get_topic_partition_data_key(topic, remove_partition)
#                 topic_partition_proc_key = _get_topic_partition_processing_key(topic, remove_partition)

#                 if redis_conn.llen(topic_partition_data_key) == 0 and redis_conn.get(topic_partition_proc_key) is None:
#                     topic_metadata["partitions"] -= 1
#                     redis_conn.set(topic_meta_key, json.dumps(topic_metadata))
#                     is_removed = True
#         finally:
#             lock.release()

#     return is_removed

def update_topic(redis_conn: Redis, topic: str, num_partitions: int):
    topic_metadata = get_topic_meta(redis_conn, topic)
    if not topic_metadata:
        raise Exception(f"ERROR: Cannot update topic '{topic}'. Topic doesn't exist")

    if num_partitions > topic_metadata.get("partitions"):
        topic_metadata = _update_topic(redis_conn, topic, num_partitions)
    
    elif num_partitions < topic_metadata.get("partitions"):
        raise Exception(f"ERROR: Cannot remove partitions from topic '{topic}'. Disallowed")

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

def _find_all_topics(redis_conn: Redis):
    topics = []

    meta_key_pattern = _get_topic_meta_key("*")
    meta_key_regex = _get_topic_meta_key("(.*)")

    for key in redis_conn.scan_iter(match=meta_key_pattern):
        topic_match = re.match(meta_key_regex, key.decode("utf-8"))
        if topic_match:
            topics.append(topic_match.group(1))
    return topics

def monitor_topic(redis_conn: Redis, topic: str, expand: bool = False):
    meta = get_topic_meta(redis_conn, topic)
    if not meta:
        return {}

    stats = {}
    num_partitions = meta.get("partitions", 0)
    stats["partitions"] = []

    for partition in range(num_partitions):
        partition_stats = {"worker_id": None, "pending": {"count": 0}, "processing": {"is_processing": None}}
        
        data_key = _get_topic_partition_data_key(topic, partition)
        worker_key = _get_topic_partition_worker_key(topic, partition)
        processing_key = _get_topic_partition_processing_key(topic, partition)

        worker_id_bytes = redis_conn.get(worker_key)
        worker_id = worker_id_bytes.decode("utf-8") if worker_id_bytes else None
        partition_stats["worker_id"] = worker_id

        pending_count = redis_conn.llen(data_key)
        partition_stats["pending"]["count"] = pending_count

        processing_raw = redis_conn.get(processing_key)
        is_processing = True if processing_raw else False
        partition_stats["processing"]["is_processing"] = is_processing

        if expand:
            if processing_raw:
                partition_stats["processing"]["message"] = JsonSerializer.deserialize(processing_raw)
            else:
                partition_stats["processing"]["message"] = None

        stats["partitions"].append(partition_stats)

    stats["dlq"] = {"count": 0}
    dlq_key = _get_topic_dlq_key(topic)
    dlq_count = redis_conn.llen(dlq_key)
    stats["dlq"]["count"] = dlq_count

    if expand:
        stats["dlq"]["messages"] = []
        if dlq_count > 0:
            dlq_messages_raw = redis_conn.lrange(dlq_key, 0, -1)
            for message in dlq_messages_raw:
                stats["dlq"]["messages"].append(JsonSerializer.deserialize(message))

    return stats

def monitor(redis_conn: Redis, expand: bool = False):
    topics = _find_all_topics(redis_conn)

    stats = {"topics": {}, "channels": {}}
    for topic in topics:
        stats["topics"][topic] = monitor_topic(redis_conn, topic, expand=expand)

    return stats
