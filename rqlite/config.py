KEY_PREFIX = "_rqlite"

PRODUCER_LABEL = "producer"
WORKER_LABEL = "worker"
TOPIC_LABEL = "topic"
CHANNEL_LABEL = "channel"
SUBSCRIBER_LABEL = "subscriber"

META_SUFFIX = "meta"
DATA_SUFFIX = "data"

_get_topic_meta_key = lambda topic: f"{KEY_PREFIX}-{TOPIC_LABEL}-{topic}-{META_SUFFIX}"
_get_topic_partition_data_key = lambda topic, partition: f"{KEY_PREFIX}-{TOPIC_LABEL}-{topic}-{partition}-{DATA_SUFFIX}"
_get_topic_partition_worker_key = lambda topic, partition: f"{KEY_PREFIX}-{TOPIC_LABEL}-{topic}-{partition}-{WORKER_LABEL}"
_get_worker_topic_partition_key = lambda worker: f"{KEY_PREFIX}-{WORKER_LABEL}-{worker}-{TOPIC_LABEL}"
_get_lock_key = lambda key: f"{KEY_PREFIX}-lock:{key}"
_get_channel_subscriber_pre = lambda channel: f"{KEY_PREFIX}-{CHANNEL_LABEL}-{channel}-{SUBSCRIBER_LABEL}-"
_get_channel_subscriber_key = lambda channel, subscriber: f"{KEY_PREFIX}-{CHANNEL_LABEL}-{channel}-{SUBSCRIBER_LABEL}-{subscriber}"
_get_channel_data_subscriber_key = lambda channel, subscriber: f"{KEY_PREFIX}-{CHANNEL_LABEL}-{channel}-data-{subscriber}"