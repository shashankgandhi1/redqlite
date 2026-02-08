KEY_PREFIX = "_rqlite"

PRODUCER_LABEL = "producer"
WORKER_LABEL = "worker"
TOPIC_LABEL = "topic"
CHANNEL_LABEL = "channel"
SUBSCRIBER_LABEL = "subscriber"
MESSAGE_LABEL = "msg"

META_SUFFIX = "meta"
DATA_SUFFIX = "data"
PROCESSING_SUFFIX = "proc"
DLQ_SUFFIX = "dlq"

TOPIC_REGEX = "[A-Za-z0-9]+"
CHANNEL_REGEX = "[A-Za-z0-9]+"

def _get_topic_meta_key(topic):
	return f"{KEY_PREFIX}-{TOPIC_LABEL}-{topic}-{META_SUFFIX}"

def _get_topic_partition_data_key(topic, partition):
	return f"{KEY_PREFIX}-{TOPIC_LABEL}-{topic}-{partition}-{DATA_SUFFIX}"

def _get_topic_partition_worker_key(topic, partition):
	return f"{KEY_PREFIX}-{TOPIC_LABEL}-{topic}-{partition}-{WORKER_LABEL}"

def _get_worker_topic_partition_key(worker):
	return f"{KEY_PREFIX}-{WORKER_LABEL}-{worker}-{TOPIC_LABEL}"

def _get_lock_key(key):
	return f"{KEY_PREFIX}-lock:{key}"

def _get_channel_subscriber_pre(channel):
	return f"{KEY_PREFIX}-{CHANNEL_LABEL}-{channel}-{SUBSCRIBER_LABEL}-"

def _get_channel_subscriber_key(channel, subscriber):
	return f"{KEY_PREFIX}-{CHANNEL_LABEL}-{channel}-{SUBSCRIBER_LABEL}-{subscriber}"

def _get_channel_data_subscriber_key(channel, subscriber):
	return f"{KEY_PREFIX}-{CHANNEL_LABEL}-{channel}-data-{subscriber}"

def _get_msg_id(id: str):
	return f"{MESSAGE_LABEL}-{id}"

def _get_topic_partition_processing_key(topic, partition):
	return f"{KEY_PREFIX}-{TOPIC_LABEL}-{topic}-{partition}-{PROCESSING_SUFFIX}"

def _get_topic_dlq_key(topic):
	return f"{KEY_PREFIX}-{TOPIC_LABEL}-{topic}-{DLQ_SUFFIX}"
