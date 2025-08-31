# RQLite

A lightweight Messaging queue implementation using Redis for producer-worker messaging.

---

## Features

- Redis-backed message queuing
- Producer and worker interface
- Lightweight and fast alternative to Kafka
- Works for long running tasks
- Provides configurable heartbeats from inside worker callback functions
- Redis server required
- Minimal setup — no Kafka or Zookeeper required

---

## Installation

```bash
pip install rqlite
```

## How It Works
This package simulates a basic Kafka-style messaging system using Redis lists.
It includes:

A **Producer** that pushes messages to a Redis queue or broadcasts messages to all subscribers.

A **Worker** that consumes messages from the queue and processes them.

A **Worker Pool** to run concurrent workers. 

A **Subscriber** that consumes messages from channel specific message queue.

## Usage
### Example: Producing a Message
```python
# Import QProducer class
from qlite.producer import QProducer

# Create a producer instance (assumes redis running on localhost:6379)
qproducer = QProducer()

# Send a message
qproducer.send(topic="your_topic_name", message="hello world")

# Broadcast message on a channel
qproducer.broadcast(channel="your_channel_name", message="hello world")
```

### Example: Starting a Worker
```python
# Import QWorker class
from qlite.worker import QWorker

def callback_func(msg, **kwargs):
	print(msg)

# Create a worker instance (assumes redis running on localhost:6379)
qworker = QWorker(topic="your_topic_name", callback=callback_func)

# Start worker thread
qworker.start()

```

### Example: Starting a subscriber
```python
# Import QSubscriber class
from qlite.subscriber import QSubscriber

def callback_func(msg, **kwargs):
	print(msg)

# Create a worker instance (assumes redis running on localhost:6379)
qsubscriber = QSubscriber(channel="your_channel_name", callback=callback_func)

# Start subscriber thread
qsubscriber.start()

```

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you’d like to change.

## Contact
Created by **Shashank Gandhi** – feel free to reach out by [opening an issue](https://github.com/shashankgandhi1/qlite/issues) or by [contacting the author](https://github.com/shashankgandhi1).


