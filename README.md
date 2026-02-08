# RedQLite

A lightweight Messaging queue implementation using Redis for producer-worker-subscriber messaging.

---

## Features

- Redis-backed message queuing
- Producer and worker interface
- Producer and subscriber interface
- Lightweight and fast alternative to Kafka/Celery
- Works for long running tasks
- Provides configurable heartbeats from inside worker callback functions
- Redis server required
- Minimal setup — no broker required


---

## Installation

```bash
pip install redqlite
```

## How It Works
This package simulates a basic messaging system using Redis lists.
It includes:

A **Producer** that pushes messages to a Redis queue or broadcasts messages to all subscribers.

A **Worker** that consumes messages from the queue and processes them.

A **Worker Pool** to run concurrent workers. 

A **Subscriber** that consumes messages from channel specific message queue.

## Usage
### Example: Producing a Message
```python
# Import RQProducer class
from redqlite.producer import RQProducer

# Create a producer instance
rqproducer = RQProducer(host="localhost", port=6379)

# Send a message
rqproducer.send(topic="yourtopicname", message="hello world")

# Broadcast message on a channel
rqproducer.broadcast(channel="yourchannelname", message="hello world")
```

### Example: Starting a Worker
```python
# Import RQWorker class
from redqlite.worker import RQWorker

def callback_func(msg, **kwargs):
	print(msg)

# Create a worker instance
## Provide your redis connection details or Redis connection pool object
rqworker = RQWorker(host="localhost", port=6379, topic="yourtopicname", callback=callback_func)

# Start worker thread
rqworker.start()

```

### Example: Starting a Worker Pool
```python
# Import RQWorkerPool class
from redqlite.worker import RQWorkerPool

def callback_func(msg, **kwargs):
	print(msg)

# Create a worker pool
## Provide your redis connection details or Redis connection pool object
rqworker_pool = RQWorkerPool(host="localhost", port=6379, topic="yourtopicname", callback=callback_func, num_workers=2)

# Start worker pool
rqworker_pool.start()
```

### Example: Starting a subscriber
```python
# Import QSubscriber class
from redqlite.subscriber import RQSubscriber
from redis import Redis

def callback_func(msg, **kwargs):
	print(msg)

# Create a worker instance
rqsubscriber = RQSubscriber(redis_conn=Redis(), channel="yourchannelname", callback=callback_func)

# Start subscriber thread
rqsubscriber.start()

```

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you’d like to change.

## Contact
Feel free to reach out by [opening an issue](https://github.com/shashankgandhi1/redqlite/issues) or by [contacting the author](https://github.com/shashankgandhi1).


