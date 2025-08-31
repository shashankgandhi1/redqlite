from redis import Redis

class SafeRedis(Redis):
    def __init__(self, *args, lock_timeout: int = 10, blocking_timeout: int = 1, **kwargs):
        super().__init__(*args, **kwargs)
        self.lock_timeout = lock_timeout
        self.blocking_timeout = blocking_timeout

    def _get_lock(self, key):
        return self.lock(f"lock:{key}", timeout=self.lock_timeout)
    
    def safe_set(self, key, value, ex: int = None):
        lock = self._get_lock(key)
        if lock.acquire(blocking=True, blocking_timeout=self.blocking_timeout):
            try:
                self.set(key, value, ex=ex)
                return True
            finally:
                lock.release()
        else:
            return False
    
    def safe_delete(self, key):
        lock = self._get_lock(key)
        if lock.acquire(blocking=True, blocking_timeout=self.blocking_timeout):
            try:
                self.delete(key)
                return True
            finally:
                lock.release()
        else:
            return False