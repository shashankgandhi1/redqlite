import json

class JsonSerializer:
    @staticmethod
    def serialize(data: dict):
        return json.dumps(data).encode("utf-8") if data else None
    
    @staticmethod
    def deserialize(data: bytes):
        return json.loads(data.decode("utf-8")) if data else None
    

class StringSerializer:
    @staticmethod
    def serialize(key: str):
        return str(key).encode("utf-8") if key else None
    
    @staticmethod
    def deserialize(key: bytes):
        return key.decode("utf-8") if key else None
