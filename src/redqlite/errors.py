class ConnectionError(Exception):
	def __init__(self, msg: str):
		super().__init__(f"Connection Error: {msg}")
