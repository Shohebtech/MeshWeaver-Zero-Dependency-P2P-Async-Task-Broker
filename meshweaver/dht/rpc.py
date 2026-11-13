import asyncio
from typing import Callable, Dict, Optional

from .messages import Message, encode

DEFAULT_TIMEOUT = 2.0  


class RPCTimeout(Exception):
    """The peer did not answer in time."""


class PendingRequests:

    def __init__(self, timeout: float = DEFAULT_TIMEOUT):
        self.timeout = timeout
        self._pending: Dict[str, "asyncio.Future[Message]"] = {}

    def __len__(self) -> int:
        return len(self._pending)

    async def request(self, message: Message, send: Callable[[bytes], None],
                      timeout: Optional[float] = None) -> Message:
        
        if message.msg_id in self._pending:
            raise ValueError("duplicate request id")
        future = asyncio.get_running_loop().create_future()
        self._pending[message.msg_id] = future
        wait = self.timeout if timeout is None else timeout
        try:
            send(encode(message))
            return await asyncio.wait_for(future, wait)
        except asyncio.TimeoutError as e:
            raise RPCTimeout(f"no reply to {message.type} within {wait}s") from e
        finally:
            self._pending.pop(message.msg_id, None)  
    def resolve(self, response: Message) -> bool:
    
        if not response.is_response:
            return False
        future = self._pending.get(response.msg_id)
        if future is None or future.done():
            return False  
        future.set_result(response)
        return True