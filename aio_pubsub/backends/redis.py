import json

from aio_pubsub.interfaces import PubSub, Subscriber
from aio_pubsub.typings import Message

aioredis_installed = False
try:
    import aioredis 

    aioredis_installed = True
except ImportError:
    pass  # pragma: no cover


class RedisSubscriber(Subscriber):
    def __init__(self, sub):
        self.sub = sub

    def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self.sub.__anext__()
        return msg["data"]



class RedisPubSub(PubSub):
    def __init__(self, url: str) -> None:
        if aioredis_installed is False:
            raise RuntimeError("Please install `aioredis`")  # pragma: no cover

        self.url = url
        self.connection = None

    async def publish(self, channel: str, message: Message) -> None:
        if self.connection is None:
            self.connection = await aioredis.Redis.from_url(self.url)

        channels = await self.connection.pubsub_channels(channel)   
        for channel in channels:
            await self.connection.publish(channel, message)

    async def subscribe(self, channel) -> "RedisSubscriber":
        if aioredis_installed is False:
            raise RuntimeError("Please install `aioredis`")  # pragma: no cover

        conn = await aioredis.Redis.from_url(self.url, encoding="utf-8", decode_responses=True)
        sub = conn.pubsub(ignore_subscribe_messages=True)

        await sub.subscribe(channel)
        return RedisSubscriber(sub.listen())


    
