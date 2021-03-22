import unittest
import time

from ratelimiter import RateLimiter, InMemoryContext


def call_resource(ratelimiter: RateLimiter, delay: float = 0.0):
    with ratelimiter:
        if delay > 0:
            time.sleep(delay)


class TestRateLimiter(unittest.IsolatedAsyncioTestCase):

    def test_sync(self):
        ratelimter = RateLimiter(InMemoryContext, max_calls=100, period=1.0)

        calls = []

        count = 1000
        for i in range(count):
            call_resource(ratelimter)
            calls.append(time.time())

        elapsed = calls[-1] - calls[0]

        rate = count/elapsed
        expectedRate = 100

    def test_use_cost(self):
        ratelimter = RateLimiter(InMemoryContext, max_calls=50, period=1.0)

        calls = []

        count = 100
        for i in range(count):
            with ratelimter.use_cost(2):
                pass

            calls.append(time.time())

        elapsed = calls[-1] - calls[0]

        rate = count/elapsed
        expectedRate = 100

    async def test_async(self):
        import asyncio

        ratelimter = RateLimiter(InMemoryContext, max_calls=50, period=1.0)

        calls = []

        count = 100
        for i in range(count):
            async with ratelimter.use_cost(2):
                await asyncio.sleep(0.001)

            calls.append(time.time())

        elapsed = calls[-1] - calls[0]

        rate = count/elapsed
        expectedRate = 100
