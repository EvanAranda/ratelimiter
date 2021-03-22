import asyncio
import collections
import threading
import time
import functools
import abc
from typing import Type, Union


class Context(abc.ABC):
    def __init__(self, *, max_calls: int, period=1.0):
        """Initialize a RateLimiter object which enforces as much as max_calls
        operations on period (eventually floating) number of seconds.
        """
        if period <= 0:
            raise ValueError('Rate limiting period should be > 0')
        if max_calls <= 0:
            raise ValueError('Rate limiting number of calls should be > 0')

        self.period = period
        self.max_calls = max_calls

    @property
    @abc.abstractmethod
    def has_exceeded_max_calls(self) -> bool:
        '''True when the current number of calls exceeds max_calls.'''

    @property
    @abc.abstractmethod
    def elapsed_time(self) -> float:
        '''The time elapsed between the last call and the first call'''

    @abc.abstractmethod
    def record_call(self, t: float, weight=1):
        '''Record the time of the resource usage'''

    @abc.abstractmethod
    def discard_call(self):
        '''Slide the window'''


class InMemoryContext(Context):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # We're using a deque to store the last execution timestamps, not for
        # its maxlen attribute, but to allow constant time front removal.
        self.calls = collections.deque()
        self.counter = 0

    @property
    def has_exceeded_max_calls(self):
        return self.counter >= self.max_calls

    @property
    def elapsed_time(self):
        return self.calls[-1][0] - self.calls[0][0]

    def record_call(self, t: float, weight=1):
        self.calls.append([t, weight])
        self.counter += weight

    def discard_call(self):
        if self.calls[0][1] == 1:
            self.calls.popleft()
        else:
            self.calls[0][1] -= 1

        self.counter -= 1


class RedisContext(Context):
    def __init__(self, *, host: str, **kwargs):
        super().__init__(**kwargs)

        import redis
        self._redis = redis.Redis(host=host)

    def has_exceeded_max_calls(self):
        raise NotImplementedError()

    def elapsed_time(self):
        raise NotImplementedError()

    def record_call(self, t, weight):
        raise NotImplementedError()

    def discard_call(self):
        raise NotImplementedError()


class RateLimiter(object):

    """Provides rate limiting for an operation with a configurable number of
    requests for a time period.
    """

    def __init__(self, context: Union[Context, Type[Context]] = Context, **kwargs):
        if (isinstance(context, type)):
            self._context = context(**kwargs)
        else:
            self._context = context

        self._lock = threading.Lock()
        self._alock = None
        self._weight = 1

        # Lock to protect creation of self._alock
        self._init_lock = threading.Lock()

    def __call__(self, f):
        """The __call__ function allows the RateLimiter object to be used as a
        regular function decorator.
        """
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            with self:
                return f(*args, **kwargs)
        return wrapped

    def use_cost(self, cost: int) -> 'RateLimiter':
        self._weight = cost
        return self

    def __enter__(self):
        with self._lock:
            # We want to ensure that no more than max_calls were run in the allowed
            # period. For this, we store the last timestamps of each call and run
            # the rate verification upon each __enter__ call.
            if self._context.has_exceeded_max_calls:
                until = time.time() + self._context.period - self._context.elapsed_time
                sleeptime = until - time.time()
                if sleeptime > 0:
                    time.sleep(sleeptime)
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._lock:
            # Store the last operation timestamp.
            self._context.record_call(time.time(), self._weight)

            # Pop the timestamp list front (ie: the older calls) until the sum goes
            # back below the period. This is our 'sliding period' window.
            while self._context.elapsed_time >= self._context.period:
                self._context.discard_call()

            # weight gets reset to default
            self._weight = 1

    def _init_async_lock(self):
        with self._init_lock:
            if self._alock is None:
                self._alock = asyncio.Lock()

    async def __aenter__(self):
        if self._alock is None:
            self._init_async_lock()

        async with self._alock:
            # We want to ensure that no more than max_calls were run in the allowed
            # period. For this, we store the last timestamps of each call and run
            # the rate verification upon each __enter__ call.
            if self._context.has_exceeded_max_calls:
                until = time.time() + self._context.period - self._context.elapsed_time
                sleeptime = until - time.time()
                if sleeptime > 0:
                    await asyncio.sleep(sleeptime)
            return self

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.coroutine(self.__exit__)(exc_type, exc, tb)
        # await asyncio.coroutine(self.__exit__)
