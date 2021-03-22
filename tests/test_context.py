import unittest
import time

from ratelimiter import InMemoryContext


class TestInMemoryContext(unittest.TestCase):

    def test_has_exceeded_max_calls_true(self):
        context = InMemoryContext(max_calls=1, period=1.0)
        context.record_call(time.time())
        context.record_call(time.time())

        assert context.has_exceeded_max_calls

    def test_has_exceeded_max_calls_false(self):
        context = InMemoryContext(max_calls=2, period=1.0)
        context.record_call(time.time())
        context.record_call(time.time())

        assert context.has_exceeded_max_calls

    def test_elapsed_time(self):
        context = InMemoryContext(max_calls=1, period=1.0)

        t1 = time.time()
        dt = 0.5
        t2 = t1 + dt

        context.record_call(t1)
        context.record_call(t2)

        assert context.elapsed_time == dt

    def test_discard_call(self):
        context = InMemoryContext(max_calls=2, period=1.0)

        t1 = time.time()
        t2 = t1 + 0.5

        context.record_call(t1)
        context.record_call(t2, 2)  # call resource with extra cost of 2

        assert len(context.calls) == 2

        context.discard_call()
        assert len(context.calls) == 1

        context.discard_call()
        assert len(context.calls) == 1


if __name__ == '__main__':
    unittest.main()
