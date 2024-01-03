from asyncio import QueueFull
from queue import Queue


class IndicatorQueue(Queue):
    def __init__(self):
        self.maxsize = 1
        self.trigger = False
        super().__init__(maxsize=self.maxsize)

    def put(self, item):
        if self.qsize() >= self.maxsize:
            raise QueueFull("model job is busy")
        super().put(item)

    def is_full(self):
        return self.full()
