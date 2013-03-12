#events.py
import sys
import logging
logger = logging.getLogger(__name__)


from .basiclock import BasicLock

# Get the best wallclock time to use.
if sys.platform == "win32":
    elapsed_time = time.clock
else:
    # Time.clock reports CPU time on unix, not good.
    elapsed_time = time.time

"""
Manage event queues and other things
"""

# A event queue class.
class EventQueue(object):
    def __init__(self):
        self.queue = []   # A heapq for events
        self.lock = BasicLock()

    def reschedule(self, delta_t):
        """
        Apply a delta-t to all timed events
        """
        self.queue = [(t+delta_t, what) for t, what in self.queue]

    def schedule_at(self, what, when):
        """
        Push an event that will be executed at the given UTC time.
        """
        arg = what, when
        with self.lock:
            heapq.heappush(self.queue, (when, arg))
        return arg

    def schedule_after(self, what, delay):
        """
        Push an event that will be executed after a certain delay in seconds.
        """
        return self.push_at(what, delay + self.time())

    def cancel(self, arg):
        """
        Cancel an event that has been submitted.  Raise False if it wasn't found, e.g.
        it may have been already run
        """
        # Note, there is no way currently to ensure that either the event was
        # removed or successfully executed, i.e. no synchronization.
        # Caveat Emptor.
        for i, e in enumerate(self.queue):
            if e[1] is arg:
                 #heapq has no "remove" method
                del self.queue[i]
                if i != len(self.queue): # if it wasn't the last element
                    heapq.heapify(self.queue)
                return True
        raise False

    def pump(self):
        """
        The worker function for the main loop to process events in the queue
        """
        q = self.queue
        if q:
            batch = []
            now = self.time()
            while q and q[0][0] <= now:
                batch.append(heapq.heappop(q)[1])


            # Run the events
            for arg in batch:
                try:
                    arg[0]()
                except Exception:
                    self.handle_exception(sys.exc_info())
            return len(batch)
        return 0

    @property
    def is_due(self):
        """Returns true if the queue needs pumping now."""
        return self.queue and self.queue[0][0] <= self.time()

    def next_time(self):
        """the UTC time at which the next event is due."""
        if self.queue:
            return self.queue[0][0]
        return None

    def handle_exception(self, exc_info):
        logger.error("Error during event queue processing", exc_info=exc_info)

    @staticmethod
    def time():
        """
        Return the wallclock time used for the event queue
        """
        return elapsed_time()

def schedule_at(what, when):
    return event_queue.schedule_at(what, when)

def schedule_after(what, after):
    return event_queue.schedule_after(what, after)

def cancel(key):
    return event_queue.cancel(key)

def break_wait():
    pass


event_queue = EventQueue()