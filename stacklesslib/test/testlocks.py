# test the locks thingie

import unittest
import test.test_support

from .. import locks

from . import lock_tests

class LockTests(lock_tests.LockTests):
    locktype = staticmethod(locks.Lock)

class RLockTests(lock_tests.RLockTests):
    locktype = staticmethod(locks.RLock)

class EventTests(lock_tests.EventTests):
    eventtype = staticmethod(locks.Event)

class ConditionAsRLockTests(lock_tests.RLockTests):
    # An Condition uses an RLock by default and exports its API.
    locktype = staticmethod(locks.Condition)

class ConditionTests(lock_tests.ConditionTests):
    locktype = staticmethod(locks.Lock)
    condtype = staticmethod(locks.Condition)

class NLConditionTests(lock_tests.NLConditionTests):
    locktype = staticmethod(locks.Lock)
    condtype = staticmethod(locks.NLCondition)


class SemaphoreTests(lock_tests.SemaphoreTests):
    semtype = staticmethod(locks.Semaphore)

class BoundedSemaphoreTests(lock_tests.BoundedSemaphoreTests):
    semtype = staticmethod(locks.BoundedSemaphore)


if __name__ == "__main__":
    unittest.main()
