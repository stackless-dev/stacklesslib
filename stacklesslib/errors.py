# errors.py
# Define error instances used by stacklesslib

class TimeoutError(RuntimeError):
    """Stackless operation timed out"""

class CancelledError(RuntimeError):
    """The operation was cancelled"""

class AsyncCallFailed(RuntimeError):
    """Exception raised when an on_failure callback is made"""

