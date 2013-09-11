# errors.py
# Define error instances used by stacklesslib

class TimeoutError(RuntimeError):
    """Stackless operation timed out"""

class CancelledError(RuntimeError):
    """The operation was cancelled"""
