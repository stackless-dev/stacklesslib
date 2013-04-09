# errors.py
# Define error instances used by stacklesslib

class TimeoutError(RuntimeError):
	"""Stackless operation timed out"""
	pass