#app.py
"""
This module contains a collection of classes and functions that are generally
swapped out to a different implementation if an application is running under
the stackless-framework.  It is intended for things such as sleep(), whose
Stackless-implementation won't work unless the framework is being ticket.
Contrast this with stacklesslib.locks.Lock() which also works as a normal
thread locking primitive.
"""

import time
import threading
from . import main
from . import locks

def install_vanilla():
	g = globals()
	g["sleep"] = time.sleep
	g["Event"] = threading.Event
	g["Lock"] = threading.Lock
	g["Rlock"] = threading.RLock
	g["Condition"] = threading.Condition
	g["Semaphore"] = threading.Semaphore


def install_stackless():
	g = globals()
	g["sleep"] = main.sleep
	g["Event"] = locks.Event
	g["Lock"] = locks.Lock
	g["Rlock"] = locks.RLock
	g["Condition"] = locks.Condition
	g["Semaphore"] = locks.Semaphore

# Run in non-stackless mode until told differently
install_vanilla()